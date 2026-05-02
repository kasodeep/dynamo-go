package store

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"hash/fnv"
	"math"
	"os"
	"sort"
	"time"
)

// SSTable layout on disk:
//
//	┌────────────────────┐
//	│  Data Block        │  one entry per key, sorted ascending
//	├────────────────────┤
//	│  Index Block       │  one entry per data block entry (sparse ok in future)
//	├────────────────────┤
//	│  Bloom Filter      │  bit-array for fast negative lookups
//	├────────────────────┤
//	│  Footer            │  block offsets + magic number + max LSN
//	└────────────────────┘
//
// Data block entry wire format:
//
//	KeyLen      (4) │ ValueLen (4) │ ForLen  (4)
//	Timestamp   (8) │ Deleted  (1)
//	Key (KeyLen) │ Value (ValueLen) │ For (ForLen)
//
// Index block entry:
//
//	KeyLen (4) │ Offset (8) │ Key (KeyLen)
//
// Footer (fixed 44 bytes):
//
//	Magic        (8)  – 0xDEADBEEFCAFEBABE
//	DataOffset   (8)
//	IndexOffset  (8)
//	BloomOffset  (8)
//	MaxSeq       (8)  – highest LSN in this table; used for WAL truncation
//	Checksum     (4)  – CRC32 over the 40 bytes before it

// ErrTombstone is returned by SSTable.Get when the key exists but was deleted.
// The caller must treat this as a definitive not-found and stop searching older
// tables — falling through would return a value that has since been deleted.
var ErrTombstone = fmt.Errorf("store: key is a tombstone")

const (
	sstMagic   uint64 = 0xDEADBEEFCAFEBABE
	footerSize        = 8 + 8 + 8 + 8 + 8 + 4 // 44 bytes
	bloomMBits        = 10                    // bits per element → ~1% false-positive rate
)

// SSTable is a read handle to a flushed, immutable sorted-string table.
type SSTable struct {
	path        string
	dataOffset  int64
	indexOffset int64
	bloomOffset int64
	maxSeq      uint64

	index []indexEntry
	bloom bloomFilter
}

type indexEntry struct {
	key    string
	offset int64
}

// flushToSSTable serialises all entries in m to a new SSTable file and returns
// the file path. Named "<nodeID>_<maxSeq>.sst".
func flushToSSTable(nodeID string, m *MemTable) (string, error) {
	if nodeID != "" && nodeID[0] == ':' {
		nodeID = nodeID[1:]
	}
	path := fmt.Sprintf("%s_%d.sst", nodeID, m.MaxSeq())

	f, err := os.Create(path)
	if err != nil {
		return "", fmt.Errorf("sstable: create %q: %w", path, err)
	}
	defer f.Close()

	w := &sstWriter{f: f}

	var entries []ScanEntry
	if err := m.Scan(func(e ScanEntry) error {
		entries = append(entries, e)
		return nil
	}); err != nil {
		return "", fmt.Errorf("sstable: scan: %w", err)
	}

	bf := newBloomFilter(len(entries), bloomMBits)
	offsets := make([]int64, len(entries))

	// ── Data Block ────────────────────────────────────────────────────────
	dataOffset := int64(0)
	for i, e := range entries {
		offsets[i] = w.pos
		if err := w.writeDataEntry(e); err != nil {
			return "", fmt.Errorf("sstable: write entry %q: %w", e.Key, err)
		}
		bf.add([]byte(e.Key))
	}

	// ── Index Block ───────────────────────────────────────────────────────
	indexOffset := w.pos
	for i, e := range entries {
		if err := w.writeIndexEntry(e.Key, offsets[i]); err != nil {
			return "", fmt.Errorf("sstable: write index entry %q: %w", e.Key, err)
		}
	}

	// ── Bloom Filter ──────────────────────────────────────────────────────
	bloomOffset := w.pos
	if err := w.writeBloom(bf); err != nil {
		return "", fmt.Errorf("sstable: write bloom: %w", err)
	}

	// ── Footer ────────────────────────────────────────────────────────────
	if err := w.writeFooter(dataOffset, indexOffset, bloomOffset, m.MaxSeq()); err != nil {
		return "", fmt.Errorf("sstable: write footer: %w", err)
	}

	if err := f.Sync(); err != nil {
		return "", fmt.Errorf("sstable: sync %q: %w", path, err)
	}
	return path, nil
}

// OpenSSTable opens an existing SSTable for reading.
// Reads the footer, index block, and bloom filter into memory.
// The data block stays on disk and is accessed per-lookup via ReadAt.
func OpenSSTable(path string) (*SSTable, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("sstable: open %q: %w", path, err)
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return nil, err
	}

	footerBuf := make([]byte, footerSize)
	if _, err := f.ReadAt(footerBuf, info.Size()-footerSize); err != nil {
		return nil, fmt.Errorf("sstable: read footer: %w", err)
	}
	sst, err := parseFooter(footerBuf)
	if err != nil {
		return nil, err
	}
	sst.path = path

	indexLen := sst.bloomOffset - sst.indexOffset
	indexBuf := make([]byte, indexLen)
	if _, err := f.ReadAt(indexBuf, sst.indexOffset); err != nil {
		return nil, fmt.Errorf("sstable: read index: %w", err)
	}
	if err := sst.parseIndex(indexBuf); err != nil {
		return nil, err
	}

	bloomLen := info.Size() - footerSize - sst.bloomOffset
	bloomBuf := make([]byte, bloomLen)
	if _, err := f.ReadAt(bloomBuf, sst.bloomOffset); err != nil {
		return nil, fmt.Errorf("sstable: read bloom: %w", err)
	}
	sst.bloom = bloomFilter{bits: bloomBuf}

	return sst, nil
}

// Get looks up a key. Lookup order:
//  1. Bloom filter — if definitely absent, skip the disk read.
//  2. Binary search over in-memory index — resolve disk offset in O(log n).
//  3. Single ReadAt for the data entry.
//
// Returns ErrNotFound if the key is absent.
// Returns ErrTombstone if the key was explicitly deleted — the caller must
// stop searching older tables rather than continuing down the chain.
func (sst *SSTable) Get(key []byte) (Object, error) {
	if !sst.bloom.mayContain(key) {
		return Object{}, ErrNotFound
	}

	offset, ok := sst.lookupIndex(string(key))
	if !ok {
		return Object{}, ErrNotFound
	}

	f, err := os.Open(sst.path)
	if err != nil {
		return Object{}, err
	}
	defer f.Close()

	entry, err := readDataEntryAt(f, offset)
	if err != nil {
		return Object{}, err
	}

	// Return ErrTombstone rather than ErrNotFound so store.go can stop
	// searching older SSTables — a tombstone here means nothing older is valid.
	if entry.Deleted {
		return Object{}, ErrTombstone
	}

	return Object{
		Key:   []byte(entry.Key),
		Value: entry.Value,
		Metadata: Metadata{
			Timestamp: entry.Timestamp,
			For:       entry.For,
		},
	}, nil
}

func (sst *SSTable) lookupIndex(key string) (int64, bool) {
	i := sort.Search(len(sst.index), func(i int) bool {
		return sst.index[i].key >= key
	})
	if i < len(sst.index) && sst.index[i].key == key {
		return sst.index[i].offset, true
	}
	return 0, false
}

func (sst *SSTable) parseIndex(buf []byte) error {
	off := 0
	for off < len(buf) {
		if off+4 > len(buf) {
			return fmt.Errorf("sstable: truncated index entry")
		}
		keyLen := int(binary.BigEndian.Uint32(buf[off:]))
		off += 4
		if off+8+keyLen > len(buf) {
			return fmt.Errorf("sstable: truncated index entry")
		}
		offset := int64(binary.BigEndian.Uint64(buf[off:]))
		off += 8
		key := string(buf[off : off+keyLen])
		off += keyLen
		sst.index = append(sst.index, indexEntry{key: key, offset: offset})
	}
	return nil
}

// ── Writer helpers ────────────────────────────────────────────────────────────

type sstWriter struct {
	f   *os.File
	pos int64
}

func (w *sstWriter) write(b []byte) error {
	n, err := w.f.Write(b)
	w.pos += int64(n)
	return err
}

func (w *sstWriter) writeDataEntry(e ScanEntry) error {
	forBytes := []byte(e.For)
	keyBytes := []byte(e.Key)

	buf := make([]byte, 4+4+4+8+1+len(keyBytes)+len(e.Value)+len(forBytes))
	off := 0

	binary.BigEndian.PutUint32(buf[off:], uint32(len(keyBytes)))
	off += 4
	binary.BigEndian.PutUint32(buf[off:], uint32(len(e.Value)))
	off += 4
	binary.BigEndian.PutUint32(buf[off:], uint32(len(forBytes)))
	off += 4
	binary.BigEndian.PutUint64(buf[off:], uint64(e.Timestamp.UTC().UnixNano()))
	off += 8
	if e.Deleted {
		buf[off] = 1
	}
	off++
	copy(buf[off:], keyBytes)
	off += len(keyBytes)
	copy(buf[off:], e.Value)
	off += len(e.Value)
	copy(buf[off:], forBytes)

	return w.write(buf)
}

func (w *sstWriter) writeIndexEntry(key string, offset int64) error {
	keyBytes := []byte(key)
	buf := make([]byte, 4+8+len(keyBytes))
	binary.BigEndian.PutUint32(buf[0:], uint32(len(keyBytes)))
	binary.BigEndian.PutUint64(buf[4:], uint64(offset))
	copy(buf[12:], keyBytes)
	return w.write(buf)
}

func (w *sstWriter) writeBloom(bf bloomFilter) error {
	return w.write(bf.bits)
}

func (w *sstWriter) writeFooter(dataOff, indexOff, bloomOff int64, maxSeq uint64) error {
	buf := make([]byte, footerSize)
	binary.BigEndian.PutUint64(buf[0:], sstMagic)
	binary.BigEndian.PutUint64(buf[8:], uint64(dataOff))
	binary.BigEndian.PutUint64(buf[16:], uint64(indexOff))
	binary.BigEndian.PutUint64(buf[24:], uint64(bloomOff))
	binary.BigEndian.PutUint64(buf[32:], maxSeq)
	checksum := crc32.ChecksumIEEE(buf[:40])
	binary.BigEndian.PutUint32(buf[40:], checksum)
	return w.write(buf)
}

// ── Reader helpers ────────────────────────────────────────────────────────────

func parseFooter(buf []byte) (*SSTable, error) {
	if len(buf) < footerSize {
		return nil, fmt.Errorf("sstable: footer too short")
	}
	if magic := binary.BigEndian.Uint64(buf[0:]); magic != sstMagic {
		return nil, fmt.Errorf("sstable: bad magic %x", magic)
	}
	got := crc32.ChecksumIEEE(buf[:40])
	want := binary.BigEndian.Uint32(buf[40:])
	if got != want {
		return nil, fmt.Errorf("sstable: footer checksum mismatch (want %d, got %d)", want, got)
	}
	return &SSTable{
		dataOffset:  int64(binary.BigEndian.Uint64(buf[8:])),
		indexOffset: int64(binary.BigEndian.Uint64(buf[16:])),
		bloomOffset: int64(binary.BigEndian.Uint64(buf[24:])),
		maxSeq:      binary.BigEndian.Uint64(buf[32:]),
	}, nil
}

func readDataEntryAt(f *os.File, offset int64) (ScanEntry, error) {
	// Fixed header: KeyLen(4) + ValueLen(4) + ForLen(4) + Timestamp(8) + Deleted(1) = 21 bytes
	hdr := make([]byte, 21)
	if _, err := f.ReadAt(hdr, offset); err != nil {
		return ScanEntry{}, fmt.Errorf("sstable: read entry header: %w", err)
	}

	keyLen := int(binary.BigEndian.Uint32(hdr[0:]))
	valueLen := int(binary.BigEndian.Uint32(hdr[4:]))
	forLen := int(binary.BigEndian.Uint32(hdr[8:]))
	tsNano := int64(binary.BigEndian.Uint64(hdr[12:]))
	deleted := hdr[20] == 1

	payload := make([]byte, keyLen+valueLen+forLen)
	if _, err := f.ReadAt(payload, offset+21); err != nil {
		return ScanEntry{}, fmt.Errorf("sstable: read entry payload: %w", err)
	}

	return ScanEntry{
		Key:       string(payload[:keyLen]),
		Value:     payload[keyLen : keyLen+valueLen],
		For:       string(payload[keyLen+valueLen:]),
		Timestamp: time.Unix(0, tsNano).UTC(),
		Deleted:   deleted,
	}, nil
}

// ── Bloom Filter ──────────────────────────────────────────────────────────────

// bloomFilter is a standard bit-array bloom filter using two independent FNV
// hashes. k hash functions are derived as h1 + i*h2 (Kirsch-Mitzenmacher),
// which avoids k separate hash computations while preserving independence.
type bloomFilter struct {
	bits []byte
	k    int
}

func newBloomFilter(n, bitsPerElem int) bloomFilter {
	m := n * bitsPerElem
	if m == 0 {
		m = 64
	}
	k := int(math.Round(float64(bitsPerElem) * math.Ln2))
	if k < 1 {
		k = 1
	}
	if k > 30 {
		k = 30
	}
	return bloomFilter{bits: make([]byte, (m+7)/8), k: k}
}

func (bf bloomFilter) setBit(i uint)       { bf.bits[i/8] |= 1 << (i % 8) }
func (bf bloomFilter) testBit(i uint) bool { return bf.bits[i/8]&(1<<(i%8)) != 0 }

func bloomHashes(key []byte) (uint64, uint64) {
	h1 := fnv.New64a()
	h1.Write(key)
	h2 := fnv.New64()
	h2.Write(key)
	return h1.Sum64(), h2.Sum64()
}

func (bf bloomFilter) add(key []byte) {
	m := uint(len(bf.bits) * 8)
	h1, h2 := bloomHashes(key)
	for i := 0; i < bf.k; i++ {
		bf.setBit(uint((h1 + uint64(i)*h2) % uint64(m)))
	}
}

func (bf bloomFilter) mayContain(key []byte) bool {
	m := uint(len(bf.bits) * 8)
	h1, h2 := bloomHashes(key)
	for i := 0; i < bf.k; i++ {
		if !bf.testBit(uint((h1 + uint64(i)*h2) % uint64(m))) {
			return false
		}
	}
	return true
}

package store

import (
	"time"

	"github.com/kasodeep/dynamo-go/internal/treemap"
)

// DefaultFlushThreshold is the size in bytes at which the MemTable should
// be flushed to an SSTable (4 MiB).
const DefaultFlushThreshold = 4 << 20

type entryKind uint8

const (
	kindPut    entryKind = iota + 1
	kindDelete           // tombstone — value field is ignored
)

// memEntry is stored in the red-black tree for every key.
// timestamp comes from Object.Metadata.Timestamp, not from insertion
// time, so that the value survives WAL replay with the correct origin time.
type memEntry struct {
	value     []byte
	timestamp time.Time
	kind      entryKind
	// for is non-empty only for hinted entries; it identifies the target peer.
	for_ string
}

// MemTable is an ordered in-memory write buffer backed by a red-black tree.
// Key order is preserved so that Scan emits entries sorted — a prerequisite
// for writing a well-formed SSTable block.
//
// Not safe for concurrent use; the owning Store serialises access via its mutex.
type MemTable struct {
	tree      *treemap.Tree[string, memEntry]
	sizeBytes int
	threshold int
	seq       uint64 // highest WAL LSN applied
}

func NewMemTable(threshold int) *MemTable {
	return &MemTable{
		tree:      treemap.New[string, memEntry](),
		threshold: threshold,
	}
}

// Put inserts or updates a key. timestamp and forPeer are taken directly
// from the Object so they survive a WAL-replay cycle unchanged.
func (m *MemTable) Put(seq uint64, key, value []byte, timestamp time.Time, forPeer string) {
	k := string(key)
	if old, ok := m.tree.Get(k); ok {
		m.sizeBytes -= len(k) + len(old.value)
	}

	m.tree.Insert(k, memEntry{
		value:     value,
		timestamp: timestamp,
		kind:      kindPut,
		for_:      forPeer,
	})
	m.sizeBytes += len(k) + len(value)

	if seq > m.seq {
		m.seq = seq
	}
}

// Delete inserts a tombstone. The tombstone shadows any older value in
// the SSTable layers so GetObject returns ErrNotFound without reading disk.
func (m *MemTable) Delete(seq uint64, key []byte, timestamp time.Time) {
	k := string(key)
	if old, ok := m.tree.Get(k); ok {
		m.sizeBytes -= len(k) + len(old.value)
	}

	m.tree.Insert(k, memEntry{
		timestamp: timestamp,
		kind:      kindDelete,
	})
	m.sizeBytes += len(k)

	if seq > m.seq {
		m.seq = seq
	}
}

// GetResult is the outcome of a MemTable lookup.
type GetResult struct {
	Value     []byte
	Timestamp time.Time
	For       string // non-empty for hinted entries
	Found     bool
	Deleted   bool
}

// Get looks up a key. Three outcomes:
//   - live entry  → Found=true, Deleted=false
//   - tombstone   → Found=true, Deleted=true  (caller must NOT fall through to SSTable)
//   - absent      → Found=false
func (m *MemTable) Get(key []byte) GetResult {
	entry, ok := m.tree.Get(string(key))
	if !ok {
		return GetResult{}
	}
	if entry.kind == kindDelete {
		return GetResult{Found: true, Deleted: true, Timestamp: entry.timestamp}
	}
	return GetResult{
		Found:     true,
		Value:     entry.value,
		Timestamp: entry.timestamp,
		For:       entry.for_,
	}
}

func (m *MemTable) ShouldFlush() bool { return m.sizeBytes >= m.threshold }
func (m *MemTable) SizeBytes() int    { return m.sizeBytes }

// MaxSeq returns the highest LSN applied. After a successful SSTable flush,
// WAL records with LSN ≤ MaxSeq can be safely truncated.
func (m *MemTable) MaxSeq() uint64 { return m.seq }

// ScanEntry is one key-value pair emitted by Scan.
type ScanEntry struct {
	Key       string
	Value     []byte
	Timestamp time.Time
	For       string // non-empty for hinted entries
	Deleted   bool
}

// Scan iterates all entries in ascending key order.
// The SSTable writer calls this to consume entries in sorted order, which
// is required to build a binary-searchable SSTable index block.
func (m *MemTable) Scan(fn func(e ScanEntry) error) error {
	var iterErr error
	m.tree.InOrder(func(k string, v memEntry) bool {
		err := fn(ScanEntry{
			Key:       k,
			Value:     v.value,
			Timestamp: v.timestamp,
			For:       v.for_,
			Deleted:   v.kind == kindDelete,
		})
		if err != nil {
			iterErr = err
			return false
		}
		return true
	})
	return iterErr
}

// Flush writes the MemTable to an SSTable file and resets internal state.
// See sstable.go for the full implementation.
func (m *MemTable) Flush(nodeID string) (string, error) {
	path, err := flushToSSTable(nodeID, m)
	if err != nil {
		return "", err
	}
	m.reset()
	return path, nil
}

func (m *MemTable) reset() {
	m.tree = treemap.New[string, memEntry]()
	m.sizeBytes = 0
	// seq is intentionally kept as the high-water mark for WAL truncation.
}

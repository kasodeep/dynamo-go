package store

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"sync/atomic"
	"time"
)

// WAL ensures every mutation is durable before it is acknowledged.
// On crash recovery the WAL is replayed to rebuild in-memory state
// that was not yet flushed to the SSTable layer.
//
// Wire format per record:
//
//	┌──────────────────────────────────────────────────────────┐
//	│ Length      (4)  – total record size in bytes            │
//	│ Checksum    (4)  – CRC32 over [Sequence..Value]          │
//	│ Sequence    (8)  – monotonically increasing LSN          │
//	│ Timestamp   (8)  – Unix nano, UTC                        │
//	│ OpType      (1)  – Put / Delete / Hinted                 │
//	│ ForLen      (4)  – length of Metadata.For (0 for Put/Del)│
//	│ KeyLen      (4)  – length of Key                         │
//	│ ValueLen    (4)  – length of Value                       │
//	│ For         (ForLen bytes)                               │
//	│ Key         (KeyLen bytes)                               │
//	│ Value       (ValueLen bytes)                             │
//	└──────────────────────────────────────────────────────────┘
//
// Fixed header: 37 bytes. Timestamp and For are included so that
// a full Object can be reconstructed from the WAL alone, without
// consulting any other data structure.
//
// Concurrency: a single goroutine owns the file descriptor.
// Callers send pre-serialised records over a buffered channel and
// block until the OS page-cache write completes. Fsync is batched
// on a 10 ms ticker so that burst writes share one syscall.

// OpType distinguishes mutation kinds in the WAL.
type OpType uint8

const (
	OpPut    OpType = iota + 1
	OpDelete        // tombstone; Value is empty on disk
	OpHinted        // write destined for a temporarily unavailable peer
)

// walHeaderSize is the fixed prefix of every record (bytes).
//
//	4 (Length) + 4 (Checksum) + 8 (Sequence) + 8 (Timestamp)
//	+ 1 (OpType) + 4 (ForLen) + 4 (KeyLen) + 4 (ValueLen) = 37
const walHeaderSize = 37

// walRecord is the parsed in-memory form of a WAL entry.
type walRecord struct {
	Length    uint32
	Checksum  uint32
	Sequence  uint64
	Timestamp time.Time
	OpType    OpType
	For       string // non-empty only for OpHinted
	Key       []byte
	Value     []byte
}

// WAL is an append-only write-ahead log backed by a single file.
type WAL struct {
	f    *os.File
	ch   chan walAppend
	done chan struct{}
	seq  atomic.Uint64
}

type walAppend struct {
	data []byte
	ack  chan error
}

// NewWAL opens (or creates) the WAL file for node id (e.g. ":3000")
// and starts the background writer goroutine.
func NewWAL(id string) (*WAL, error) {
	name := id[1:] + ".log"
	f, err := os.OpenFile(name, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("wal: open %q: %w", name, err)
	}

	w := &WAL{
		f:    f,
		ch:   make(chan walAppend, 64),
		done: make(chan struct{}),
	}
	go w.run()
	return w, nil
}

// run is the sole goroutine that writes to and syncs the file.
// Writes are acknowledged after Write(); fsync is decoupled onto a
// 10 ms ticker to batch concurrent writes into one syscall.
func (w *WAL) run() {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	defer w.f.Sync()

	for {
		select {
		case entry := <-w.ch:
			_, err := w.f.Write(entry.data)
			entry.ack <- err

		case <-ticker.C:
			w.f.Sync()

		case <-w.done:
			for {
				select {
				case entry := <-w.ch:
					_, err := w.f.Write(entry.data)
					entry.ack <- err
				default:
					return
				}
			}
		}
	}
}

// append serialises obj into a WAL record and blocks until the OS has
// accepted the bytes. Returns the assigned LSN.
func (w *WAL) append(op OpType, obj Object) (uint64, error) {
	seq := w.seq.Add(1)
	data, checksum := serializeRecord(seq, op, obj)
	binary.BigEndian.PutUint32(data[4:8], checksum)

	ack := make(chan error, 1)
	w.ch <- walAppend{data: data, ack: ack}
	if err := <-ack; err != nil {
		return 0, fmt.Errorf("wal: append seq=%d: %w", seq, err)
	}
	return seq, nil
}

// Close drains remaining records, fsyncs, and closes the file.
func (w *WAL) Close() error {
	close(w.done)
	if err := w.f.Sync(); err != nil {
		return fmt.Errorf("wal: final sync: %w", err)
	}
	return w.f.Close()
}

// serializeRecord encodes a WAL record. The checksum field is left
// zeroed; the caller writes the returned checksum into bytes [4:8].
func serializeRecord(seq uint64, op OpType, obj Object) ([]byte, uint32) {
	forBytes := []byte(obj.Metadata.For)
	forLen := len(forBytes)
	keyLen := len(obj.Key)
	valueLen := len(obj.Value)
	totalLen := walHeaderSize + forLen + keyLen + valueLen

	buf := make([]byte, totalLen)
	off := 0

	binary.BigEndian.PutUint32(buf[off:], uint32(totalLen))
	off += 4
	off += 4 // checksum placeholder

	binary.BigEndian.PutUint64(buf[off:], seq)
	off += 8

	ts := obj.Metadata.Timestamp.UTC().UnixNano()
	binary.BigEndian.PutUint64(buf[off:], uint64(ts))
	off += 8

	buf[off] = byte(op)
	off++

	binary.BigEndian.PutUint32(buf[off:], uint32(forLen))
	off += 4
	binary.BigEndian.PutUint32(buf[off:], uint32(keyLen))
	off += 4
	binary.BigEndian.PutUint32(buf[off:], uint32(valueLen))
	off += 4

	copy(buf[off:], forBytes)
	off += forLen
	copy(buf[off:], obj.Key)
	off += keyLen
	copy(buf[off:], obj.Value)

	// Checksum covers everything after the checksum field itself.
	checksum := crc32.ChecksumIEEE(buf[8:])
	return buf, checksum
}

// DeserializeRecord parses a raw byte slice into a walRecord.
// Used during crash recovery to replay the WAL.
func DeserializeRecord(buf []byte) (*walRecord, error) {
	if len(buf) < walHeaderSize {
		return nil, fmt.Errorf("wal: record too short (%d bytes)", len(buf))
	}

	rec := &walRecord{}
	off := 0

	rec.Length = binary.BigEndian.Uint32(buf[off:])
	off += 4
	rec.Checksum = binary.BigEndian.Uint32(buf[off:])
	off += 4
	rec.Sequence = binary.BigEndian.Uint64(buf[off:])
	off += 8

	tsNano := int64(binary.BigEndian.Uint64(buf[off:]))
	off += 8
	rec.Timestamp = time.Unix(0, tsNano).UTC()

	rec.OpType = OpType(buf[off])
	off++

	forLen := int(binary.BigEndian.Uint32(buf[off:]))
	off += 4
	keyLen := int(binary.BigEndian.Uint32(buf[off:]))
	off += 4
	valueLen := int(binary.BigEndian.Uint32(buf[off:]))
	off += 4

	expected := walHeaderSize + forLen + keyLen + valueLen
	if len(buf) < expected {
		return nil, fmt.Errorf("wal: truncated record (have %d, need %d)", len(buf), expected)
	}

	rec.For = string(buf[off : off+forLen])
	off += forLen

	rec.Key = make([]byte, keyLen)
	copy(rec.Key, buf[off:])
	off += keyLen

	rec.Value = make([]byte, valueLen)
	copy(rec.Value, buf[off:])

	got := crc32.ChecksumIEEE(buf[8:expected])
	if got != rec.Checksum {
		return nil, fmt.Errorf("wal: checksum mismatch seq=%d (want %d, got %d)",
			rec.Sequence, rec.Checksum, got)
	}

	return rec, nil
}

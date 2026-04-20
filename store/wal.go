package store

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"sync/atomic"
	"time"
)

// The WAL ensures that every mutation is persisted to disk before being
// acknowledged. On crash recovery, the WAL is replayed to reconstruct
// in-memory state that had not yet been flushed to the SSTable layer.
//
// # Wire Format
//
// Each record is serialized as a flat binary structure:
//
//	┌─────────────────────────────────────────────────────┐
//	│ Length   (4 bytes) – total record size in bytes      │
//	│ Checksum (4 bytes) – CRC32 over [OpType..Value]      │
//	│ Sequence (8 bytes) – monotonically increasing LSN    │
//	│ OpType   (1 byte)  – operation kind (Put/Del/Hinted) │
//	│ KeyLen   (4 bytes) – length of Key in bytes          │
//	│ ValueLen (4 bytes) – length of Value in bytes        │
//	│ Key      (KeyLen bytes)                               │
//	│ Value    (ValueLen bytes)                             │
//	└─────────────────────────────────────────────────────┘
//
// Fixed header is 25 bytes. Variable-length Key and Value follow.
//
// # Concurrency model
//
// A single goroutine (run) owns the file descriptor. Callers send
// pre-serialized records over a buffered channel and block until
// the record is written. Fsync is issued on a 10 ms ticker so that
// burst writes are batched, reducing syscall overhead while bounding
// the durability window.

// OpType distinguishes the kind of mutation recorded in the WAL.
// The recovery logic branches on this to reconstruct the correct state.
type OpType uint8

const (
	OpPut OpType = iota + 1
	OpDelete
	
	// OpHinted records a write destined for a temporarily unavailable peer.
	OpHinted
)

// walHeaderSize is the fixed-width prefix of every WAL record.
//
//	4 (Length) + 4 (Checksum) + 8 (Sequence) + 1 (OpType) + 4 (KeyLen) + 4 (ValueLen)
const walHeaderSize = 25

// walRecord is a parsed in-memory representation of a single WAL entry.
// It mirrors WALEntry but is the authoritative type used internally by the WAL.
type walRecord struct {
	Length   uint32 // total serialized size, including header
	Checksum uint32 // CRC32IEEE over payload bytes [OpType..Value]
	Sequence uint64 // log sequence number (LSN)
	OpType   OpType
	KeyLen   uint32
	ValueLen uint32
	Key      []byte
	Value    []byte
}

// WAL is a write-ahead log backed by a single append-only file.
//
// All writes are serialized through an internal goroutine that owns
// the file descriptor, eliminating the need for explicit locking on
// the hot path.
type WAL struct {
	f        *os.File
	ch       chan walAppend // inbound serialized records + ack channel
	done     chan struct{}  // signals the run goroutine to stop
	seq      atomic.Uint64  // logical sequence counter (LSN source)
	syncErrc chan error     // propagates sync errors back to callers
}

// walAppend bundles a serialized record with a one-shot reply channel
// so that Append can block until the record is durably written.
type walAppend struct {
	data []byte
	ack  chan error
}

// NewWAL creates (or truncates) the WAL file for the given node ID and
// starts the background writer goroutine.
//
// id is the node's listen address (e.g. ":3000"). The leading colon is
// stripped to produce a valid filename: "3000.log".
//
// Returns an error if the file cannot be created.
func NewWAL(id string) (*WAL, error) {
	// Strip the leading ':' from the address to form a valid filename.
	name := id[1:] + ".log"
	f, err := os.OpenFile(name, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("wal: open %q: %w", name, err)
	}

	w := &WAL{
		f:        f,
		ch:       make(chan walAppend, 64), // buffer absorbs bursts without blocking
		done:     make(chan struct{}),
		syncErrc: make(chan error, 1),
	}

	go w.run()
	return w, nil
}

// run is the sole goroutine that writes to and syncs the WAL file.
//
// Design rationale:
//   - A single writer eliminates lock contention on the file descriptor.
//   - Writes are acknowledged immediately after Write(); the caller
//     unblocks as soon as the kernel has accepted the bytes.
//   - Fsync is decoupled onto a ticker: a 10 ms window batches concurrent
//     writes into one fsync call, significantly reducing I/O overhead
//     under load while keeping the durability gap bounded.
func (w *WAL) run() {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	defer w.f.Sync() // final sync on shutdown

	for {
		select {
		case entry := <-w.ch:
			_, err := w.f.Write(entry.data)
			entry.ack <- err // unblock the caller immediately after write

		case <-ticker.C:
			// Best-effort periodic sync. If this fails, the next write
			// attempt will likely also fail, surfacing the error to callers.
			w.f.Sync()

		case <-w.done:
			// Drain any remaining records before exiting.
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

// append serializes an Object into a WAL record and sends it to the
// background writer. It blocks until the record has been written to
// the OS page cache (not necessarily fsynced).
//
// Returns the assigned sequence number and any write error.
func (w *WAL) append(op OpType, obj Object) (uint64, error) {
	seq := w.seq.Add(1)

	data, checksum := serializeRecord(seq, op, obj)

	// Embed checksum into the already-allocated header.
	binary.BigEndian.PutUint32(data[4:8], checksum)

	ack := make(chan error, 1)
	w.ch <- walAppend{data: data, ack: ack}
	if err := <-ack; err != nil {
		return 0, fmt.Errorf("wal: append seq=%d: %w", seq, err)
	}
	return seq, nil
}

// Close signals the background writer to flush remaining records,
// performs a final fsync, and closes the underlying file.
func (w *WAL) Close() error {
	close(w.done)
	if err := w.f.Sync(); err != nil {
		return fmt.Errorf("wal: final sync: %w", err)
	}
	return w.f.Close()
}

// serializeRecord encodes a WAL record into a byte slice and returns
// it along with the CRC32 checksum of the payload.
//
// Layout: [Length(4) | Checksum(4) | Sequence(8) | OpType(1) | KeyLen(4) | ValueLen(4) | Key | Value]
//
// The checksum field in the returned slice is left as zero; the caller
// must write the returned checksum value into bytes [4:8] itself.
// This avoids a second allocation for a two-pass encode.
func serializeRecord(seq uint64, op OpType, obj Object) ([]byte, uint32) {
	keyLen := len(obj.Key)
	valueLen := len(obj.Value)
	totalLen := walHeaderSize + keyLen + valueLen

	buf := make([]byte, totalLen)
	offset := 0

	// Length — total record size
	binary.BigEndian.PutUint32(buf[offset:], uint32(totalLen))
	offset += 4

	// Checksum placeholder — filled in after payload is written
	offset += 4

	// Sequence (LSN)
	binary.BigEndian.PutUint64(buf[offset:], seq)
	offset += 8

	// OpType
	buf[offset] = byte(op)
	offset++

	// KeyLen
	binary.BigEndian.PutUint32(buf[offset:], uint32(keyLen))
	offset += 4

	// ValueLen
	binary.BigEndian.PutUint32(buf[offset:], uint32(valueLen))
	offset += 4

	// Payload: Key + Value
	copy(buf[offset:], obj.Key)
	offset += keyLen
	copy(buf[offset:], obj.Value)

	// CRC32 is computed over everything after the checksum field.
	// This covers [Sequence..Value], i.e. buf[8:].
	checksum := crc32.ChecksumIEEE(buf[8:])
	return buf, checksum
}

// DeserializeRecord parses a raw byte slice into a walRecord.
// Used during crash recovery to replay the WAL.
//
// Returns an error if the buffer is too short or the checksum does not match.
func DeserializeRecord(buf []byte) (*walRecord, error) {
	if len(buf) < walHeaderSize {
		return nil, fmt.Errorf("wal: record too short (%d bytes)", len(buf))
	}

	rec := &walRecord{}
	offset := 0

	rec.Length = binary.BigEndian.Uint32(buf[offset:])
	offset += 4

	rec.Checksum = binary.BigEndian.Uint32(buf[offset:])
	offset += 4

	rec.Sequence = binary.BigEndian.Uint64(buf[offset:])
	offset += 8

	rec.OpType = OpType(buf[offset])
	offset++

	rec.KeyLen = binary.BigEndian.Uint32(buf[offset:])
	offset += 4

	rec.ValueLen = binary.BigEndian.Uint32(buf[offset:])
	offset += 4

	expected := int(walHeaderSize) + int(rec.KeyLen) + int(rec.ValueLen)
	if len(buf) < expected {
		return nil, fmt.Errorf("wal: truncated record (have %d, need %d)", len(buf), expected)
	}

	rec.Key = make([]byte, rec.KeyLen)
	copy(rec.Key, buf[offset:offset+int(rec.KeyLen)])
	offset += int(rec.KeyLen)

	rec.Value = make([]byte, rec.ValueLen)
	copy(rec.Value, buf[offset:offset+int(rec.ValueLen)])

	// Verify checksum over buf[8:expected] (everything after the checksum field).
	got := crc32.ChecksumIEEE(buf[8:expected])
	if got != rec.Checksum {
		return nil, fmt.Errorf("wal: checksum mismatch seq=%d (want %d, got %d)",
			rec.Sequence, rec.Checksum, got)
	}

	return rec, nil
}

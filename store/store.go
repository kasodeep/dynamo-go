// Package store provides durable object storage backed by a write-ahead log
// and an in-memory MemTable, with an SSTable flush path reserved for
// milestone-3.
//
// # Write path (current)
//
//	WriteObject / WriteHintedObject / DeleteObject
//	       │
//	       ├─① WAL.append()        ← crash-durability guarantee
//	       │
//	       ├─② MemTable.Put/Delete  ← in-memory ordered index, O(log n)
//	       │
//	       └─③ MemTable.ShouldFlush?
//	              yes → MemTable.Flush()   ← TODO milestone-3: SSTable write
//	              no  → return
//
// # Read path (current)
//
//	GetObject
//	       │
//	       ├─① MemTable.Get()       ← O(log n) in-memory lookup
//	       │      hit (live)    → return value
//	       │      hit (deleted) → return ErrNotFound
//	       │      miss          → TODO milestone-3: SSTable lookup
//	       │
//	       └─② (SSTable chain – TODO milestone-3)
//
// # Hinted handoff
//
// Hinted writes are stored in a dedicated MemTable (hintedMem) so that the
// local read path never returns data that belongs to another peer.
// The background handoff goroutine (TODO milestone-4) will drain hintedMem
// once the target node is reachable again.
//
// # Concurrency
//
// Store is NOT safe for concurrent use in its current form. A sync.RWMutex
// will be added in milestone-3 alongside the full read path, where the
// distinction between read-lock (Get) and write-lock (Put/Delete/Flush)
// starts to matter.
package store

import (
	"errors"
	"fmt"
	"time"
)

// ErrNotFound is returned by GetObject when the key does not exist in any
// accessible layer (MemTable or — once available — SSTable chain).
var ErrNotFound = errors.New("store: key not found")

// Store is the durable object store for a single node.
//
// It composes:
//   - a WAL for crash durability
//   - a primary MemTable for live writes
//   - a hinted MemTable for hinted-handoff writes
type Store struct {
	nodeID    string
	wal       *WAL
	mem       *MemTable // primary: serves local reads and writes
	hintedMem *MemTable // hinted: forwarded to peers on recovery (milestone-4)
}

// New initialises a Store for the given node address.
//
// id is the node's listen address (e.g. ":3000"). The WAL derives its
// filename from id (stripping the leading colon), and both MemTables
// are created with [DefaultFlushThreshold].
//
// Returns an error if the WAL file cannot be opened.
func New(id string) (*Store, error) {
	wal, err := NewWAL(id)
	if err != nil {
		return nil, fmt.Errorf("store: init node %q: %w", id, err)
	}
	return &Store{
		nodeID:    id,
		wal:       wal,
		mem:       NewMemTable(DefaultFlushThreshold),
		hintedMem: NewMemTable(DefaultFlushThreshold),
	}, nil
}

// WriteObject durably records a client-originated write.
//
// Steps:
//  1. Stamp Metadata.Timestamp if the caller left it zero.
//  2. Append an OpPut record to the WAL (blocks until OS page-cache write).
//  3. Insert into the primary MemTable.
//  4. If the MemTable has crossed the flush threshold, flush to SSTable.
//
// The WAL write happens before the MemTable insert so that a crash between
// steps 2 and 3 leaves the record in the WAL for recovery, not silently lost.
//
// Returns the WAL LSN assigned to this write and any I/O error.
func (s *Store) WriteObject(obj Object) (uint64, error) {
	if obj.Metadata.Timestamp.IsZero() {
		obj.Metadata.Timestamp = time.Now().UTC()
	}

	// ① WAL — durability before visibility.
	seq, err := s.wal.append(OpPut, obj)
	if err != nil {
		return 0, fmt.Errorf("store: WriteObject key=%q: %w", obj.Key, err)
	}

	// ② MemTable — make the write visible to reads.
	s.mem.Put(seq, obj.Key, obj.Value)

	// ③ Flush if the MemTable has grown beyond the threshold.
	if err := s.maybeFlush(s.mem); err != nil {
		// A flush failure is non-fatal for the write itself: the data is safe
		// in the WAL. Log and continue; the next write will retry the flush.
		// TODO(milestone-3): surface this via a metrics counter or alert.
		_ = err
	}

	return seq, nil
}

// WriteHintedObject durably records a write destined for an unavailable peer.
//
// The object is stored in the hinted MemTable under OpHinted in the WAL.
// The hinted MemTable is flushed independently of the primary MemTable;
// its entries are forwarded to the target peer once it recovers.
//
// obj.Metadata.For must be set to the target peer's address by the caller
// before invoking this method.
//
// Returns the WAL LSN and any I/O error.
func (s *Store) WriteHintedObject(obj Object) (uint64, error) {
	if obj.Metadata.For == "" {
		return 0, fmt.Errorf("store: WriteHintedObject: Metadata.For must identify the target peer")
	}
	if obj.Metadata.Timestamp.IsZero() {
		obj.Metadata.Timestamp = time.Now().UTC()
	}

	// ① WAL.
	seq, err := s.wal.append(OpHinted, obj)
	if err != nil {
		return 0, fmt.Errorf("store: WriteHintedObject key=%q for=%q: %w",
			obj.Key, obj.Metadata.For, err)
	}

	// ② Hinted MemTable.
	// TODO(milestone-4): define a richer hinted-entry struct instead of
	// storing only the value bytes; the handoff goroutine needs the target
	// peer address to route the forwarded write correctly.
	s.hintedMem.Put(seq, obj.Key, obj.Value)

	// ③ Flush if needed (hinted MemTable has its own independent threshold).
	if err := s.maybeFlush(s.hintedMem); err != nil {
		_ = err // non-fatal; WAL is the source of truth
	}

	return seq, nil
}

// DeleteObject records a tombstone for the given key.
//
// A tombstone prevents the key from being returned by GetObject even if
// an older value exists in an SSTable layer. The tombstone is propagated
// into the SSTable during flush, where compaction will eventually reclaim
// the disk space.
//
// Returns the WAL LSN and any I/O error.
func (s *Store) DeleteObject(key []byte) (uint64, error) {
	obj := Object{
		Key: key,
		Metadata: Metadata{
			Timestamp: time.Now().UTC(),
		},
	}

	// ① WAL.
	seq, err := s.wal.append(OpDelete, obj)
	if err != nil {
		return 0, fmt.Errorf("store: DeleteObject key=%q: %w", key, err)
	}

	// ② MemTable tombstone.
	s.mem.Delete(seq, key)

	// ③ Flush check.
	if err := s.maybeFlush(s.mem); err != nil {
		_ = err
	}

	return seq, nil
}

// GetObject looks up a key, checking the MemTable first.
//
// Current lookup order:
//  1. Primary MemTable  → O(log n), in-memory.
//  2. SSTable chain     → TODO milestone-3.
//
// Returns ErrNotFound when:
//   - the key is absent from the MemTable and there are no SSTables yet, or
//   - the key exists in the MemTable as a tombstone (explicit delete).
//
// The tombstone case is a definitive answer — we do NOT fall through to the
// SSTable even if an older version exists there. This is correct LSM
// semantics: a newer tombstone shadows all older values.
func (s *Store) GetObject(key []byte) (Object, error) {
	result := s.mem.Get(key)

	switch {
	case result.Found && result.Deleted:
		// Tombstone — definitively not found; no SSTable fallthrough.
		return Object{}, ErrNotFound

	case result.Found:
		return Object{
			Key:   key,
			Value: result.Value,
			Metadata: Metadata{
				Timestamp: time.Now().UTC(), // TODO(milestone-3): preserve original write timestamp
			},
		}, nil

	default:
		// Key not in MemTable — check SSTable chain.
		// TODO(milestone-3): iterate Level-0 SSTables (newest first),
		// then Level-1+, performing a binary-search index lookup per file
		// and a bloom filter check to skip files that cannot contain the key.
		return Object{}, ErrNotFound
	}
}

// maybeFlush triggers an SSTable flush if the given MemTable has crossed its
// size threshold.
//
// This runs synchronously on the write path. In production you would move the
// flush to a background goroutine and apply write-stall back-pressure when
// the Level-0 SSTable file count is too high (as RocksDB does). That
// complexity is deferred to milestone-3.
//
// TODO(milestone-3): replace with a background flush goroutine and a
// compaction scheduler that enforces Level-0 file count limits.
func (s *Store) maybeFlush(m *MemTable) error {
	if !m.ShouldFlush() {
		return nil
	}
	return m.Flush()
}

// Close flushes the WAL and releases the file descriptor.
//
// It does NOT flush the MemTable to an SSTable — that is intentional.
// On restart the WAL is replayed to rebuild the MemTable. Once milestone-3
// lands, Close will flush the MemTable before closing so that WAL replay
// on restart covers only the delta since the last flush.
//
// Must be called exactly once on node shutdown.
func (s *Store) Close() error {
	// TODO(milestone-3): flush mem and hintedMem to SSTables before closing
	// so that WAL replay on restart covers only the tail of the log.
	if err := s.wal.Close(); err != nil {
		return fmt.Errorf("store: close node %q: %w", s.nodeID, err)
	}
	return nil
}

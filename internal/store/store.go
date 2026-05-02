// Package store provides durable object storage backed by a write-ahead log,
// an in-memory MemTable, and an SSTable flush path.
package store

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

var ErrNotFound = errors.New("store: key not found")

// Store is the durable object store for a single node.
// Writes go WAL → MemTable (→ SSTable on threshold).
// Reads check MemTable first, then SSTable chain newest-to-oldest.
type Store struct {
	mu        sync.RWMutex
	nodeID    string
	wal       *WAL
	mem       *MemTable
	hintedMem *MemTable
	// sstables holds flushed tables in flush order; newest is last.
	// Reads scan from the end so a more recent flush shadows an older one.
	sstables []*SSTable
}

// New initialises a Store for the given node address (e.g. ":3000").
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

// WriteObject durably records a client-originated write:
//  1. Stamp Metadata.Timestamp if zero.
//  2. Append OpPut to WAL.
//  3. Insert into primary MemTable.
//  4. Flush to SSTable if threshold is crossed.
func (s *Store) WriteObject(obj Object) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if obj.Metadata.Timestamp.IsZero() {
		obj.Metadata.Timestamp = time.Now().UTC()
	}

	seq, err := s.wal.append(OpPut, obj)
	if err != nil {
		return 0, fmt.Errorf("store: WriteObject key=%q: %w", obj.Key, err)
	}

	s.mem.Put(seq, obj.Key, obj.Value, obj.Metadata.Timestamp, "")
	if err := s.maybeFlush(s.mem); err != nil {
		return seq, fmt.Errorf("store: WriteObject flush: %w", err)
	}
	return seq, nil
}

// WriteHintedObject records a write destined for an unavailable peer.
// obj.Metadata.For must identify the target peer before calling this.
func (s *Store) WriteHintedObject(obj Object) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if obj.Metadata.For == "" {
		return 0, fmt.Errorf("store: WriteHintedObject: Metadata.For must identify the target peer")
	}
	if obj.Metadata.Timestamp.IsZero() {
		obj.Metadata.Timestamp = time.Now().UTC()
	}

	seq, err := s.wal.append(OpHinted, obj)
	if err != nil {
		return 0, fmt.Errorf("store: WriteHintedObject key=%q for=%q: %w",
			obj.Key, obj.Metadata.For, err)
	}

	s.hintedMem.Put(seq, obj.Key, obj.Value, obj.Metadata.Timestamp, obj.Metadata.For)
	if err := s.maybeFlush(s.hintedMem); err != nil {
		return seq, fmt.Errorf("store: WriteHintedObject flush: %w", err)
	}
	return seq, nil
}

// DeleteObject records a tombstone. The tombstone shadows any older value
// in the SSTable layers so reads return ErrNotFound without hitting disk.
func (s *Store) DeleteObject(key []byte) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	obj := Object{
		Key:      key,
		Metadata: Metadata{Timestamp: time.Now().UTC()},
	}

	seq, err := s.wal.append(OpDelete, obj)
	if err != nil {
		return 0, fmt.Errorf("store: DeleteObject key=%q: %w", key, err)
	}

	s.mem.Delete(seq, key, obj.Metadata.Timestamp)
	if err := s.maybeFlush(s.mem); err != nil {
		return seq, fmt.Errorf("store: DeleteObject flush: %w", err)
	}
	return seq, nil
}

// GetObject looks up a key in this order:
//  1. Primary MemTable — O(log n), no I/O.
//  2. SSTable chain, newest flush first — bloom check then one disk read per table.
//
// A tombstone at any layer is definitive: stop and return ErrNotFound.
// This is correct LSM semantics — a newer tombstone always shadows older values,
// so falling through to an older SSTable would return a value that was deleted.
func (s *Store) GetObject(key []byte) (Object, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := s.mem.Get(key)
	switch {
	case result.Found && result.Deleted:
		return Object{}, ErrNotFound
	case result.Found:
		return Object{
			Key:   key,
			Value: result.Value,
			Metadata: Metadata{
				Timestamp: result.Timestamp,
			},
		}, nil
	}

	// MemTable miss — walk SSTables from newest to oldest.
	for i := len(s.sstables) - 1; i >= 0; i-- {
		obj, err := s.sstables[i].Get(key)
		if errors.Is(err, ErrTombstone) {
			// Tombstone in this table: the key was deleted after this flush.
			// Stop — nothing in older tables is valid anymore.
			return Object{}, ErrNotFound
		}
		if errors.Is(err, ErrNotFound) {
			// Key absent in this table; check the next older one.
			continue
		}
		if err != nil {
			return Object{}, fmt.Errorf("store: GetObject sstable[%d]: %w", i, err)
		}
		return obj, nil
	}

	return Object{}, ErrNotFound
}

// GetHintedObjects returns all hinted entries destined for the given peer.
// Called by the handoff dispatcher when a peer comes back online.
func (s *Store) GetHintedObjects(peer string) ([]Object, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var out []Object
	err := s.hintedMem.Scan(func(e ScanEntry) error {
		if e.For == peer && !e.Deleted {
			out = append(out, Object{
				Key:   []byte(e.Key),
				Value: e.Value,
				Metadata: Metadata{
					Timestamp: e.Timestamp,
					For:       e.For,
				},
			})
		}
		return nil
	})
	return out, err
}

// maybeFlush triggers an SSTable flush when the MemTable crosses its threshold.
// On success the new SSTable is registered in s.sstables so GetObject can find it.
//
// This runs synchronously on the write path. In production, flush would move to
// a background goroutine with write-stall back-pressure when Level-0 file count
// is too high (as RocksDB does). Deferred until compaction is added.
func (s *Store) maybeFlush(m *MemTable) error {
	if !m.ShouldFlush() {
		return nil
	}

	path, err := m.Flush(s.nodeID)
	if err != nil {
		return err
	}

	sst, err := OpenSSTable(path)
	if err != nil {
		return fmt.Errorf("store: open flushed sstable %q: %w", path, err)
	}

	s.sstables = append(s.sstables, sst)
	return nil
}

// Close flushes the WAL and releases the file descriptor.
// The MemTable is NOT flushed to SSTable here — WAL replay on restart rebuilds it.
// Once compaction lands, Close will flush so that WAL replay covers only the delta.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.wal.Close(); err != nil {
		return fmt.Errorf("store: close node %q: %w", s.nodeID, err)
	}
	return nil
}

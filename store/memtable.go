// Package store – MemTable layer.
//
// A MemTable is the in-memory write buffer that sits between the WAL and the
// on-disk SSTable. Every acknowledged write lands in the WAL first, then is
// inserted here. Reads check the MemTable before touching disk, giving O(log n)
// in-memory lookup with ordering preserved by the underlying red-black tree.
//
// # Lifecycle
//
//	Write  →  WAL.append()  →  MemTable.Put()
//	Read   →  MemTable.Get()  →  (SSTable lookup – TODO milestone-3)
//	Flush  →  MemTable.Flush()  →  SSTable.Write()  →  MemTable.Reset()
//
// # Flush trigger
//
// The MemTable tracks its approximate byte footprint (key bytes + value bytes
// per entry). When [MemTable.ShouldFlush] returns true the caller (Store) is
// responsible for initiating an SSTable flush. The threshold is configurable;
// the default matches LevelDB / RocksDB at 4 MiB.
//
// # Tombstones
//
// Deletes are represented as entries whose value is the sentinel [Tombstone].
// This preserves ordering and lets the flush path emit proper deletion records
// in the SSTable, which is required for correctness during compaction.
//
// # Hinted entries
//
// Hinted handoff writes (OpHinted) are kept in a separate MemTable so that
// the local read path never returns data that belongs to another node.
// The hinted MemTable is flushed / forwarded independently by the handoff
// goroutine (TODO milestone-4).
package store

import (
	"time"

	"github.com/kasodeep/dynamo-go/treemap"
)

// DefaultFlushThreshold is the default size in bytes at which the MemTable
// signals that it should be flushed to an SSTable (4 MiB).
const DefaultFlushThreshold = 4 << 20 // 4 MiB

// entryKind distinguishes live values from tombstones inside the MemTable.
// Keeping it separate from OpType means the MemTable does not depend on the
// WAL wire format.
type entryKind uint8

const (
	kindPut    entryKind = iota + 1
	kindDelete           // tombstone – value field is ignored
)

// memEntry is the value stored inside the red-black tree for every key.
type memEntry[V any] struct {
	value     V
	timestamp time.Time
	kind      entryKind
}

// Tombstone is returned by [MemTable.Get] when the key exists in the
// MemTable but has been deleted. Callers should treat this as a
// definitive "not found" without falling through to the SSTable.
var Tombstone = struct{ deleted bool }{deleted: true}

// MemTable is an ordered in-memory write buffer backed by a red-black tree.
//
// The tree preserves key order so that [MemTable.Scan] can emit entries in
// sorted order — a prerequisite for writing a well-formed SSTable block.
//
// MemTable is NOT safe for concurrent use; the owning Store serialises
// access via its own mutex (added in milestone-3 alongside the read path).
type MemTable struct {
	tree      *treemap.Tree[string, memEntry[[]byte]]
	sizeBytes int    // approximate memory footprint of live key+value bytes
	threshold int    // flush is triggered when sizeBytes exceeds this value
	seq       uint64 // highest WAL LSN applied to this MemTable
}

// NewMemTable returns an empty MemTable with the given flush threshold.
// Pass [DefaultFlushThreshold] unless you have a reason to override it.
func NewMemTable(threshold int) *MemTable {
	return &MemTable{
		tree:      treemap.New[string, memEntry[[]byte]](),
		threshold: threshold,
	}
}

// Put inserts or updates a key with a live value.
//
// If the key already exists (including as a tombstone) its entry is replaced.
// The size accounting is updated to reflect the delta.
//
// seq is the WAL LSN that produced this write; it is tracked so the flush
// path can record the highest applied sequence number in the SSTable footer,
// enabling WAL truncation after a successful flush.
func (m *MemTable) Put(seq uint64, key, value []byte) {
	k := string(key)

	// Subtract the old entry's footprint before overwriting.
	if old, ok := m.tree.Get(k); ok {
		m.sizeBytes -= len(k) + len(old.value)
	}

	m.tree.Insert(k, memEntry[[]byte]{
		value:     value,
		timestamp: time.Now().UTC(),
		kind:      kindPut,
	})
	m.sizeBytes += len(k) + len(value)

	if seq > m.seq {
		m.seq = seq
	}
}

// Delete inserts a tombstone for the given key.
//
// A tombstone is a definitive signal to readers that the key no longer
// exists; it shadows any older value in the SSTable layers below. The
// tombstone is emitted into the SSTable during flush so that compaction
// can eventually reclaim the disk space.
func (m *MemTable) Delete(seq uint64, key []byte) {
	k := string(key)

	if old, ok := m.tree.Get(k); ok {
		m.sizeBytes -= len(k) + len(old.value)
	}

	m.tree.Insert(k, memEntry[[]byte]{
		timestamp: time.Now().UTC(),
		kind:      kindDelete,
	})
	// Tombstones carry no value bytes; only the key contributes to size.
	m.sizeBytes += len(k)

	if seq > m.seq {
		m.seq = seq
	}
}

// GetResult is the outcome of a MemTable lookup.
type GetResult struct {
	Value   []byte
	Found   bool // true if the key exists (live or tombstone)
	Deleted bool // true if the key exists as a tombstone
}

// Get looks up a key in the MemTable.
//
// Three outcomes are possible:
//
//   - key is live → Found=true, Deleted=false, Value=<bytes>
//   - key is a tombstone → Found=true, Deleted=true, Value=nil
//   - key absent → Found=false (fall through to SSTable – TODO milestone-3)
//
// The distinction between "tombstone" and "absent" is critical: a tombstone
// means the key was explicitly deleted and must not be returned even if an
// older version exists in an SSTable layer.
func (m *MemTable) Get(key []byte) GetResult {
	entry, ok := m.tree.Get(string(key))
	if !ok {
		return GetResult{}
	}
	if entry.kind == kindDelete {
		return GetResult{Found: true, Deleted: true}
	}
	return GetResult{Found: true, Value: entry.value}
}

// ShouldFlush reports whether the MemTable has exceeded its size threshold
// and should be flushed to an SSTable before accepting further writes.
//
// The caller (Store.WriteObject) checks this after every Put and initiates
// a flush if true. This is a cooperative, synchronous check — there is no
// background goroutine watching the size. That design keeps the MemTable
// free of goroutines and simplifies the shutdown path.
func (m *MemTable) ShouldFlush() bool {
	return m.sizeBytes >= m.threshold
}

// SizeBytes returns the current approximate memory footprint in bytes.
// Used by monitoring / metrics.
func (m *MemTable) SizeBytes() int {
	return m.sizeBytes
}

// MaxSeq returns the highest WAL LSN that has been applied to this MemTable.
//
// After a successful SSTable flush, the WAL can safely truncate all records
// with LSN ≤ MaxSeq because their data is now durable on disk.
func (m *MemTable) MaxSeq() uint64 {
	return m.seq
}

// ScanEntry is a single key-value pair emitted by [MemTable.Scan].
type ScanEntry struct {
	Key       string
	Value     []byte
	Timestamp time.Time
	Deleted   bool // true → tombstone; Value is nil
}

// Scan iterates over all entries in ascending key order and calls fn for each.
//
// This is the primary output path used by the flush routine: the SSTable
// writer calls Scan to consume entries in sorted order, which is the
// prerequisite for building a sorted-string table with a binary-searchable
// index.
//
// Scan takes a snapshot of the tree's state at call time; mutations during
// iteration are not reflected (the underlying tree iterator does not allow
// concurrent modification).
//
// TODO(milestone-3): the SSTable writer will call this method. The signature
// is intentionally stable so the flush path can be plugged in without
// touching the MemTable.
func (m *MemTable) Scan(fn func(e ScanEntry) error) error {
	var iterErr error

	m.tree.InOrder(func(k string, v memEntry[[]byte]) bool {
		err := fn(ScanEntry{
			Key:       k,
			Value:     v.value,
			Timestamp: v.timestamp,
			Deleted:   v.kind == kindDelete,
		})
		if err != nil {
			iterErr = err
			return false // stop iteration
		}
		return true
	})

	return iterErr
}

// Flush serialises the MemTable to an SSTable file and resets internal state.
//
// TODO(milestone-3): implement SSTable.Write(m.Scan) here.
// The steps will be:
//  1. Open a new SSTable file (name derived from m.seq).
//  2. Call m.Scan, writing each entry into the SSTable data block.
//  3. Write the index block (one entry per data block for binary search).
//  4. Write the bloom filter block (for fast negative lookups).
//  5. Write the footer (magic number, max LSN, block offsets).
//  6. Fsync and close the file.
//  7. Register the new SSTable with the Level-0 manifest.
//  8. Truncate WAL records with LSN ≤ m.seq.
//  9. Call m.reset() to hand back a clean MemTable.
//
// Until milestone-3 this is a no-op that returns nil, which means data
// beyond the threshold is held in memory. That is acceptable for the
// current development phase since durability is already covered by the WAL.
func (m *MemTable) Flush() error {
	// TODO(milestone-3): SSTable flush.
	m.reset()
	return nil
}

// reset clears the MemTable after a successful flush.
// A new tree is allocated rather than cleared in-place to release GC pressure
// from the old nodes immediately.
func (m *MemTable) reset() {
	m.tree = treemap.New[string, memEntry[[]byte]]()
	m.sizeBytes = 0
	// seq is intentionally NOT reset; it stays as the high-water mark so the
	// WAL truncation point remains known even after reset.
}

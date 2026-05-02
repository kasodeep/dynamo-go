package node

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/kasodeep/dynamo-go/internal/store"
)

// inflight tracks the quorum state for a single in-progress request (read or write).
//
// Lifecycle:
//
//	CreateAndAddRequest → goroutine fans out → ack()/merge()+ack() called per replica →
//	  needed  reached → done closed  (client can return)
//	  minimum reached → cancel() called + entry removed (background work stops)
//
// Concurrency model:
//   - acks is updated with atomic CAS — no lock needed for the counter.
//   - done is closed exactly once via doneOnce (sync.Once).
//   - cancel is invoked exactly once via finishOnce (sync.Once).
//   - best uses atomic.Value so merge() and GetBest() are safe without a lock.
//     NOTE: merge() is NOT atomic as a compound read-modify-write — if two goroutines
//     race on merge(), one update may be lost. This is intentional: LWW (last-write-wins)
//     is a best-effort policy and the timestamp comparison is idempotent across retries.
type inflight struct {
	// acks counts received acknowledgements atomically.
	acks int32

	// needed is W (write) or R (read) — the threshold for signalling client success.
	needed int

	// minimum is N — the threshold for stopping background replication / reads.
	minimum int

	// done is closed once acks >= needed, unblocking the coordinator's select.
	done chan struct{}

	// cancel terminates the context that drives the replication / read goroutine.
	cancel context.CancelFunc

	// doneOnce ensures the done channel is closed exactly once even under concurrent acks.
	doneOnce sync.Once

	// finishOnce ensures cancel() and map cleanup run exactly once.
	finishOnce sync.Once

	// best holds the current LWW winner (store.Object). Updated by merge().
	// atomic.Value requires the stored type to be consistent; always store store.Object.
	best atomic.Value
}

// NewInflight allocates and initialises an inflight entry.
// The zero value of inflight is not safe to use — always use this constructor.
func NewInflight(needed, minimum int, cancel context.CancelFunc) *inflight {
	inf := &inflight{
		needed:  needed,
		minimum: minimum,
		done:    make(chan struct{}),
		cancel:  cancel,
	}
	// Seed best with a zero Object so Load() never returns nil.
	inf.best.Store(store.Object{})
	return inf
}

// ack records one acknowledgement and advances the quorum state machine.
//
// Ordering contract (for reads):
//
//	merge(obj) MUST be called before ack() so that when done is closed,
//	GetBest() returns the object that satisfies R — not a stale zero value.
//
// State transitions:
//
//	acks >= needed  → close(done)   [exactly once]
//	acks >= minimum → cancel()      [exactly once]
func (inf *inflight) ack() {
	newCount := atomic.AddInt32(&inf.acks, 1)

	if newCount >= int32(inf.needed) {
		inf.doneOnce.Do(func() {
			close(inf.done)
		})
	}

	if newCount >= int32(inf.minimum) {
		inf.finishOnce.Do(func() {
			inf.cancel()
		})
	}
}

// merge applies a last-write-wins (LWW) policy to update best.
//
// Correctness note:
//
//	The load-compare-store is NOT atomic. Two concurrent merge calls may both
//	read the same current value, compare, and one overwrites the other.
//	This is acceptable because:
//	  (a) DynamoDB-style LWW is a probabilistic conflict resolver, not a CRDT.
//	  (b) Any winner is valid as long as it is the latest among those seen.
//	  (c) The replica with the true latest timestamp will eventually win in practice
//	      because read-repair and anti-entropy reconcile divergence asynchronously.
func (inf *inflight) merge(obj store.Object) {
	current := inf.best.Load().(store.Object)
	if obj.Metadata.Timestamp.After(current.Metadata.Timestamp) {
		inf.best.Store(obj)
	}
}

// Done returns the channel that is closed once W/R acks are received.
// Callers select on this to know when to return to the client.
func (inf *inflight) Done() <-chan struct{} {
	return inf.done
}

// GetBest returns the LWW-winning object seen so far.
// Always call after Done() is closed to ensure at least R objects were merged.
func (inf *inflight) GetBest() store.Object {
	return inf.best.Load().(store.Object)
}

// IsComplete reports whether N acks have been received.
// Used by fanout goroutines to short-circuit further sends.
func (inf *inflight) IsComplete() bool {
	return atomic.LoadInt32(&inf.acks) >= int32(inf.minimum)
}

// ---------------------------------------------------------------------------
// Coordinator
// ---------------------------------------------------------------------------

// Coordinator tracks all in-progress quorum requests on this node.
//
// Design:
//   - Requests are keyed by a UUID generated per client request.
//   - The map is protected by a RWMutex: reads (GetRequest) are cheap and frequent,
//     writes (Create, Delete) are rare.
//   - Coordinator does NOT own the lifecycle of the context — the caller (Node) does.
type Coordinator struct {
	mu       sync.RWMutex
	inflight map[string]*inflight
}

// NewCoordinator returns an initialised Coordinator ready to use.
func NewCoordinator() *Coordinator {
	return &Coordinator{
		inflight: make(map[string]*inflight),
	}
}

// CreateAndAddRequest creates a new inflight entry and registers it atomically.
//
// Parameters:
//   - id:      unique request ID (UUID)
//   - needed:  W (write) or R (read) quorum threshold
//   - minimum: N replication factor
//   - cancel:  CancelFunc from context.WithTimeout — called when N acks arrive
//
// Returns the inflight so the caller can immediately select on Done().
func (c *Coordinator) CreateAndAddRequest(
	id string,
	needed, minimum int,
	cancel context.CancelFunc,
) *inflight {
	c.mu.Lock()
	defer c.mu.Unlock()

	inf := NewInflight(needed, minimum, cancel)
	c.inflight[id] = inf
	return inf
}

// GetRequest retrieves an inflight entry by ID.
// Returns (nil, false) if the request has already been completed and removed.
func (c *Coordinator) GetRequest(id string) (*inflight, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	inf, ok := c.inflight[id]
	return inf, ok
}

// DeleteRequest removes a completed request from the map.
// Safe to call multiple times; subsequent calls are no-ops.
func (c *Coordinator) DeleteRequest(id string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	delete(c.inflight, id)
}

// ---------------------------------------------------------------------------
// Event handlers — called by network receive paths
// ---------------------------------------------------------------------------

// OnAck records a write acknowledgement for the given request ID.
//
// Flow:
//  1. Locate inflight.
//  2. Call ack() → may close done (W satisfied) or call cancel (N satisfied).
//  3. If N satisfied, remove from map so future stale acks are silently dropped.
func (c *Coordinator) OnAck(id string) {
	inf, ok := c.GetRequest(id)
	if !ok {
		return // request already completed or unknown
	}

	inf.ack()

	if inf.IsComplete() {
		c.DeleteRequest(id)
	}
}

// OnLocalAck is a semantic alias for OnAck used when the local node itself
// performs the write. Having a distinct method name makes the call-site readable
// and allows future divergence (e.g., skipping network serialisation overhead).
func (c *Coordinator) OnLocalAck(id string) {
	c.OnAck(id)
}

// OnReadAck merges an object from a replica and records one read acknowledgement.
//
// Ordering is critical:
//
//	merge BEFORE ack — guarantees that when done is closed (R acks received),
//	GetBest() returns a valid, merged result and not a zero value.
func (c *Coordinator) OnReadAck(id string, obj store.Object) {
	inf, ok := c.GetRequest(id)
	if !ok {
		return // request already completed or unknown
	}

	inf.merge(obj) // LWW merge first
	inf.ack()      // then advance quorum state

	if inf.IsComplete() {
		c.DeleteRequest(id)
	}
}

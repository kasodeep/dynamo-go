// Package member provides a concurrency-safe membership table used for
// tracking node liveness in a distributed system.
//
// It is designed for gossip-based protocols (e.g., SWIM-style), where
// nodes exchange membership state and converge toward eventual consistency.
//
// Core properties:
//   - Thread-safe access via RWMutex
//   - Version-based conflict resolution
//   - Deterministic state precedence (Dead > Suspect > Alive)
//   - Snapshot isolation for readers
//
// Note:
// During gossip protocol, we may receive a table with our id, so callers must ensure this void.
// The two funcs using this void are Set and Upsert.
package member

import (
	"sync"
	"time"
)

// Table maintains a mapping of node IDs to their membership state.
//
// It is safe for concurrent use. All mutations are guarded by a write lock,
// while reads use a read lock for better concurrency.
type Table struct {
	mu      sync.RWMutex
	members map[string]*Member
}

// New creates and returns an empty membership table.
func New() *Table {
	return &Table{
		members: make(map[string]*Member),
	}
}

// Get returns a copy of the Member associated with the given ID.
//
// The returned Member is a value copy, ensuring callers cannot mutate
// internal state without going through the Table API.
func (t *Table) Get(id string) (Member, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	m, ok := t.members[id]
	if !ok {
		return Member{}, false
	}
	return *m, true
}

// Set inserts or replaces the given Member.
//
// This is a direct write and does not perform version checks.
// Intended for initialization or authoritative updates.
func (t *Table) Set(m *Member) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.members[m.ID] = m
}

// Upsert merges an incoming Member into the table.
//
// Conflict resolution rules:
//  1. If member does not exist → insert
//  2. If incoming.Version > existing.Version:
//     - Accept normally, BUT:
//     - Reject unsafe transitions (e.g., Alive → Dead without local suspicion)
//  3. If versions equal → choose worse state (with same safety guard)
//
// Safety:
//   - Prevents blind Alive → Dead transitions from gossip
//   - Ensures local failure detector has precedence over remote claims
func (t *Table) Upsert(incoming *Member) {
	t.mu.Lock()
	defer t.mu.Unlock()

	existing, ok := t.members[incoming.ID]
	if !ok {
		t.members[incoming.ID] = incoming
		return
	}

	// Reject unsafe transition: Alive → Dead directly via gossip
	if existing.State == Alive && incoming.State == Dead {
		return
	}

	if incoming.Version > existing.Version {
		t.members[incoming.ID] = incoming
		return
	}

	if incoming.Version == existing.Version {
		if worseState(incoming.State, existing.State) {
			t.members[incoming.ID] = incoming
		}
	}
}

// UpdateState updates only the state of a member.
//
// Behavior:
//   - Increments Version unconditionally
//   - Updates State
//   - Updates LastSeen only if state == Alive
//
// Used for local failure detection transitions.
func (t *Table) UpdateState(id string, state State) {
	t.mu.Lock()
	defer t.mu.Unlock()

	m, ok := t.members[id]
	if !ok {
		return
	}

	m.Version++
	m.State = state

	if state == Alive {
		m.LastSeen = time.Now()
	}
}

// MarkAlive marks a member as Alive.
//
// Behavior:
//   - Updates LastSeen timestamp
//   - If state was not Alive, increments Version and sets Alive
//
// Used for successful ping/pong or reconnection events.
func (t *Table) MarkAlive(id string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	m, ok := t.members[id]
	if !ok {
		return
	}

	m.LastSeen = time.Now()

	if m.State != Alive {
		m.Version++
		m.State = Alive
	}
}

// Snapshot returns a consistent copy of all members.
//
// The returned slice contains value copies, ensuring callers cannot
// mutate internal state. Suitable for gossip dissemination.
func (t *Table) Snapshot() []Member {
	t.mu.RLock()
	defer t.mu.RUnlock()

	out := make([]Member, 0, len(t.members))
	for _, m := range t.members {
		out = append(out, *m)
	}
	return out
}

// Remove deletes a member from the table.
//
// This is typically used sparingly (e.g., tombstone cleanup).
func (t *Table) Remove(id string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	delete(t.members, id)
}

package member

import (
	"sync"
	"time"
)

// Maps the id of remote peer to it's member state.
// Upsert, MarkAlive and UpdateState are used separately for each purpose.
// For adding we need to use the Set method.
type Table struct {
	mu      sync.RWMutex
	members map[string]*Member
}

// Returns a new empty table with an empty map.
func New() *Table {
	return &Table{
		members: make(map[string]*Member),
	}
}

// Returns the member and true if it is present, else returns false.
func (t *Table) Get(id string) (Member, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	m, ok := t.members[id]
	if !ok {
		return Member{}, false
	}

	return *m, true
}

// Set the member associated with the given id in it's data.
func (t *Table) Set(m *Member) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.members[m.ID] = m
}

// Used for gossip protocol, where incoming request provides different state.
// Checks for exists, the versions, and handles worstState in same version.
func (t *Table) Upsert(incoming *Member) {
	t.mu.Lock()
	defer t.mu.Unlock()

	existing, ok := t.members[incoming.ID]
	if !ok {
		t.members[incoming.ID] = incoming
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

// Only updates the state of the member.
// Updates lastseen only when found to be of state alive.
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

// Updates the version and the status along with last seen.
// Useful for pong reaction, and reconnect handshakes after failure.
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

// Returns a copy of all the members.
func (t *Table) Snapshot() []Member {
	t.mu.RLock()
	defer t.mu.RUnlock()

	out := make([]Member, 0, len(t.members))
	for _, m := range t.members {
		out = append(out, *m)
	}
	return out
}

// Removes the member from the table, used rarely.
func (t *Table) Remove(id string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	delete(t.members, id)
}

// Defines failure severity ordering: DEAD > SUSPECT > ALIVE
func worseState(a, b State) bool {
	order := map[State]int{
		Alive:   0,
		Suspect: 1,
		Dead:    2,
	}
	return order[a] > order[b]
}

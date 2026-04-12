package member

import (
	"sync"
	"time"
)

type Table struct {
	mu      sync.RWMutex
	members map[string]*Member
}

func New() *Table {
	return &Table{
		members: make(map[string]*Member),
	}
}

func (t *Table) Get(id string) (Member, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	m, ok := t.members[id]
	if !ok {
		return Member{}, false
	}

	return *m, true
}

func (t *Table) Set(m *Member) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.members[m.ID] = m
}

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

func (t *Table) UpdateState(id string, state State) {
	t.mu.Lock()
	defer t.mu.Unlock()

	m, ok := t.members[id]
	if !ok {
		return
	}

	m.Version++
	m.State = state
	m.LastSeen = time.Now()
}

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

func (t *Table) Snapshot() []Member {
	t.mu.RLock()
	defer t.mu.RUnlock()

	out := make([]Member, 0, len(t.members))
	for _, m := range t.members {
		out = append(out, *m)
	}
	return out
}

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

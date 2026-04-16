package node

import (
	"context"
	"sync"
)

// inflight represents a request in flight, waiting for acks from other nodes.
// Details:
//   - acks represent the current number of acknowledgement received.
//   - needed is the number of request required before which the channel can be closed.
//   - minimum represents the total replication factor (N), after which we stop background work.
//   - done is the chan, which allows the sender (flight request sender), to wait for consensus.
//   - cancel allows for the context
type inflight struct {
	acks    int32
	needed  int
	minimum int

	once   sync.Once
	done   chan struct{}
	cancel context.CancelFunc
}

func NewInflight(needed, minimum int, cancel context.CancelFunc) *inflight {
	return &inflight{
		acks:    0,
		needed:  needed,
		minimum: minimum,
		done:    make(chan struct{}),
		cancel:  cancel,
	}
}

// Coordinator is the concurrent safe access abstraction for inlfight request, for each node.
// During quorum consensus each node, needs to have minimum acknowledged request before return.
// To track the request across node, coordinator provides an in-memory state.
type Coordinator struct {
	mu       sync.RWMutex
	inflight map[string]*inflight
}

// Returns a new coordinator reference with an empty map of inflight request.
func NewCoordinator() *Coordinator {
	return &Coordinator{
		inflight: make(map[string]*inflight),
	}
}

// It creates a new inflight request with the acks needed and the cancel context.
// It maps the request to the provided id and returns the newly created inflight.
func (c *Coordinator) CreateAndAddRequest(id string, needed, minimum int, cancel context.CancelFunc) *inflight {
	c.mu.Lock()
	defer c.mu.Unlock()

	inf := NewInflight(needed, minimum, cancel)
	c.inflight[id] = inf
	return inf
}

// Removes the requets from memory, as part of aggressive garbage collection strategy.
func (c *Coordinator) DeleteRequest(id string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	delete(c.inflight, id)
}

// Returns the inflight request with true, if the request with id exists or else returns false.
func (c *Coordinator) GetRequest(id string) (*inflight, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	inf, ok := c.inflight[id]
	return inf, ok
}

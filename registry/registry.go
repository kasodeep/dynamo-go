package registry

import (
	"sync"

	"github.com/kasodeep/dynamo-go/peer"
)

// Registry maintains a thread-safe mapping of peer IDs to active peer connections.
type Registry struct {
	mu    sync.RWMutex
	peers map[string]peer.Peer
}

// New initializes and returns an empty peer registry.
func New() *Registry {
	return &Registry{peers: make(map[string]peer.Peer)}
}

// Has checks whether a peer with the given ID exists in the registry.
func (r *Registry) Has(id string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.peers[id]
	return ok
}

// Add inserts or updates a peer in the registry by its ID.
func (r *Registry) Add(id string, p peer.Peer) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.peers[id] = p
}

// Remove deletes a peer from the registry by its ID.
func (r *Registry) Remove(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.peers, id)
}

// Each iterates over a snapshot of peers and applies the given function.
func (r *Registry) Each(fn func(peer.Peer)) {
	r.mu.RLock()
	snapshot := make([]peer.Peer, 0, len(r.peers))
	for _, p := range r.peers {
		snapshot = append(snapshot, p)
	}
	r.mu.RUnlock()

	for _, p := range snapshot {
		fn(p)
	}
}

// Len returns the current number of registered peers.
func (r *Registry) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.peers)
}

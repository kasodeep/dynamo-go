// It started as a simple map, but now converted to treemap using internal implementation.
// It provides sorted (key, value) pairs from (hash(id)) -> Peer.
package registry

import (
	"sync"

	"github.com/kasodeep/dynamo-go/peer"
	"github.com/kasodeep/dynamo-go/treemap"
)

// TODO: To make it to consistent hashing we need to convert to hash() -> use crypto.

// Registry maintains a thread-safe mapping of peer IDs to active peer connections.
type Registry struct {
	mu    sync.RWMutex
	peers *treemap.Tree[string, peer.Peer]
}

// New initializes and returns an empty peer registry.
func New() *Registry {
	return &Registry{peers: treemap.New[string, peer.Peer]()}
}

// Has checks whether a peer with the given ID exists in the registry.
func (r *Registry) Has(id string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.peers.Get(id)
	return ok
}

// Add inserts or updates a peer in the registry by its ID.
func (r *Registry) Add(id string, p peer.Peer) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.peers.Insert(id, p)
}

// Remove deletes a peer from the registry by its ID.
func (r *Registry) Remove(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.peers.Delete(id)
}

// Each iterates over a snapshot of peers and applies the given function.
func (r *Registry) Each(fn func(peer.Peer)) {
	r.mu.RLock()
	snapshot := make([]peer.Peer, 0, r.peers.Len())
	for _, p := range r.peers.Values() {
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
	return r.peers.Len()
}

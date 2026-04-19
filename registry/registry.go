// Package registry provides a thread-safe consistent hashing registry
// backed by a treemap (ordered map). It maps hash(peerID) -> peerID -> peer
//
// Design:
//   - ring  : hash(peerID) -> peerID (for key position)
//   - cluster : peerID -> peer (for identity tracking)
//
// Semantics:
//
//   - Add(id, p) is idempotent and acts as an upsert:
//
//   - If id exists → old peer is replaced
//
//   - If not → new peer is added
//
//   - RemoveIfMatch(id, p) removes a peer ONLY if the provided connection
//     matches the currently registered one. This prevents stale disconnects
//     from removing newer connections.
//
// Concurrency:
//   - All operations are thread-safe via RWMutex.
//   - Read-heavy operations use RLock.
//
// Responsibility boundary:
//   - Registry does NOT:
//   - validate connections
//   - detect duplicates
//   - manage lifecycle timing
//   - Caller is responsible for:
//   - when to Add (e.g., after handshake)
//   - when to Remove (e.g., on disconnect)
//   - ensuring correct peer identity
package registry

import (
	"crypto/sha256"
	"encoding/hex"
	"math/rand"
	"strconv"
	"sync"
	"time"

	"github.com/kasodeep/dynamo-go/peer"
	"github.com/kasodeep/dynamo-go/treemap"
)

// V is the number of virtual nodes per physical peer.
var V = 2

// Registry maintains a consistent hashing ring and cluster map.
type Registry struct {
	mu      sync.RWMutex
	ring    *treemap.Tree[string, string]
	cluster map[string]peer.Peer
}

// New initializes an empty registry.
func New() *Registry {
	return &Registry{
		ring:    treemap.New[string, string](),
		cluster: make(map[string]peer.Peer),
	}
}

// Add inserts or replaces a peer in the ring and the cluster.
//
// Behavior:
//   - If id does not exist → inserts new peer
//   - If id exists → replaces old peer (removes old vnode mappings)
//
// This ensures:
//   - At most one active connection per peer ID
//   - Safe handling of reconnects
func (r *Registry) Add(id string, p peer.Peer) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.cluster[id] = p
	r.addToRing(id)
}

// AddSelf inserts the current node maintaining the registry into the ring.
// This addition allows us to ensure that we are a part of the ring, but no peer connection to us.
func (r *Registry) AddSelf(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.addToRing(id)
}

// RemoveIfMatch removes a peer ONLY if the provided peer matches
// the currently registered connection.
//
// This prevents stale connections from deleting newer ones.
func (r *Registry) RemoveIfMatch(id string, p peer.Peer) {
	r.mu.Lock()
	defer r.mu.Unlock()

	current, exists := r.cluster[id]
	if !exists || current != p {
		return // stale or already replaced
	}

	delete(r.cluster, id)
	r.removeFromRing(id)
}

// Get returns (peer, true), if the peer with id exists in the cluster.
// Else it returns (nil, false)
func (r *Registry) Get(id string) (peer.Peer, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	p, ok := r.cluster[id]
	return p, ok
}

// Len returns the number of nodes (actual) in the cluster.
func (r *Registry) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.cluster)
}

// returns the length of the ring, number of virtual nodes.
func (r *Registry) RingLen() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.ring.Len()
}

// Each iterates over all nodes in the cluster.
//
// Guarantees:
//   - Each peer is visited exactly once
//   - Safe snapshot semantics (no lock during callback)
func (r *Registry) Each(fn func(peer.Peer)) {
	r.mu.RLock()
	snapshot := make([]peer.Peer, 0, len(r.cluster))
	for _, p := range r.cluster {
		snapshot = append(snapshot, p)
	}
	r.mu.RUnlock()

	for _, p := range snapshot {
		fn(p)
	}
}

// addToRing inserts V virtual nodes for the given node with id.
func (r *Registry) addToRing(id string) {
	base := id + "_"

	for i := 1; i <= V; i++ {
		key := base + strconv.Itoa(i)
		hash := hash([]byte(key))
		r.ring.Insert(hash, id)
	}
}

// removeFromRing removes all virtual nodes for the given peer ID.
func (r *Registry) removeFromRing(id string) {
	base := id + "_"

	for i := 1; i <= V; i++ {
		key := base + strconv.Itoa(i)
		hash := hash([]byte(key))
		r.ring.Delete(hash)
	}
}

// It convert the key to it's sha256 hash and provides, encoded hash.
func hash(key []byte) string {
	hash := sha256.Sum256(key)
	return hex.EncodeToString(hash[:])
}

// NodesFrom returns the ids of the nodes in the ring, they may contain duplicate.
// It returns the id array, along with the length of the ring.
func (r *Registry) NodesFrom(key []byte) ([]string, int) {
	r.mu.RLock()

	if r.ring.Len() == 0 {
		r.mu.RUnlock()
		return nil, 0
	}

	// snapshot keys
	keys := r.ring.Keys()
	count := len(keys)

	startHash := hash(key)
	ids := make([]string, 0, count)

	// find start index (ceiling)
	startIdx := 0
	for i, k := range keys {
		if k >= startHash {
			startIdx = i
			break
		}
		if i == count-1 {
			startIdx = 0
		}
	}

	for len(ids) < count {
		id, ok := r.ring.Get(keys[startIdx])
		if !ok {
			return ids, len(ids)
		}

		ids = append(ids, id)
		startIdx = (startIdx + 1) % count
	}

	r.mu.RUnlock()
	return ids, len(ids)
}

// NextFrom returns the peer from the virtual ring (hashed), that is larger than it's position
// at an offset.
//
// It collects the snapshot of the keys in the ring. Identifies the startIdx, moves to offset and returns the peer.
func (r *Registry) NextFrom(key []byte, offset int) (peer.Peer, bool) {
	r.mu.RLock()

	if r.ring.Len() == 0 {
		r.mu.RUnlock()
		return nil, false
	}

	// snapshot keys
	keys := r.ring.Keys()
	r.mu.RUnlock()

	startHash := hash(key)

	// find start index (ceiling)
	startIdx := 0
	for i, k := range keys {
		if k >= startHash {
			startIdx = i
			break
		}
		if i == len(keys)-1 {
			startIdx = 0
		}
	}

	// compute final index with offset
	idx := (startIdx + offset) % len(keys)

	r.mu.RLock()
	id, ok := r.ring.Get(keys[idx])
	p, ok := r.cluster[id]
	r.mu.RUnlock()

	return p, ok
}

// RandomSubset returns up to k distinct peers chosen uniformly at random from cluster.
//
// Properties:
//   - No duplicates
//   - Returns min(k, N)
//   - Snapshot semantics (no lock during use)
//   - Safe under concurrent mutations
func (r *Registry) RandomSubset(k int) []peer.Peer {
	r.mu.RLock()

	// snapshot
	n := len(r.cluster)
	if n == 0 || k <= 0 {
		r.mu.RUnlock()
		return nil
	}

	peers := make([]peer.Peer, 0, n)
	for _, p := range r.cluster {
		peers = append(peers, p)
	}

	r.mu.RUnlock()

	// shuffle (Fisher–Yates)
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := len(peers) - 1; i > 0; i-- {
		j := rng.Intn(i + 1)
		peers[i], peers[j] = peers[j], peers[i]
	}

	if k >= len(peers) {
		return peers
	}
	return peers[:k]
}

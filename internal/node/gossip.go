package node

import (
	"fmt"
	"time"

	"github.com/kasodeep/dynamo-go/internal/member"
	"github.com/kasodeep/dynamo-go/internal/message"
	"github.com/kasodeep/dynamo-go/internal/peer"
)

// k defines the fanout for gossip dissemination.
// Small constant ensures probabilistic convergence with bounded overhead.
var k = 2

// gossipLoop periodically disseminates the full membership snapshot
// to a random subset of peers.
//
// Semantics:
//   - Push-based gossip (anti-entropy)
//   - Eventually consistent convergence
//   - Best-effort delivery (failures are ignored)
//
// Behavior:
//   - Every GossipInterval, select k random peers
//   - Send full membership snapshot
//
// Notes:
//   - Uses full-state gossip (not delta-based)
//   - Failures in Send are ignored (handled by failure detection elsewhere)
//   - Snapshot is immutable copy → safe for concurrent use
func (n *Node) gossipLoop() {
	ticker := time.NewTicker(n.cfg.GossipInterval)
	defer ticker.Stop()

	for {
		select {
		case <-n.ctx.Done():
			return

		case <-ticker.C:
			peers := n.registry.RandomSubset(k)
			if len(peers) == 0 {
				continue
			}

			snapshot := n.table.Snapshot()
			payload, err := member.EncodeMembers(snapshot)
			if err != nil {
				n.log.Error("gossip encode failed", "err", err)
				continue
			}

			msg := &message.Message{
				Type:    message.Gossip,
				Payload: payload,
			}

			for _, p := range peers {
				_ = p.Send(msg)
			}
		}
	}
}

// onGossip merges incoming membership state into the local table.
//
// Protocol:
//  1. Validate sender (must be a known peer)
//  2. Decode incoming membership list
//  3. For each member:
//     - Skip self
//     - Merge using version + state precedence (Upsert)
//
// Security:
//   - Rejects gossip from unknown peers (basic trust boundary)
//
// Convergence:
//   - Monotonic via Version
//   - Conflict resolution via state severity
//
// Limitations:
//   - No authentication (trusts registry)
//   - Full snapshot (no delta optimization)
//   - Does NOT trigger connection establishment for new nodes
//
// Concurrency:
//   - Safe due to Table internal locking
func (n *Node) onGossip(p peer.Peer, m *message.Message) error {
	id := p.ID()
	if id == "" {
		return fmt.Errorf("gossip from unidentified peer")
	}

	// Basic trust check: only accept gossip from known peers
	if _, ok := n.registry.Get(id); !ok {
		return fmt.Errorf("peer %s not in registry", id)
	}

	incoming, err := member.DecodeMembers(m.Payload)
	if err != nil {
		return err
	}
	n.log.Info("gossip received", "from", id, "members", len(incoming))

	for i := range incoming {
		m := incoming[i] // avoid pointer-to-loop-variable bug
		if m.ID == n.cfg.ListenAddr {
			continue
		}
		n.table.Upsert(&m)
	}

	return nil
}

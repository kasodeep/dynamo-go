package node

import (
	"fmt"

	"github.com/kasodeep/dynamo-go/message"
	"github.com/kasodeep/dynamo-go/peer"
)

// Broadcast sends a message to all registered peers.
func (n *Node) Broadcast(msg *message.Message) {
	n.registry.Each(func(p peer.Peer) {
		if err := p.Send(msg); err != nil {
			n.log.Warn("broadcast failed", "peer", p.ID(), "err", err)
			n.evict(p)
		}
	})
}

// Send sends a message to a specific peer by its stable ID.
func (n *Node) Send(peerID string, msg *message.Message) error {
	// We don't expose the registry directly — keep the lookup internal.
	var target peer.Peer
	n.registry.Each(func(p peer.Peer) {
		if p.ID() == peerID {
			target = p
		}
	})
	if target == nil {
		return fmt.Errorf("node: peer %q not found", peerID)
	}
	return target.Send(msg)
}

func (n *Node) evict(p peer.Peer) {
	n.registry.RemoveIfMatch(p.ID(), p)
	p.Close()
}

package node

import (
	"fmt"

	"github.com/kasodeep/dynamo-go/internal/message"
	"github.com/kasodeep/dynamo-go/internal/peer"
)

// Broadcast sends a message to all registered peers.
func (n *Node) Broadcast(msg *message.Message) {
	n.registry.Each(func(p peer.Peer) {
		if err := p.Send(msg); err != nil {
			n.log.Warn("broadcast failed", "peer", p.ID(), "err", err)
		}
	})
}

// Send sends a message to a specific peer by its stable ID.
func (n *Node) Send(peerID string, msg *message.Message) error {
	var target peer.Peer
	target, ok := n.registry.Get(peerID)

	if target == nil || !ok {
		return fmt.Errorf("node: peer %q not found", peerID)
	}
	return target.Send(msg)
}

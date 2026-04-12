package node

import (
	"fmt"

	"github.com/kasodeep/dynamo-go/codec"
	"github.com/kasodeep/dynamo-go/member"
	"github.com/kasodeep/dynamo-go/message"
	"github.com/kasodeep/dynamo-go/peer"
	"github.com/kasodeep/dynamo-go/router"
)

// Handle registers an application-level message handler.
// Must be called before Start.
func (n *Node) Handle(msgType uint8, fn router.HandlerFunc) {
	n.router.Handle(msgType, fn)
}

// Called by the dialer node, when the connection was accepted by us.
// We update the registry (ring), and the membership of the node.
func (n *Node) onHandshake(p peer.Peer, msg *message.Message) error {
	id := string(msg.Payload)
	if id == "" || id == n.cfg.ListenAddr {
		return fmt.Errorf("node: empty handshake ID or bad ID from %s", p.RemoteAddr())
	}

	check, ok := n.registry.Get(id)
	if ok && check == p {
		return nil
	}

	p.SetID(id)

	// overwrite is fine (RemoveIfMatch protects correctness)
	n.registry.Add(id, p)

	// membership handling (critical)
	if _, ok := n.table.Get(id); !ok {
		n.table.Set(member.NewMember(id))
	} else {
		n.table.MarkAlive(id)
	}

	n.log.Info("peer registered", "id", id)

	return p.Send(&message.Message{
		Type:    message.Handshake,
		Payload: []byte(n.cfg.ListenAddr),
	})
}

func (n *Node) onPing(p peer.Peer, _ *message.Message) error {
	return p.Send(&message.Message{Type: message.Pong})
}

func (n *Node) onPong(p peer.Peer, _ *message.Message) error {
	id := p.ID()
	if id == "" {
		return nil
	}

	// This is a liveness confirmation, not a state transition
	n.table.MarkAlive(id)
	n.log.Debug("pong received", "id", id)

	return nil
}

func (n *Node) onGossip(p peer.Peer, m *message.Message) error {
	incoming := codec.DecodeMembers(m.Payload)
	n.log.Debug("gossip received", "from", p.ID(), "members", len(incoming))

	for _, member := range incoming {
		if member.ID == n.cfg.ListenAddr {
			continue
		}
		n.table.Upsert(&member)
	}

	return nil
}

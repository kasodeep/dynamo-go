package node

import (
	"fmt"

	"github.com/kasodeep/dynamo-go/internal/member"
	"github.com/kasodeep/dynamo-go/internal/message"
	"github.com/kasodeep/dynamo-go/internal/peer"
)

// Handles the handshake protocol, and invoked when we receive Handshake (0x01) msg.
// Protocol Approach:
//  1. Take the id of the remote node from the msg.
//  2. Check the registry, if same peer exists return. (Repeat send from y twice)
//  3. Set the peerID, and add the peer to the registry.
//  4. If not in table, add or mark it as alive. (Retry after failure of y)
//  5. Send our handshake msg so that they can register us.
//
// Initial Handshake:
//   - Node y -> x. -> denotes dialing and handshake
//   - x stores y, and x -> y. -> handshake from x.
//   - y stores x, and send y -> x.
//   - x ignores and marks y as alive.
//
// y failed temporary:
//   - y -> x. -> denotes dialing and handshake
//   - x stores and mark-alive y, and x -> y. -> handshake from x.
//   - again repeat.
//
// When y fails and comesback up fast so that we still have the peer in registry,
// before go routine for Recv failed acted, we overwrite. go routine secure by remove if match.
func (n *Node) onHandshake(p peer.Peer, msg *message.Message) error {
	id := string(msg.Payload)
	if id == "" || id == n.cfg.ListenAddr {
		return fmt.Errorf("node: empty handshake ID or bad ID from %s", p.RemoteAddr())
	}

	check, ok := n.registry.Get(id)
	if ok && check == p {
		n.table.MarkAlive(id)
		return nil
	}

	p.SetID(id)
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

// Handles pinging when other nodes check upon use for liveliness.
func (n *Node) onPing(p peer.Peer, _ *message.Message) error {
	return p.Send(&message.Message{Type: message.Pong})
}

// Receive reply for ping, marking the node as alive.
func (n *Node) onPong(p peer.Peer, _ *message.Message) error {
	id := p.ID()
	if id == "" {
		return nil
	}

	n.table.MarkAlive(id)
	return nil
}

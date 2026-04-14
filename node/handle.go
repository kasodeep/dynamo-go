package node

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/kasodeep/dynamo-go/codec"
	"github.com/kasodeep/dynamo-go/member"
	"github.com/kasodeep/dynamo-go/message"
	"github.com/kasodeep/dynamo-go/peer"
	"github.com/kasodeep/dynamo-go/router"
	"github.com/kasodeep/dynamo-go/store"
)

// Handle registers an application-level message handler.
// Must be called before Start.
func (n *Node) Handle(msgType uint8, fn router.HandlerFunc) {
	n.router.Handle(msgType, fn)
}

// Called by the dialer node, when the connection was accepted by us.
// We update the registry (ring), and the membership of the node.
// Avoids internal loop by check at registry on id.
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

// When the other nodes check upon use for liveliness.
func (n *Node) onPing(p peer.Peer, _ *message.Message) error {
	return p.Send(&message.Message{Type: message.Pong})
}

// Receive reply for ping, marking the node as alive.
func (n *Node) onPong(p peer.Peer, _ *message.Message) error {
	id := p.ID()
	if id == "" {
		return nil
	}

	// This is a liveness confirmation, not a state transition
	n.table.MarkAlive(id)
	n.log.Info("pong received", "from", id)

	return nil
}

// When we receive a updated state table from others, we upsert the nodes.
func (n *Node) onGossip(p peer.Peer, m *message.Message) error {
	incoming, err := codec.DecodeMembers(m.Payload)

	if err != nil {
		return err
	}

	n.log.Info("gossip received", "from", p.ID(), "members", len(incoming))
	for _, member := range incoming {
		if member.ID == n.cfg.ListenAddr {
			continue
		}
		n.table.Upsert(&member)
	}

	return nil
}

func (n *Node) onPutRequest(p peer.Peer, m *message.Message) error {
	req, err := codec.DecodePutRequest(m.Payload)
	if err != nil {
		return err
	}

	reqID := uuid.NewString()

	ctx, cancel := context.WithCancel(n.ctx)

	inf := &inflight{
		acks:   0,
		needed: int32(W),
		done:   make(chan struct{}),
		cancel: cancel,
	}

	n.inflightMu.Lock()
	n.inflight[reqID] = inf
	n.inflightMu.Unlock()

	obj := store.Object{
		Key:   req.Key,
		Value: req.Value,
		Metadata: store.Metadata{
			Timestamp: time.Now(),
		},
	}

	// fanout writes
	go n.replicate(ctx, reqID, obj)

	// wait for quorum
	select {
	case <-inf.done:
		// quorum reached
	case <-ctx.Done():
		return fmt.Errorf("request cancelled")
	case <-time.After(2 * time.Second):
		return fmt.Errorf("timeout waiting for quorum")
	}

	// cleanup context (important)
	inf.cancel()

	n.inflightMu.Lock()
	delete(n.inflight, reqID)
	n.inflightMu.Unlock()

	return nil
}

func (n *Node) replicate(ctx context.Context, reqID string, obj store.Object) {
	maxAttempts := n.registry.Len()

	for offset := 0; offset < maxAttempts; offset++ {
		select {
		case <-ctx.Done():
			return
		default:
		}

		p, ok := n.registry.NextFrom(obj.Key, offset)
		if !ok {
			continue
		}

		go func(offset int, p peer.Peer) {
			o := obj

			// primary vs sloppy
			if offset < N {
				o.Metadata.For = p.ID()
			} else {
				primary, _ := n.registry.NextFrom(obj.Key, offset%N)
				o.Metadata.For = primary.ID()
			}

			payload, _ := codec.EncodeWriteRequest(&store.WriteRequest{
				ID:  reqID,
				Obj: o,
			})

			_ = p.Send(&message.Message{
				Type:    message.WriteRequest,
				Payload: payload,
			})

		}(offset, p)
	}
}

func (n *Node) onWriteRequest(p peer.Peer, m *message.Message) error {
	req, err := codec.DecodeWriteRequest(m.Payload)
	if err != nil {
		return err
	}

	// TODO: store locally
	if err != nil {
		return err
	}

	// send ack
	payload, _ := codec.EncodeWriteAck(&store.WriteAck{
		ID: req.ID,
	})

	return p.Send(&message.Message{
		Type:    message.WriteRequestAck,
		Payload: payload,
	})
}

func (n *Node) onWriteAck(p peer.Peer, m *message.Message) error {
	ack, err := codec.DecodeWriteAck(m.Payload)
	if err != nil {
		return err
	}

	n.inflightMu.Lock()
	inf, ok := n.inflight[ack.ID]
	n.inflightMu.Unlock()

	if !ok {
		return nil // late or irrelevant ack
	}

	newCount := atomic.AddInt32(&inf.acks, 1)

	if newCount >= inf.needed {
		select {
		case <-inf.done:
			// already closed
		default:
			close(inf.done)
		}
	}

	return nil
}

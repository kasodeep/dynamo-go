package node

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/kasodeep/dynamo-go/member"
	"github.com/kasodeep/dynamo-go/message"
	"github.com/kasodeep/dynamo-go/peer"
	"github.com/kasodeep/dynamo-go/store"
)

func (n *Node) onPutRequest(p peer.Peer, m *message.Message) error {
	req, err := store.DecodePutRequest(m.Payload)
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

		mem, ok := n.table.Get(p.ID())
		if !ok || mem.State != member.Alive {
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

			payload, _ := store.EncodeWriteRequest(&store.WriteRequest{
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
	req, err := store.DecodeWriteRequest(m.Payload)
	if err != nil {
		return err
	}

	// TODO: store locally	

	// send ack
	payload, _ := store.EncodeWriteAck(&store.WriteAck{
		ID: req.ID,
	})

	return p.Send(&message.Message{
		Type:    message.WriteRequestAck,
		Payload: payload,
	})
}

func (n *Node) onWriteRequestAck(p peer.Peer, m *message.Message) error {
	ack, err := store.DecodeWriteAck(m.Payload)
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

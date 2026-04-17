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

// variables for sloppy quorum
var N int = 3
var R int = 2
var W int = 2

// maxConcurrentSends bounds the number of in-flight replication RPCs.
// This prevents unbounded goroutine fanout and provides backpressure.
var maxConcurrentSends = 16

// onPutRequest handles the storage request of the client.
//  1. It decodes the put request, maps to coordinator, and creates a storage object.
//  2. Starts a goroutine to fanout the writes waiting for w acks.
//  3. Waits for the quorum to reach or timeout.
//  4. Returns early on quorum (W), while replication continues in background until N or timeout.
//
// Design Justification:
//   - We separate client success (W) from full replication (N).
//   - Context is NOT cancelled on quorum to allow background convergence.
//   - Cancellation happens when either:
//     a) N acknowledgements are received
//     b) timeout triggers
//     c) node shuts down
func (n *Node) onPutRequest(p peer.Peer, m *message.Message) error {
	req, err := store.DecodePutRequest(m.Payload)
	if err != nil {
		return err
	}

	reqID := uuid.NewString()
	ctx, cancel := context.WithTimeoutCause(
		n.ctx,
		2*time.Second,
		fmt.Errorf("timeout waiting for quorum"),
	)
	inf := n.coordinator.CreateAndAddRequest(reqID, W, N, cancel)

	obj := store.Object{
		Key:   req.Key,
		Value: req.Value,
		Metadata: store.Metadata{
			Timestamp: time.Now(),
		},
	}

	n.log.Info("put request received",
		"req_id", reqID,
		"key", string(req.Key),
		"W", W,
		"N", N,
	)

	go n.replicate(ctx, reqID, obj)

	select {
	case <-inf.done:
		n.log.Info("quorum reached",
			"req_id", reqID,
			"acks", atomic.LoadInt32(&inf.acks),
			"needed", inf.needed,
		)

	case <-ctx.Done(): // if parent failed.
		n.log.Error("put request failed",
			"req_id", reqID,
			"error", context.Cause(ctx),
		)
		n.coordinator.DeleteRequest(reqID)
		return context.Cause(ctx)
	}

	return nil
}

// Consensus Protocol
//
//   - Iterates over the ring from the first node >= key.
//   - Skips dead nodes using membership table.
//   - Sends WriteRequest using bounded concurrency.
//   - Stops early when context is cancelled (N reached / timeout / shutdown).
//
// Design Justification:
//   - Bounded concurrency prevents goroutine explosion under high fanout.
//   - Semaphore ensures controlled parallelism instead of burst fanout.
//   - Context ensures structured cancellation across all goroutines.
func (n *Node) replicate(ctx context.Context, reqID string, obj store.Object) {
	maxAttempts := n.registry.RingLen()
	sem := make(chan struct{}, maxConcurrentSends)

	var attempts int32

	for offset := 0; offset < maxAttempts; offset++ {
		select {
		case <-ctx.Done():
			n.log.Debug("replication stopped",
				"req_id", reqID,
				"reason", context.Cause(ctx),
				"attempts", atomic.LoadInt32(&attempts),
			)
			return
		default:
		}

		p, ok := n.registry.NextFrom(obj.Key, offset)
		if !ok {
			continue
		}

		mem, ok := n.table.Get(p.ID())
		if !ok || mem.State == member.Dead {
			continue
		}

		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			return
		}

		atomic.AddInt32(&attempts, 1)

		go func(offset int, p peer.Peer) {
			defer func() { <-sem }()

			select {
			case <-ctx.Done():
				return
			default:
			}

			o := obj

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

			if err := p.Send(&message.Message{
				Type:    message.WriteRequest,
				Payload: payload,
			}); err != nil {
				n.log.Warn("send failed",
					"req_id", reqID,
					"target", p.ID(),
					"error", err,
				)
			}

		}(offset, p)
	}
}

// onWriteRequest handles the write request received from the remote node.
// Stores the object locally, and sends the ack.
//
// Design Justification:
//   - Write is acknowledged after local persistence (TODO).
//   - Ack drives quorum and replication lifecycle on coordinator.
func (n *Node) onWriteRequest(p peer.Peer, m *message.Message) error {
	req, err := store.DecodeWriteRequest(m.Payload)
	if err != nil {
		return err
	}

	n.log.Debug("write received",
		"req_id", req.ID,
		"from", p.ID(),
	)

	// TODO: store locally

	payload, _ := store.EncodeWriteAck(&store.WriteAck{
		ID: req.ID,
	})

	return p.Send(&message.Message{
		Type:    message.WriteRequestAck,
		Payload: payload,
	})
}

// onWriteRequestAck handles acknowledgements from replicas.
//
// Design Justification:
//   - W acks → client success signal (done channel closed)
//   - N acks → replication lifecycle complete (cancel context)
//   - sync.Once ensures cancel + cleanup happens exactly once
func (n *Node) onWriteRequestAck(p peer.Peer, m *message.Message) error {
	ack, err := store.DecodeWriteAck(m.Payload)
	if err != nil {
		return err
	}

	inf, ok := n.coordinator.GetRequest(ack.ID)
	if !ok {
		return nil
	}

	newCount := atomic.AddInt32(&inf.acks, 1)

	n.log.Debug("ack received",
		"req_id", ack.ID,
		"from", p.ID(),
		"acks", newCount,
	)

	// W reached
	if newCount >= int32(inf.needed) {
		select {
		case <-inf.done:
		default:
			n.log.Info("write quorum satisfied",
				"req_id", ack.ID,
				"acks", newCount,
			)
			inf.closeOnce.Do(func() {
				close(inf.done)
			})
		}
	}

	// N reached
	if newCount >= int32(inf.minimum) {
		inf.once.Do(func() {
			n.log.Info("full replication achieved",
				"req_id", ack.ID,
				"acks", newCount,
			)

			inf.cancel()
			n.coordinator.DeleteRequest(ack.ID)
		})
	}

	return nil
}

// onGetRequest is the coordinator entry point for reads.
//
// Flow:
//  1. Fanout read requests to replicas.
//  2. Aggregate responses using LWW (last-write-wins).
//  3. Return result once R responses received.
//  4. Continue until N for completeness (future repair hook).
//
// Guarantees:
//   - Fast reads (R responses)
//   - Deterministic resolution via timestamp
func (n *Node) onGetRequest(p peer.Peer, m *message.Message) error {
	req, err := store.DecodeGetRequest(m.Payload)
	if err != nil {
		return err
	}

	reqID := uuid.NewString()
	ctx, cancel := context.WithTimeoutCause(
		n.ctx,
		2*time.Second,
		fmt.Errorf("timeout waiting for read quorum"),
	)

	inf := n.coordinator.CreateAndAddRequest(reqID, R, N, cancel)

	n.log.Info("get request received",
		"req_id", reqID,
		"key", string(req.Key),
	)

	go n.readReplicate(ctx, reqID, req.Key)

	select {
	case <-inf.done:
		obj := inf.GetBest()

		n.log.Info("read quorum reached",
			"req_id", reqID,
			"value", string(obj.Value),
		)

		payload, _ := store.EncodeReadAck(&store.ReadAck{
			ID:  reqID,
			Obj: obj,
		})

		return p.Send(&message.Message{
			Type:    message.ReadRequestAck,
			Payload: payload,
		})

	case <-ctx.Done():
		n.log.Error("read failed",
			"req_id", reqID,
			"error", context.Cause(ctx),
		)
		n.coordinator.DeleteRequest(reqID)
		return context.Cause(ctx)
	}
}

// readReplicate mirrors write fanout but for reads.
func (n *Node) readReplicate(ctx context.Context, reqID string, key []byte) {
	maxAttempts := n.registry.Len()
	sem := make(chan struct{}, maxConcurrentSends)

	for offset := 0; offset < maxAttempts; offset++ {
		select {
		case <-ctx.Done():
			return
		default:
		}

		p, ok := n.registry.NextFrom(key, offset)
		if !ok {
			continue
		}

		mem, ok := n.table.Get(p.ID())
		if !ok || mem.State == member.Dead {
			continue
		}

		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			return
		}

		go func(p peer.Peer) {
			defer func() { <-sem }()

			payload, _ := store.EncodeReadRequest(&store.ReadRequest{
				ID:  reqID,
				Key: key,
			})

			if err := p.Send(&message.Message{
				Type:    message.ReadRequest,
				Payload: payload,
			}); err != nil {
				n.log.Warn("read send failed",
					"req_id", reqID,
					"target", p.ID(),
					"error", err,
				)
			}
		}(p)
	}
}

// onReadRequest executes on replicas.
//
// Behavior:
//   - Fetches object locally.
//   - Returns it to coordinator.
func (n *Node) onReadRequest(p peer.Peer, m *message.Message) error {
	req, err := store.DecodeReadRequest(m.Payload)
	if err != nil {
		return err
	}

	n.log.Debug("read request received",
		"req_id", req.ID,
		"from", p.ID(),
	)

	// TODO: fetch from local store
	// obj, _ := n.store.Get(req.Key)
	obj := store.Object{}

	payload, _ := store.EncodeReadAck(&store.ReadAck{
		ID:  req.ID,
		Obj: obj,
	})

	return p.Send(&message.Message{
		Type:    message.ReadRequestAck,
		Payload: payload,
	})
}

// onReadRequestAck aggregates responses at coordinator.
//
// Semantics:
//   - Merge using LWW (latest timestamp wins).
//   - R reached → return to client.
//   - N reached → stop background work.
func (n *Node) onReadRequestAck(p peer.Peer, m *message.Message) error {
	ack, err := store.DecodeReadAck(m.Payload)
	if err != nil {
		return err
	}

	inf, ok := n.coordinator.GetRequest(ack.ID)
	if !ok {
		return nil
	}

	newCount := atomic.AddInt32(&inf.acks, 1)

	n.log.Debug("read ack received",
		"req_id", ack.ID,
		"from", p.ID(),
		"acks", newCount,
	)

	// --- LWW merge ---
	inf.UpdateBest(ack.Obj)

	// --- R reached (client can return) ---
	if newCount >= int32(inf.needed) {
		select {
		case <-inf.done:
		default:
			n.log.Info("read quorum satisfied",
				"req_id", ack.ID,
				"acks", newCount,
			)
			inf.closeOnce.Do(func() {
				close(inf.done)
			})
		}
	}

	// --- N reached (stop background work) ---
	if newCount >= int32(inf.minimum) {
		inf.once.Do(func() {
			n.log.Info("read complete",
				"req_id", ack.ID,
			)

			inf.cancel()
			n.coordinator.DeleteRequest(ack.ID)
		})
	}

	return nil
}

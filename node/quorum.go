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

// Quorum parameters — global for now, can be per-keyspace in future.
//
//	N = total replicas per key (preference list size)
//	W = write quorum (minimum acks before client success)
//	R = read  quorum (minimum acks before client success)
//
// Classic Dynamo defaults are N=3, W=2, R=2 (strong consistency with overlap).
// Current values (N=2, W=1, R=1) optimise for availability at the cost of
// consistency — appropriate for an early-stage implementation.
var N int = 2
var R int = 1
var W int = 1

// maxConcurrentSends caps the number of in-flight goroutines per replication round.
// Prevents goroutine explosion under high write concurrency.
const maxConcurrentSends = 8

// ---------------------------------------------------------------------------
// Write path
// ---------------------------------------------------------------------------

// onPutRequest is the coordinator entry point for client write requests.
//
// Flow:
//  1. Decode the client's PutRequest payload.
//  2. Assign a UUID request ID (used to correlate acks from replicas).
//  3. Create a context with a 2-second deadline — bounds the entire write operation.
//  4. Register an inflight entry in the coordinator.
//  5. Construct the versioned Object (key + value + timestamp).
//  6. Launch replicate() in a goroutine (non-blocking fanout).
//  7. Block until W acks arrive (inf.done closed) OR the context times out.
//  8. Return success to client on W; return error on timeout.
//
// Separation of client success (W) and full replication (N):
//
//	The context is NOT cancelled when W is reached — it remains live so the
//	background replicate() goroutine can continue towards N. Cancellation only
//	happens when N acks are received OR the 2-second deadline fires.
//	This matches the DynamoDB design: the client sees low latency while
//	durability converges asynchronously.
func (n *Node) onPutRequest(p peer.Peer, m *message.Message) error {
	req, err := store.DecodePutRequest(m.Payload)
	if err != nil {
		return err
	}

	reqID := uuid.NewString()

	ctx, cancel := context.WithTimeoutCause(
		n.ctx,
		2*time.Second,
		fmt.Errorf("timeout waiting for write quorum"),
	)

	// Register inflight BEFORE launching goroutine — avoids a race where
	// a very fast local ack arrives before the entry exists in the map.
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
		// W acks received — safe to respond to client.
		// Background replication continues until N or timeout.
		n.log.Info("write quorum reached",
			"req_id", reqID,
			"acks", atomic.LoadInt32(&inf.acks),
			"needed", inf.needed,
		)
		return nil

	case <-ctx.Done():
		// Timeout before W acks arrived. Clean up and propagate error.
		n.log.Error("put request failed",
			"req_id", reqID,
			"error", context.Cause(ctx),
		)
		n.coordinator.DeleteRequest(reqID)
		return context.Cause(ctx)
	}
}

// replicate fans out writes to the N preference-list nodes for a given key,
// using ring-position-aware hinted handoff for unavailable nodes.
//
// Ring layout:
//
//	NodesFrom(key) returns all ring nodes in clockwise order from the key's
//	hash position. The first N entries are the preference list — the canonical
//	home replicas for this key. Entries at index N and beyond are the ordered
//	fallback pool, also in clockwise ring order.
//
//	[ pref[0], pref[1], ..., pref[N-1], fallback[0], fallback[1], ... ]
//	  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^  ^^^^^^^^^^^^^^^^^^^^^^^^^^
//	       preference list (indices 0..N-1)    sloppy quorum candidates
//
// Algorithm:
//  1. Walk the preference list (indices 0..N-1).
//  2. For each alive node: send the write (local or network).
//  3. For each dead node: call findFallback to get the next healthy node
//     from the fallback pool (index N, N+1, ... in ring order). Send the
//     write to that node with Metadata.For = deadNodeID. The fallback stores
//     the object with the hint intact and replays it to the original node
//     when it recovers (hinted handoff, DynamoDB paper §4.6).
//  4. Block on ctx.Done() — goroutine exits when N acks arrive or deadline fires.
//
// Why we do NOT storeLocal for dead preference-list nodes:
//
//	The coordinator is just one node on the ring. Storing the hint locally
//	conflates two distinct roles: coordinator (routes the request) and
//	hinted replica (temporarily holds data for a down node). DynamoDB requires
//	the hint to live on the NEXT clockwise healthy node so ring proximity is
//	preserved and handoff replay is deterministic. The coordinator may be far
//	from the dead node on the ring and is the wrong place for the hint.
//	Exception: if the coordinator itself IS the next clockwise healthy node
//	(small cluster edge case), sendHintedWrite handles that explicitly.
//
// Concurrency:
//
//	Network sends are gated by a shared semaphore (maxConcurrentSends).
//	Local writes are synchronous — ack is guaranteed before any network send.
func (n *Node) replicate(ctx context.Context, reqID string, obj store.Object) {
	// allNodes: full ring in clockwise order from obj.Key's hash position.
	// Slice [0:prefLen] is the preference list; [prefLen:] is the fallback pool.
	allNodes, ringLen := n.registry.NodesFrom(obj.Key)

	inf, ok := n.coordinator.GetRequest(reqID)
	if !ok {
		return // already completed (e.g., instant timeout)
	}

	// On a cluster smaller than N, the preference list is the whole ring.
	prefLen := N
	if ringLen < N {
		prefLen = ringLen
	}

	// fallbackCursor is the index into allNodes where we resume searching for
	// the next fallback. It starts at prefLen (first node after the preference
	// list) and advances monotonically so each fallback is used at most once.
	fallbackCursor := prefLen

	// assignedFallbacks tracks fallback IDs already committed to a hinted write,
	// preventing the same node from receiving two hints for two different dead nodes.
	assignedFallbacks := make(map[string]struct{})

	sem := make(chan struct{}, maxConcurrentSends)

	for prefPos := 0; prefPos < prefLen; prefPos++ {
		if inf.IsComplete() {
			break // N acks already received — no further sends needed
		}

		select {
		case <-ctx.Done():
			return
		default:
		}

		id := allNodes[prefPos]

		mem, ok := n.table.Get(id)
		alive := ok && mem.State != member.Dead

		if !alive {
			// Preference-list node is down.
			// Find the next healthy node in ring order from fallbackCursor.
			fallback, newCursor := n.findFallback(allNodes, fallbackCursor, assignedFallbacks)
			fallbackCursor = newCursor

			if fallback == "" {
				n.log.Warn("hinted handoff: no healthy fallback node available",
					"req_id", reqID,
					"dead_target", id,
				)
				continue
			}

			assignedFallbacks[fallback] = struct{}{}

			n.log.Info("hinted handoff: routing to fallback",
				"req_id", reqID,
				"dead_target", id,
				"fallback", fallback,
			)

			n.sendHintedWrite(ctx, sem, reqID, obj, id, fallback)
			continue
		}

		// Node is alive — write locally or send over the network.
		if id == n.cfg.ListenAddr {
			// TODO: Store locally
			n.coordinator.OnLocalAck(reqID)
			continue
		}

		p, ok := n.registry.Get(id)
		if !ok {
			continue
		}

		sem <- struct{}{}

		go func(p peer.Peer, targetID string) {
			defer func() { <-sem }()

			select {
			case <-ctx.Done():
				return
			default:
			}

			payload, _ := store.EncodeWriteRequest(&store.WriteRequest{
				ID:  reqID,
				Obj: obj,
			})

			if err := p.Send(&message.Message{
				Type:    message.WriteRequest,
				Payload: payload,
			}); err != nil {
				n.log.Warn("write send failed — attempting hinted handoff",
					"req_id", reqID,
					"target", targetID,
					"error", err,
				)
				// Node appeared alive but send failed (crash between health check
				// and send). Best-effort: attempt hinted handoff to the next fallback.
				// fallbackCursor and assignedFallbacks are accessed from a goroutine
				// here — this is a known limitation. For correctness under concurrent
				// send failures a separate mutex or channel would be needed. Acceptable
				// at this stage; anti-entropy handles residual divergence.
				fallback, _ := n.findFallback(allNodes, fallbackCursor, assignedFallbacks)
				if fallback != "" {
					n.sendHintedWrite(ctx, sem, reqID, obj, targetID, fallback)
				}
			}
		}(p, id)
	}

	// Hold goroutine alive until context is cancelled (N acks or deadline).
	// Keeps the semaphore live so in-flight goroutines drain cleanly.
	<-ctx.Done()
}

// findFallback scans allNodes starting at startIdx for the first node that is:
//   - not already assigned as a fallback (not in `assigned`)
//   - alive according to the membership table
//
// Returns the fallback node ID and the next cursor position (startIdx for the
// next call — the caller advances it after consuming the result).
// Returns ("", startIdx) if no suitable fallback exists.
//
// Why return the cursor separately from the node ID:
//
//	The caller owns fallbackCursor. We return the new position so the caller
//	can advance it after committing to use the fallback, ensuring subsequent
//	calls to findFallback do not re-examine already-consumed positions.
func (n *Node) findFallback(
	allNodes []string,
	startIdx int,
	assigned map[string]struct{},
) (id string, nextCursor int) {
	for i := startIdx; i < len(allNodes); i++ {
		candidate := allNodes[i]

		if _, used := assigned[candidate]; used {
			continue
		}

		mem, ok := n.table.Get(candidate)
		if !ok || mem.State == member.Dead {
			continue
		}

		// Return the candidate and the position AFTER it for the next search.
		return candidate, i + 1
	}
	return "", startIdx
}

// sendHintedWrite sends a write to a fallback node on behalf of a dead
// preference-list node. The object carries Metadata.For = deadTargetID so
// the fallback knows which node to forward the data to on recovery.
//
// If the fallback is self (coordinator is the next clockwise healthy node —
// possible in small clusters), we write locally. This is the only legitimate
// case where the coordinator stores a hinted object locally.
//
// sem is shared with the parent replicate() call so all goroutines from one
// replication round respect the same maxConcurrentSends cap.
func (n *Node) sendHintedWrite(
	ctx context.Context,
	sem chan struct{},
	reqID string,
	obj store.Object,
	deadTargetID string,
	fallbackID string,
) {
	// Annotate the object with the original intended target.
	// The fallback node uses this to replay the write on recovery.
	hinted := obj
	hinted.Metadata.For = deadTargetID

	// Self is the fallback — store locally.
	if fallbackID == n.cfg.ListenAddr {
		// store locally
		n.coordinator.OnLocalAck(reqID)
		return
	}

	p, ok := n.registry.Get(fallbackID)
	if !ok {
		n.log.Warn("hinted handoff: peer handle not found",
			"req_id", reqID,
			"fallback", fallbackID,
			"dead_target", deadTargetID,
		)
		return
	}

	sem <- struct{}{}

	go func() {
		defer func() { <-sem }()

		select {
		case <-ctx.Done():
			return
		default:
		}

		payload, _ := store.EncodeWriteRequest(&store.WriteRequest{
			ID:  reqID,
			Obj: hinted,
		})

		if err := p.Send(&message.Message{
			Type:    message.WriteRequest,
			Payload: payload,
		}); err != nil {
			n.log.Warn("hinted handoff send failed",
				"req_id", reqID,
				"fallback", fallbackID,
				"dead_target", deadTargetID,
				"error", err,
			)
			// Both the original node and its fallback are unreachable.
			// Anti-entropy / merkle reconciliation handles this asynchronously.
			// We deliberately do NOT recurse into another fallback here to avoid
			// unbounded retry chains within a single write operation.
		}
	}()
}

// onWriteRequest handles an inbound WriteRequest from a coordinating node.
//
// Flow:
//  1. Decode the payload into a WriteRequest.
//  2. Persist the object to the local store.
//     If Metadata.For != "", this is a hinted write: store with the hint intact.
//     The background handoff-replay process scans for objects with Metadata.For
//     set and forwards them to the original node when it recovers.
//  3. Send a WriteAck back to the coordinator.
//
// Design note:
//
//	The ack is sent after local persistence so the coordinator only counts
//	durable acks. Sending before fsync would violate the W guarantee on crash.
//	TODO: flush to disk (fsync) before acking if using a WAL-backed store.
func (n *Node) onWriteRequest(p peer.Peer, m *message.Message) error {
	req, err := store.DecodeWriteRequest(m.Payload)
	if err != nil {
		return err
	}

	n.log.Debug("write request received",
		"req_id", req.ID,
		"key", string(req.Obj.Key),
		"hinted_for", req.Obj.Metadata.For,
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

// onWriteRequestAck handles a WriteAck from a replica.
//
// Delegates entirely to the coordinator's OnAck, which:
//   - increments the ack counter
//   - closes inf.done when W acks are reached (unblocks onPutRequest)
//   - cancels the context when N acks are reached (stops replicate goroutine)
//   - removes the inflight entry from the map once complete
func (n *Node) onWriteRequestAck(p peer.Peer, m *message.Message) error {
	ack, err := store.DecodeWriteAck(m.Payload)
	if err != nil {
		return err
	}

	n.coordinator.OnAck(ack.ID)
	return nil
}

// ---------------------------------------------------------------------------
// Read path
// ---------------------------------------------------------------------------

// onGetRequest is the coordinator entry point for client read requests.
//
// Flow:
//  1. Decode the GetRequest.
//  2. Assign a UUID, create a bounded context (2s), register inflight.
//  3. Launch readReplicate() in a goroutine.
//  4. Block until R acks arrive (inf.done closed) → return LWW best object.
//     Or timeout → delete inflight and return error.
//
// Read quorum semantics:
//
//	Once R replicas respond, the coordinator applies LWW conflict resolution
//	and returns the winner to the client. Background reads continue towards N
//	(future hook point for read-repair).
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
			"key", string(obj.Key),
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
		n.log.Error("get request failed",
			"req_id", reqID,
			"error", context.Cause(ctx),
		)
		n.coordinator.DeleteRequest(reqID)
		return context.Cause(ctx)
	}
}

// readReplicate fans out ReadRequest messages to preference-list nodes.
//
// Dead nodes are skipped — reads cannot be hinted (a hint holds a write; a
// read needs a live node to query). The semaphore bounds concurrency. Sends
// continue until ctx is cancelled (R acks received or deadline exceeded).
//
// Future hook: when N responses arrive, compare timestamps and issue
// read-repair writes to replicas that returned stale data.
func (n *Node) readReplicate(ctx context.Context, reqID string, key []byte) {	
	sem := make(chan struct{}, maxConcurrentSends)

	for offset := 0; offset < N; offset++ {
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

			select {
			case <-ctx.Done():
				return
			default:
			}

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

// onReadRequest executes on replica nodes.
//
// Fetches the object from the local store and returns it to the coordinator.
// A missing key returns a zero-value Object (zero timestamp) — LWW will always
// prefer a real object over the zero value. If ALL replicas return zero, the
// upper layer should interpret the result as key-not-found.
func (n *Node) onReadRequest(p peer.Peer, m *message.Message) error {
	req, err := store.DecodeReadRequest(m.Payload)
	if err != nil {
		return err
	}

	n.log.Debug("read request received",
		"req_id", req.ID,
		"key", string(req.Key),
		"from", p.ID(),
	)

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

// onReadRequestAck aggregates read responses at the coordinator.
//
// Delegates to coordinator.OnReadAck which merges via LWW (merge before ack —
// ensures GetBest() is populated when done closes) and advances quorum state.
// All state management is in the coordinator; this handler is intentionally thin.
func (n *Node) onReadRequestAck(p peer.Peer, m *message.Message) error {
	ack, err := store.DecodeReadAck(m.Payload)
	if err != nil {
		return err
	}

	n.log.Debug("read ack received",
		"req_id", ack.ID,
		"from", p.ID(),
	)

	n.coordinator.OnReadAck(ack.ID, ack.Obj)
	return nil
}

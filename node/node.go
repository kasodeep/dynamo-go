package node

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/kasodeep/dynamo-go/codec"
	"github.com/kasodeep/dynamo-go/member"
	"github.com/kasodeep/dynamo-go/message"
	"github.com/kasodeep/dynamo-go/peer"
	"github.com/kasodeep/dynamo-go/registry"
	"github.com/kasodeep/dynamo-go/router"
	"github.com/kasodeep/dynamo-go/transport"
)

// randome peers.
var k int = 2

// Node is the remote node or a server providing the cluster service.
// Each node belongs to a ring setup, maintaining registry of peers, it's own transport.
// For each message type we must also add the router handler to handle it.
//
// Context:
//   - It provides structured concurrecy, where every go routine can be cancelled and called of gracefully during shutdown.
//   - The New func initializes it with cancel channel.
type Node struct {
	cfg Config
	log *slog.Logger

	transport transport.Transport
	registry  *registry.Registry
	router    *router.Router
	table     *member.Table

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// Initializes a new node, by configuring the defaults, context and the router.
func New(cfg Config, t transport.Transport, log *slog.Logger) *Node {
	cfg.defaults()
	ctx, cancel := context.WithCancel(context.Background())

	n := &Node{
		cfg:       cfg,
		log:       log,
		transport: t,
		registry:  registry.New(),
		router:    router.New(),
		table:     member.New(),
		ctx:       ctx,
		cancel:    cancel,
	}

	n.router.Handle(message.Handshake, n.onHandshake)
	n.router.Handle(message.Ping, n.onPing)
	n.router.Handle(message.Pong, n.onPong)

	n.router.Handle(message.Gossip, n.onGossip)

	return n
}

// Starts the node, to listen of the server address, plus dial to bootstrap nodes.
// It starts to Listen on the transport, adds to wg and calls a go routine to accept connections.
// Dials a connection with retry to bootstrap nodes.
func (n *Node) Start() error {
	if err := n.transport.Listen(n.cfg.ListenAddr); err != nil {
		return err
	}
	n.log.Info("listening", "addr", n.cfg.ListenAddr)

	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		n.acceptLoop()
	}()

	for _, addr := range n.cfg.BootstrapAddrs {
		if addr == "" || addr == n.cfg.ListenAddr {
			continue
		}
		go n.dialWithRetry(addr)
	}

	n.startLoops()
	return nil
}

// Provides a way to stop the node, by closing the context, so that all go routines are collected.
// Closes the transport connection to stop listening, and waits for the go routines to complete.
func (n *Node) Stop() {
	n.cancel()
	n.transport.Close()
	n.wg.Wait()
}

// Returns the length of the number of peers (nodes)
func (n *Node) PeerCount() int {
	return n.registry.Len()
}

// Starts accepting the conn, and generates a peer.
// Serves the connection
func (n *Node) acceptLoop() {
	for {
		p, err := n.transport.Accept()
		if err != nil {
			select {
			case <-n.ctx.Done():
				return
			default:
				n.log.Error("accept error", "err", err)
				continue
			}
		}

		n.wg.Add(1)
		go func() {
			defer n.wg.Done()
			n.serveConn(p)
		}()
	}
}

// Dial with retry uses exponential backoff, to connect to the remote node.
// Sends the handshake message with ListenAddr (as our peerID)
func (n *Node) dialWithRetry(addr string) {
	backoff := time.Second
	for {
		select {
		case <-n.ctx.Done():
			return
		default:
		}

		p, err := n.transport.Dial(addr)
		if err != nil {
			n.log.Warn("dial failed, retrying", "addr", addr, "backoff", backoff, "err", err)
			select {
			case <-n.ctx.Done():
				return
			case <-time.After(backoff):
			}
			if backoff < 60*time.Second {
				backoff *= 2
			}
			continue
		}

		// Handshake: tell the remote our stable ID (listen addr).
		if err := p.Send(&message.Message{
			Type:    message.Handshake,
			Payload: []byte(n.cfg.ListenAddr),
		}); err != nil {
			n.log.Warn("handshake send failed", "addr", addr, "err", err)
			p.Close()
			continue
		}

		n.log.Info("dialed peer", "addr", addr)
		n.wg.Add(1)
		go func() {
			defer n.wg.Done()
			n.serveConn(p)
		}()
		return
	}
}

// Serves the connection, so that we can receive data from the peer.
// It dispatches the message router, to appropriately handle different types of message.
func (n *Node) serveConn(p peer.Peer) {
	defer func() {
		if id := p.ID(); id != "" {
			n.registry.RemoveIfMatch(id, p)
			n.log.Info("peer disconnected", "id", id)
		}
		p.Close()
	}()

	for {
		msg, err := p.Recv()
		if err != nil {
			select {
			case <-n.ctx.Done():
			default:
				n.log.Warn("recv error", "peer", p.RemoteAddr(), "err", err)
			}
			return
		}

		if id := p.ID(); id != "" {
			n.table.MarkAlive(id)
		}

		if err := n.router.Dispatch(p, msg); err != nil {
			n.log.Warn("dispatch error", "peer", p.RemoteAddr(), "type", msg.Type, "err", err)
		}
	}
}

// Starts the ping, failure and gossip loops on different go routines for each node.
func (n *Node) startLoops() {
	if n.cfg.PingInterval > 0 {
		n.wg.Add(1)
		go func() {
			defer n.wg.Done()
			n.pingLoop()
		}()
	}

	if n.cfg.FailCheckInterval > 0 {
		n.wg.Add(1)
		go func() {
			defer n.wg.Done()
			n.failureDetector()
		}()
	}

	if n.cfg.GossipInterval > 0 {
		n.wg.Add(1)
		go func() {
			defer n.wg.Done()
			n.gossipLoop()
		}()
	}
}

// Periodically depending on ping interval checks the peers and accepts pong from them.
func (n *Node) pingLoop() {
	tick := time.NewTicker(n.cfg.PingInterval)
	defer tick.Stop()
	for {
		select {
		case <-n.ctx.Done():
			return
		case <-tick.C:
			peers := n.registry.RandomSubset(k)
			for _, p := range peers {
				_ = p.Send(&message.Message{Type: message.Ping})
			}
		}
	}
}

func (n *Node) failureDetector() {
	ticker := time.NewTicker(n.cfg.FailCheckInterval)

	for {
		select {
		case <-ticker.C:
			now := time.Now()

			for _, m := range n.table.Snapshot() {
				delta := now.Sub(m.LastSeen)

				if delta > n.cfg.SuspectInterval && m.State == member.Alive {
					n.table.UpdateState(m.ID, member.Suspect)
				}

				if delta > n.cfg.DeadInterval && m.State == member.Suspect {
					n.table.UpdateState(m.ID, member.Dead)
				}
			}
		case <-n.ctx.Done():
			return
		}
	}
}

func (n *Node) gossipLoop() {
	ticker := time.NewTicker(n.cfg.GossipInterval)

	for {
		select {
		case <-ticker.C:
			peers := n.registry.RandomSubset(k) // pick k random peers
			snapshot := n.table.Snapshot()

			for _, p := range peers {
				_ = p.Send(&message.Message{
					Type:    message.Gossip,
					Payload: codec.EncodeMembers(snapshot),
				})
			}

		case <-n.ctx.Done():
			return
		}
	}
}

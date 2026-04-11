package node

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/kasodeep/dynamo-go/message"
	"github.com/kasodeep/dynamo-go/peer"
	"github.com/kasodeep/dynamo-go/registry"
	"github.com/kasodeep/dynamo-go/router"
	"github.com/kasodeep/dynamo-go/transport"
)

type Config struct {
	// ListenAddr is both the bind address AND the stable node ID.
	ListenAddr     string
	BootstrapAddrs []string

	// DialTimeout for outbound connections. Default 5s.
	DialTimeout time.Duration

	// PingInterval keeps connections alive. 0 disables. Default 10s.
	PingInterval time.Duration
}

func (c *Config) defaults() {
	if c.DialTimeout == 0 {
		c.DialTimeout = 5 * time.Second
	}
	if c.PingInterval == 0 {
		c.PingInterval = 10 * time.Second
	}
}

type Node struct {
	cfg       Config
	log       *slog.Logger
	transport transport.Transport
	registry  *registry.Registry
	router    *router.Router

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func New(cfg Config, t transport.Transport, log *slog.Logger) *Node {
	cfg.defaults()
	ctx, cancel := context.WithCancel(context.Background())

	n := &Node{
		cfg:       cfg,
		log:       log,
		transport: t,
		registry:  registry.New(),
		router:    router.New(),
		ctx:       ctx,
		cancel:    cancel,
	}

	n.router.Handle(message.Handshake, n.onHandshake)
	n.router.Handle(message.Ping, n.onPing)
	n.router.Handle(message.Pong, n.onPong)

	return n
}

// Handle registers an application-level message handler.
// Must be called before Start.
func (n *Node) Handle(msgType uint8, fn router.HandlerFunc) {
	n.router.Handle(msgType, fn)
}

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

	if n.cfg.PingInterval > 0 {
		n.wg.Add(1)
		go func() {
			defer n.wg.Done()
			n.pingLoop()
		}()
	}

	return nil
}

func (n *Node) Stop() {
	n.cancel()
	n.transport.Close()
	n.wg.Wait()
}

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

func (n *Node) PeerCount() int {
	return n.registry.Len()
}

// --- internal ---

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

		if err := n.router.Dispatch(p, msg); err != nil {
			n.log.Warn("dispatch error", "peer", p.RemoteAddr(), "type", msg.Type, "err", err)
		}
	}
}

func (n *Node) evict(p peer.Peer) {
	n.registry.RemoveIfMatch(p.ID(), p)
	p.Close()
}

func (n *Node) pingLoop() {
	tick := time.NewTicker(n.cfg.PingInterval)
	defer tick.Stop()
	for {
		select {
		case <-n.ctx.Done():
			return
		case <-tick.C:
			n.Broadcast(&message.Message{Type: message.Ping})
		}
	}
}

// --- built-in handlers ---

func (n *Node) onHandshake(p peer.Peer, msg *message.Message) error {
	id := string(msg.Payload)
	if id == "" {
		return fmt.Errorf("node: empty handshake ID from %s", p.RemoteAddr())
	}
	if n.registry.Has(id) {
		n.log.Info("duplicate connection, dropping", "id", id)
		return fmt.Errorf("node: duplicate peer %s", id)
	}
	p.SetID(id)
	n.registry.Add(id, p)
	n.log.Info("peer registered", "id", id)

	// Reply with our own ID so the dialer can register us too.
	return p.Send(&message.Message{
		Type:    message.Handshake,
		Payload: []byte(n.cfg.ListenAddr),
	})
}

func (n *Node) onPing(p peer.Peer, _ *message.Message) error {
	return p.Send(&message.Message{Type: message.Pong})
}

func (n *Node) onPong(_ peer.Peer, _ *message.Message) error {
	return nil
}

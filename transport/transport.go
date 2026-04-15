// Package transport defines the abstraction for managing network connectivity
// between nodes in the cluster.
//
// A Transport is responsible for:
//   - Listening for inbound connections
//   - Accepting and wrapping them as Peer instances
//   - Establishing outbound connections via dialing
//
// It decouples the networking layer from higher-level protocols,
// enabling pluggable implementations (TCP, QUIC, TLS, in-memory, etc.).
//
// Lifecycle:
//  1. Listen(addr) starts accepting incoming connections
//  2. Accept() blocks until a new Peer is available
//  3. Dial(addr) establishes outbound connections
//  4. Close() shuts down the transport and all active listeners
//
// Concurrency:
//   - Accept may be called from a single goroutine (typical).
//   - Dial must be safe for concurrent use.
//   - Close must be safe and terminate all blocking operations.
package transport

import "github.com/kasodeep/dynamo-go/peer"

// Transport abstracts connection lifecycle management for node-to-node communication.
//
// It provides a uniform interface over different networking stacks,
// allowing the system to remain agnostic of the underlying transport.
type Transport interface {
	Listen(addr string) error
	Accept() (peer.Peer, error)
	Dial(addr string) (peer.Peer, error)
	Close() error
}

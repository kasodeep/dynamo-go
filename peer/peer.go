// Package peer defines the abstraction for a remote node connection
// in the cluster.
//
// A Peer represents a single bidirectional communication channel between
// two nodes. It is intentionally minimal and focuses on message transport,
// leaving higher-level concerns (membership, replication, routing) to other layers.
//
// Design goals:
//   - Transport-agnostic interface (TCP, QUIC, in-memory, etc.)
//   - Clear separation between connection identity and network address
//   - Safe concurrent usage for outbound messaging
//
// Concurrency:
//   - Implementations MUST guarantee that Send is safe for concurrent use.
//   - Recv is typically expected to be called from a single reader loop.
//   - Close must be idempotent and safe under concurrent access.
package peer

import "github.com/kasodeep/dynamo-go/message"

// Peer represents a live connection to a remote node.
//
// It encapsulates both identity (logical node ID) and transport-level
// communication (send/receive). The ID is typically established during
// an application-level handshake after connection establishment.
type Peer interface {
	ID() string
	SetID(string)

	Send(*message.Message) error

	Recv() (*message.Message, error)
	RemoteAddr() string

	Close() error
}

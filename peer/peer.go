package peer

import "github.com/kasodeep/dynamo-go/message"

// Peer is a single remote node connection.
// Implementations must be safe for concurrent Send calls.
type Peer interface {
	// ID returns the stable listen address of the remote node.
	// Empty until the handshake completes.
	ID() string
	SetID(string)

	Send(*message.Message) error
	Recv() (*message.Message, error)

	// RemoteAddr is the ephemeral address (for logging).
	RemoteAddr() string

	Close() error
}

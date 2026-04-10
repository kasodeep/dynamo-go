package transport

import "github.com/kasodeep/dynamo-go/peer"

// Transport abstracts connection lifecycle management for node-to-node communication,
// enabling listening, accepting inbound peers, and dialing outbound peers.
type Transport interface {
	Listen(addr string) error
	Accept() (peer.Peer, error)
	Dial(addr string) (peer.Peer, error)
	Close() error
}

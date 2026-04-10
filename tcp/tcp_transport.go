package tcp

import (
	"crypto/tls"
	"fmt"
	"net"

	"github.com/kasodeep/dynamo-go/peer"
)

// Transport implements the Transport interface over TCP, with optional TLS support,
// handling listener setup, peer acceptance, and outbound connections.
type Transport struct {
	ln     net.Listener
	tlsCfg *tls.Config // nil → plain TCP
}

// New initializes a TCP transport, optionally enabling TLS if configuration is provided.
func New(tlsCfg *tls.Config) *Transport {
	return &Transport{tlsCfg: tlsCfg}
}

// Listen binds to the given address and starts listening for incoming connections over TCP/TLS.
func (t *Transport) Listen(addr string) error {
	var err error
	if t.tlsCfg != nil {
		t.ln, err = tls.Listen("tcp", addr, t.tlsCfg)
	} else {
		t.ln, err = net.Listen("tcp", addr)
	}
	if err != nil {
		return fmt.Errorf("tcp: listen %s: %w", addr, err)
	}
	return nil
}

// Accept waits for and returns the next inbound peer connection.
func (t *Transport) Accept() (peer.Peer, error) {
	conn, err := t.ln.Accept()
	if err != nil {
		return nil, err
	}
	return NewPeer(conn), nil
}

// Dial establishes an outbound connection to the given address over TCP/TLS.
func (t *Transport) Dial(addr string) (peer.Peer, error) {
	var conn net.Conn
	var err error
	if t.tlsCfg != nil {
		conn, err = tls.Dial("tcp", addr, t.tlsCfg)
	} else {
		conn, err = net.Dial("tcp", addr)
	}
	if err != nil {
		return nil, fmt.Errorf("tcp: dial %s: %w", addr, err)
	}
	return NewPeer(conn), nil
}

// Close shuts down the listener and releases underlying resources.
func (t *Transport) Close() error {
	if t.ln != nil {
		return t.ln.Close()
	}
	return nil
}

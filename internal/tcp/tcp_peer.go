// Package tcp provides tcp implementations of the Peer and Transport contracts.
//
// For Peer, it uses the net.Conn, bufio.Writer and mutex to ensure concurrent writes don't
// mix up.
//
// Transport consist of the Listener, which allows to accept the inbound connections, wrap them as peers.
// It also provides a way to dial connection to the remote nodes.
package tcp

import (
	"bufio"
	"net"
	"sync"

	"github.com/kasodeep/dynamo-go/internal/codec"
	"github.com/kasodeep/dynamo-go/internal/message"
)

// Peer represents a single TCP connection to a remote node.
//
// It provides a thin abstraction over net.Conn with:
//   - Buffered writes (via bufio.Writer)
//   - Serialized Send() operations (via mutex)
//   - A logical peer identity (set after handshake)
//
// Concurrency model:
//   - Send() is safe for concurrent use by multiple goroutines.
//   - Recv() must be called from a single dedicated reader goroutine.
//   - ID() / SetID() are NOT thread-safe unless externally synchronized.
//
// Protocol model:
//   - Messages are encoded/decoded using the codec package.
//   - Each message is length-prefixed (handled inside codec).
type Peer struct {
	conn net.Conn
	bw   *bufio.Writer
	mu   sync.Mutex // guards bw + conn.Write
	id   string
}

// Provides a new peer abstraction wrapping the conn.
//
// We wrap the conn with a bufio writer, so that using Flush(), we can provide the entire msg at once.
// Use conn directly for very low latency or low messaging.
func NewPeer(conn net.Conn) *Peer {
	return &Peer{
		conn: conn,
		bw:   bufio.NewWriterSize(conn, 64<<10),
	}
}

func (p *Peer) ID() string {
	return p.id
}

func (p *Peer) SetID(id string) {
	p.id = id
}

func (p *Peer) RemoteAddr() string {
	return p.conn.RemoteAddr().String()
}

// Send encodes and writes a message to the peer.
//
// It is safe for concurrent use:
//   - A mutex ensures that each message is written atomically
//     (header + payload are not interleaved with other writes).
//
// Write flow:
//
//	message → bufio buffer → Flush() → TCP connection
//
// Returns an error if encoding, writing, or flushing fails.
func (p *Peer) Send(msg *message.Message) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := codec.Write(p.bw, msg); err != nil {
		return err
	}
	return p.bw.Flush()
}

// Recv blocks and reads the next message from the peer.
//
// This should be run in a single dedicated goroutine.
//
// Read flow:
//
//	TCP connection → codec.Read → message
//
// Returns:
//   - (*message.Message, nil) on success
//   - (nil, error) on EOF or decode failure
func (p *Peer) Recv() (*message.Message, error) {
	return codec.Read(p.conn)
}

func (p *Peer) Close() error {
	return p.conn.Close()
}

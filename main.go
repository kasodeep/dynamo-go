package main

import (
	"log/slog"
	"os"
	"time"

	"github.com/kasodeep/dynamo-go/message"
	"github.com/kasodeep/dynamo-go/node"
	"github.com/kasodeep/dynamo-go/peer"
	"github.com/kasodeep/dynamo-go/tcp"
)

const appMsg uint8 = 0x10

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, nil))

	n1 := makeNode(":4001", nil, log.With("node", ":4001"))
	n2 := makeNode(":4002", []string{":4001"}, log.With("node", ":4002"))
	n3 := makeNode(":4003", []string{":4001", ":4002"}, log.With("node", ":4003"))

	if err := n1.Start(); err != nil {
		log.Error("n1 start", "err", err)
		os.Exit(1)
	}
	if err := n2.Start(); err != nil {
		log.Error("n2 start", "err", err)
		os.Exit(1)
	}
	if err := n3.Start(); err != nil {
		log.Error("n3 start", "err", err)
		os.Exit(1)
	}

	time.Sleep(500 * time.Millisecond) // let handshakes complete

	log.Info("peer counts",
		"n1", n1.PeerCount(),
		"n2", n2.PeerCount(),
		"n3", n3.PeerCount(),
	)

	// n1 broadcasts; n2 and n3 should receive.
	n1.Broadcast(&message.Message{
		Type:    appMsg,
		Payload: []byte("hello from n1"),
	})

	time.Sleep(200 * time.Millisecond)

	n1.Stop()
	n2.Stop()
	n3.Stop()
}

func makeNode(addr string, bootstrap []string, log *slog.Logger) *node.Node {
	t := tcp.New(nil) // nil = plain TCP; pass tls.Config for TLS
	
	n := node.New(node.Config{
		ListenAddr:     addr,
		BootstrapAddrs: bootstrap,
	}, t, log)

	n.Handle(appMsg, func(p peer.Peer, msg *message.Message) error {
		log.Info("received", "from", p.ID(), "payload", string(msg.Payload))
		return nil
	})

	return n
}

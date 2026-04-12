package main

import (
	"flag"
	"log/slog"
	"os"
	"strings"

	"github.com/kasodeep/dynamo-go/node"
	"github.com/kasodeep/dynamo-go/tcp"
)

func main() {
	addr := flag.String("addr", ":4001", "listen address")
	bootstrap := flag.String("bootstrap", "", "comma-separated bootstrap nodes")
	flag.Parse()

	log := slog.New(
		slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
			Level: slog.LevelDebug,
		}),
	).With("node", *addr)

	var peers []string
	if *bootstrap != "" {
		peers = strings.Split(*bootstrap, ",")
	}

	t := tcp.New(nil)

	n := node.New(node.Config{
		ListenAddr:     *addr,
		BootstrapAddrs: peers,
	}, t, log)

	if err := n.Start(); err != nil {
		log.Error("start failed", "err", err)
		os.Exit(1)
	}

	select {} // block forever
}

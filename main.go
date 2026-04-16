package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/kasodeep/dynamo-go/node"
	"github.com/kasodeep/dynamo-go/tcp"
)

func main() {
	addr := flag.String("addr", ":4001", "listen address")
	bootstrap := flag.String("bootstrap", "", "comma-separated bootstrap nodes")
	flag.Parse()

	log := slog.New(
		slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{}),
	).With("node", *addr)

	var peers []string
	if *bootstrap != "" {
		peers = strings.Split(*bootstrap, ",")
	}

	ctx := context.Background()

	t := tcp.New(nil)
	cfg := node.Config{
		ListenAddr:     *addr,
		BootstrapAddrs: peers,
	}

	n := node.New(ctx, cfg, t, log)

	if err := n.Start(); err != nil {
		log.Error("start failed", "err", err)
		os.Exit(1)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	log.Info("node started")

	sig := <-sigCh
	signal.Stop(sigCh)

	log.Info("shutdown signal received", "signal", sig.String())

	n.Stop()

	log.Info("node stopped cleanly")
}

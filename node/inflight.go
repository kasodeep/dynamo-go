package node

import "context"

type inflight struct {
	acks   int32
	needed int32
	done   chan struct{}
	cancel context.CancelFunc
}

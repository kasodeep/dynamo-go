package router

import (
	"fmt"

	"github.com/kasodeep/dynamo-go/internal/message"
	"github.com/kasodeep/dynamo-go/internal/peer"
)

// HandlerFunc defines the function signature for processing incoming messages from a peer.
type HandlerFunc func(p peer.Peer, msg *message.Message) error

// Router maps message types to their corresponding handlers for dispatching.
type Router struct {
	handlers map[uint8]HandlerFunc
}

// New initializes and returns an empty message router.
func New() *Router {
	return &Router{handlers: make(map[uint8]HandlerFunc)}
}

// Handle registers a handler function for a specific message type.
func (r *Router) Handle(msgType uint8, fn HandlerFunc) {
	r.handlers[msgType] = fn
}

// Dispatch routes the message to its registered handler based on message type.
func (r *Router) Dispatch(p peer.Peer, msg *message.Message) error {
	fn, ok := r.handlers[msg.Type]
	if !ok {
		return fmt.Errorf("router: unhandled message type 0x%02x", msg.Type)
	}
	return fn(p, msg)
}

package message

const (
	Handshake uint8 = 0x01
	Ping      uint8 = 0x02
	Pong      uint8 = 0x03

	// Reserve 0x10+ for application-defined types.
	Gossip uint8 = 0x10
)

// Message defines a minimal wire-level structure with a type identifier and raw payload.
// It is used for protocol-level communication, supporting control (handshake/ping/pong) and extensible message types.
type Message struct {
	Type    uint8
	Payload []byte
}

// Package message represents the contract of types of messages that can be send and detected and deducted over the network.
package message

// 0x01 - 0x09 for default wiring and handshake messages.
// 0x10+ reserved for application-defined types.
const (
	Handshake uint8 = 0x01
	Ping      uint8 = 0x02
	Pong      uint8 = 0x03

	Gossip          uint8 = 0x10
	WriteRequest    uint8 = 0x11
	WriteRequestAck uint8 = 0x12
	ReadRequest     uint8 = 0x13
	ReadRequestAck  uint8 = 0x14

	PutRequest uint8 = 0x20
	GetRequest uint8 = 0x21
)

// Message defines a minimal wire-level structure with a type identifier and raw payload.
// It is used for protocol-level communication.
type Message struct {
	Type    uint8
	Payload []byte
}

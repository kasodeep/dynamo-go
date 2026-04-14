package message

const (
	Handshake uint8 = 0x01
	Ping      uint8 = 0x02
	Pong      uint8 = 0x03

	// Reserve 0x10+ for application-defined types.
	Gossip          uint8 = 0x10
	WriteRequest    uint8 = 0x11
	WriteRequestAck uint8 = 0x12
	ReadRequest     uint8 = 0x13
	ReadRequestAck  uint8 = 0x14

	PutRequest uint8 = 0x20
	GetRequest uint8 = 0x21
)

// Message defines a minimal wire-level structure with a type identifier and raw payload.
// It is used for protocol-level communication, supporting control (handshake/ping/pong) and extensible message types.
type Message struct {
	Type    uint8
	Payload []byte
}

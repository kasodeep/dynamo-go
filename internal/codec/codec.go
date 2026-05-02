// Package codec implements a simple length-prefixed binary framing protocol.
//
// Wire format per message:
//
//	[1 byte type][4 bytes payload length (big-endian)][N bytes payload]
package codec

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/kasodeep/dynamo-go/message"
)

const maxPayload = 32 << 20 // 32 MiB hard cap

// Wrtie writes the msg to w according to the wireframe protocol.
// Callers should ensure that the payload len is at-max 32MiB, otherwise it throws an error.
func Write(w io.Writer, msg *message.Message) error {
	if len(msg.Payload) > maxPayload {
		return fmt.Errorf("codec: payload %d exceeds max %d", len(msg.Payload), maxPayload)
	}

	hdr := [5]byte{}
	hdr[0] = msg.Type
	binary.BigEndian.PutUint32(hdr[1:], uint32(len(msg.Payload)))

	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	if len(msg.Payload) > 0 {
		_, err := w.Write(msg.Payload)
		return err
	}
	return nil
}

// Read reads the msg from r and decodes it (according to payload length).
func Read(r io.Reader) (*message.Message, error) {
	var hdr [5]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return nil, err
	}

	msgType := hdr[0]
	length := binary.BigEndian.Uint32(hdr[1:])

	if length > maxPayload {
		return nil, fmt.Errorf("codec: declared payload %d exceeds max %d", length, maxPayload)
	}

	payload := make([]byte, length)
	if length > 0 {
		if _, err := io.ReadFull(r, payload); err != nil {
			return nil, err
		}
	}

	return &message.Message{Type: msgType, Payload: payload}, nil
}

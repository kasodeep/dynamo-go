package main

import (
	"fmt"
	"net"
	"time"

	"github.com/kasodeep/dynamo-go/codec"
	"github.com/kasodeep/dynamo-go/message"
	"github.com/kasodeep/dynamo-go/store"
)

func main() {
	conn, err := net.Dial("tcp", "localhost:4001")
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	// ---------- PUT ----------
	put := store.PutRequest{
		Key:   []byte("deep"),
		Value: []byte("bar"),
	}

	payload, _ := store.EncodePutRequest(&put)
	fmt.Println("Sending PUT")

	err = codec.Write(conn, &message.Message{
		Type:    message.PutRequest,
		Payload: payload,
	})
	if err != nil {
		panic(err)
	}

	time.Sleep(500 * time.Millisecond)

	// ---------- GET ----------
	get := store.GetRequest{
		Key: []byte("deep"),
	}

	payload, _ = store.EncodeGetRequest(&get)

	fmt.Println("Sending GET")

	err = codec.Write(conn, &message.Message{
		Type:    message.GetRequest,
		Payload: payload,
	})
	if err != nil {
		panic(err)
	}

	// ---------- READ RESPONSE ----------
	fmt.Println("Waiting for response...")

	resp, err := codec.Read(conn)
	if err != nil {
		panic(err)
	}

	ack, _ := store.DecodeReadAck(resp.Payload)

	fmt.Println("Response:")
	fmt.Println("Key:", string(ack.Obj.Key))
	fmt.Println("Value:", string(ack.Obj.Value))
}

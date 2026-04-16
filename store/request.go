package store

import (
	"encoding/json"
)

// PutRequest is the request sent by external gateway to any random node for storing the k,v pair
type PutRequest struct {
	Key   []byte `json:"key"`
	Value []byte `json:"value"`
}

// Decodes the PutRequest, assumes that the data was encoded using json formatting.
func DecodePutRequest(data []byte) (PutRequest, error) {
	var req PutRequest
	err := json.Unmarshal(data, &req)
	return req, err
}

// WriteRequest wraps the object with the ID of the inflight request.
// Allows the storing node in the cluster to return the ack.
type WriteRequest struct {
	ID  string `json:"id"`
	Obj Object `json:"obj"`
}

// WriteAck abstracts the ID of the inflight request, denoting (ack) the object is stored.
type WriteAck struct {
	ID string `json:"id"`
}

func EncodeWriteRequest(req *WriteRequest) ([]byte, error) {
	return json.Marshal(req)
}

func DecodeWriteRequest(data []byte) (WriteRequest, error) {
	var req WriteRequest
	err := json.Unmarshal(data, &req)
	return req, err
}

func EncodeWriteAck(ack *WriteAck) ([]byte, error) {
	return json.Marshal(ack)
}

func DecodeWriteAck(data []byte) (WriteAck, error) {
	var ack WriteAck
	err := json.Unmarshal(data, &ack)
	return ack, err
}

// Represents the get request being sent to get the object.
type GetRequest struct {
	Key   []byte `json:"key"`
	Value []byte `json:"value"`
}

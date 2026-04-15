package store

import (
	"encoding/json"
)

// Represents the request being sent to store the object.
type PutRequest struct {
	Key   []byte `json:"key"`
	Value []byte `json:"value"`
}

type WriteRequest struct {
	ID  string `json:"id"`
	Obj Object `json:"obj"`
}

type WriteAck struct {
	ID string `json:"id"`
}

// Represents the get request being sent to get the object.
type GetRequest struct {
	Key   []byte `json:"key"`
	Value []byte `json:"value"`
}

func DecodePutRequest(data []byte) (PutRequest, error) {
	var req PutRequest
	err := json.Unmarshal(data, &req)
	return req, err
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

func EncodeObject(obj Object) ([]byte, error) {
	return json.Marshal(obj)
}

func DecodeObject(data []byte) (Object, error) {
	var obj Object
	err := json.Unmarshal(data, &obj)
	return obj, err
}

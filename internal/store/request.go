package store

import (
	"encoding/json"
)

// PutRequest is sent by an external client (or gateway) to any node
// in the cluster to store a key-value pair.
type PutRequest struct {
	Key   []byte `json:"key"`
	Value []byte `json:"value"`
}

// EncodePutRequest serializes a PutRequest into JSON.
func EncodePutRequest(req *PutRequest) ([]byte, error) {
	return json.Marshal(req)
}

// DecodePutRequest deserializes JSON data into a PutRequest.
func DecodePutRequest(data []byte) (PutRequest, error) {
	var req PutRequest
	err := json.Unmarshal(data, &req)
	return req, err
}

// GetRequest is sent by a client to retrieve the value for a given key.
type GetRequest struct {
	Key []byte `json:"key"`
}

// EncodeGetRequest serializes a GetRequest into JSON.
func EncodeGetRequest(req *GetRequest) ([]byte, error) {
	return json.Marshal(req)
}

// DecodeGetRequest deserializes JSON data into a GetRequest.
func DecodeGetRequest(data []byte) (GetRequest, error) {
	var req GetRequest
	err := json.Unmarshal(data, &req)
	return req, err
}

// WriteRequest is used for inter-node communication during replication.
// It wraps the object along with a unique request ID so that acknowledgements
// can be correlated back to the originating coordinator.
type WriteRequest struct {
	ID  string `json:"id"`
	Obj Object `json:"obj"`
}

// EncodeWriteRequest serializes a WriteRequest into JSON.
func EncodeWriteRequest(req *WriteRequest) ([]byte, error) {
	return json.Marshal(req)
}

// DecodeWriteRequest deserializes JSON data into a WriteRequest.
func DecodeWriteRequest(data []byte) (WriteRequest, error) {
	var req WriteRequest
	err := json.Unmarshal(data, &req)
	return req, err
}

// WriteAck is sent by replica nodes back to the coordinator
// to indicate successful persistence of the object.
type WriteAck struct {
	ID string `json:"id"` // must match WriteRequest.ID
}

// EncodeWriteAck serializes a WriteAck into JSON.
func EncodeWriteAck(ack *WriteAck) ([]byte, error) {
	return json.Marshal(ack)
}

// DecodeWriteAck deserializes JSON data into a WriteAck.
func DecodeWriteAck(data []byte) (WriteAck, error) {
	var ack WriteAck
	err := json.Unmarshal(data, &ack)
	return ack, err
}

// ReadRequest is sent by the coordinator to replicas to fetch
// the latest version of an object. The ID is used for correlation.
type ReadRequest struct {
	ID  string `json:"id"`
	Key []byte `json:"key"`
}

// EncodeReadRequest serializes a ReadRequest into JSON.
func EncodeReadRequest(req *ReadRequest) ([]byte, error) {
	return json.Marshal(req)
}

// DecodeReadRequest deserializes JSON data into a ReadRequest.
func DecodeReadRequest(data []byte) (ReadRequest, error) {
	var req ReadRequest
	err := json.Unmarshal(data, &req)
	return req, err
}

// ReadAck is sent by replicas in response to a ReadRequest.
// It includes the object (if found) along with the request ID.
type ReadAck struct {
	ID  string `json:"id"`
	Obj Object `json:"obj"` // returned object (may be empty if not found)
}

// EncodeReadAck serializes a ReadAck into JSON.
func EncodeReadAck(ack *ReadAck) ([]byte, error) {
	return json.Marshal(ack)
}

// DecodeReadAck deserializes JSON data into a ReadAck.
func DecodeReadAck(data []byte) (ReadAck, error) {
	var ack ReadAck
	err := json.Unmarshal(data, &ack)
	return ack, err
}

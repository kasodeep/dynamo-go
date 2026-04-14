package codec

import (
	"encoding/json"

	"github.com/kasodeep/dynamo-go/member"
	"github.com/kasodeep/dynamo-go/store"
)

func EncodeMembers(members []member.Member) ([]byte, error) {
	return json.Marshal(members)
}

func DecodeMembers(data []byte) ([]member.Member, error) {
	var members []member.Member
	err := json.Unmarshal(data, &members)
	return members, err
}

func DecodePutRequest(data []byte) (store.PutRequest, error) {
	var req store.PutRequest
	err := json.Unmarshal(data, &req)
	return req, err
}

func EncodeWriteRequest(req *store.WriteRequest) ([]byte, error) {
	return json.Marshal(req)
}

func DecodeWriteRequest(data []byte) (store.WriteRequest, error) {
	var req store.WriteRequest
	err := json.Unmarshal(data, &req)
	return req, err
}

func EncodeWriteAck(ack *store.WriteAck) ([]byte, error) {
	return json.Marshal(ack)
}

func DecodeWriteAck(data []byte) (store.WriteAck, error) {
	var ack store.WriteAck
	err := json.Unmarshal(data, &ack)
	return ack, err
}

func EncodeObject(obj store.Object) ([]byte, error) {
	return json.Marshal(obj)
}

func DecodeObject(data []byte) (store.Object, error) {
	var obj store.Object
	err := json.Unmarshal(data, &obj)
	return obj, err
}

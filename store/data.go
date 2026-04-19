package store

import (
	"encoding/json"
	"time"
)

// Object represents the basic type supported by the database.
// It provides the key, value and the metadata (helpful in conflict handling)
type Object struct {
	Key      []byte   `json:"key"`
	Value    []byte   `json:"value"`
	Metadata Metadata `json:"metadata"`
}

// Metadata represents the timestamp for the latest write for the object.
// For represents the ID of the peer, to perform hinted handoff.
// Since we don't use pointer, internally, := creates a safe copy.
type Metadata struct {
	For       string    `json:"for"`
	Timestamp time.Time `json:"timestamp"`
}

// Encodes the object into json format.
func EncodeObject(obj Object) ([]byte, error) {
	return json.Marshal(obj)
}

// Decodes the object from the json format.
func DecodeObject(data []byte) (Object, error) {
	var obj Object
	err := json.Unmarshal(data, &obj)
	return obj, err
}

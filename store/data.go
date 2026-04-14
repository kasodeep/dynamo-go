package store

import "time"

// Object represents the basic type supported by the database.
// It provides the key, value and the metadata (helpful in conflict handling)
type Object struct {
	Key      []byte   `json:"key"`
	Value    []byte   `json:"value"`
	Metadata Metadata `json:"metadata"`
}

// Metadata represents the timestamp for the latest write for the object.
// For represents the ID of the peer, to perform hinted handoff.
type Metadata struct {
	For       string    `json:"for"`
	Timestamp time.Time `json:"timestamp"`
}

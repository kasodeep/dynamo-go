package store

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

// TODO: Add the responses as well.

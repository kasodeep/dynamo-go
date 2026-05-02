package member

import (
	"encoding/json"
	"time"
)

// State represents the liveness status of a cluster member.
type State string

const (
	// Alive indicates the node is responsive and healthy.
	Alive State = "ALIVE"

	// Suspect indicates the node is potentially failed.
	// This is a transient state pending confirmation.
	Suspect State = "SUSPECT"

	// Dead indicates the node is permanently considered failed.
	Dead State = "DEAD"
)

// worseState returns true if state `a` is considered worse (more severe)
// than state `b`, based on predefined ordering.
//
// Ordering: Dead > Suspect > Alive
func worseState(a, b State) bool {
	order := map[State]int{
		Alive:   0,
		Suspect: 1,
		Dead:    2,
	}
	return order[a] > order[b]
}

// Member represents a node in the cluster membership table.
//
// It is a logical representation of liveness, not a network connection.
// Version is used for conflict resolution during gossip merges.
type Member struct {
	ID       string    `json:"id"`
	State    State     `json:"state"`
	Version  uint64    `json:"version"`
	LastSeen time.Time `json:"last_seen"`
}

// NewMember returns a new Member initialized in Alive state.
//
// Version starts at 0 and LastSeen is set to the current time.
func NewMember(id string) *Member {
	return &Member{
		ID:       id,
		State:    Alive,
		Version:  0,
		LastSeen: time.Now(),
	}
}

// EncodeMembers serializes a slice of members into JSON.
//
// Used for gossip message payloads.
func EncodeMembers(members []Member) ([]byte, error) {
	return json.Marshal(members)
}

// DecodeMembers deserializes JSON into a slice of members.
//
// The returned slice is safe for further processing via Upsert.
func DecodeMembers(data []byte) ([]Member, error) {
	var members []Member
	err := json.Unmarshal(data, &members)
	return members, err
}

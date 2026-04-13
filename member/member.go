package member

import "time"

type State string

const (
	Alive   State = "ALIVE"
	Suspect State = "SUSPECT"
	Dead    State = "DEAD"
)

// Stores the info about the member of the cluster.
// Version and LastSeen allows us to mark the updates during gossip protocol consistently.
// Separate from Registry and Peer, since it represents membership not connection.
type Member struct {
	ID       string
	State    State
	Version  uint64
	LastSeen time.Time
}

// Returns a new member, marking it as alive with the base version of 0 and lastseen of now.
func NewMember(id string) *Member {
	return &Member{
		ID:       id,
		State:    Alive,
		Version:  0,
		LastSeen: time.Now(),
	}
}

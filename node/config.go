package node

import "time"

var ping = time.Second * 2   // how often we ping
var fail = time.Second * 2   // how often failure detector runs (same cadence)
var gossip = time.Second * 5 // gossip dissemination

// suspect after missing ~3 pings
// dead after missing ~3 more
var suspect = time.Second * 6 // > 3 * pingInterval
var dead = time.Second * 12   // > suspect + 3 * pingInterval

// It provides configuration to the nodes, with the listen addr, bootstrap nodes.
// PingInterval denotes a way to perodically check the peer conn.
// Suspect and Dead Interval denotes the time duration over which we consider network threshold as the failure of communication.
type Config struct {
	ListenAddr     string
	BootstrapAddrs []string

	PingInterval      time.Duration
	GossipInterval    time.Duration
	FailCheckInterval time.Duration

	SuspectInterval time.Duration
	DeadInterval    time.Duration
}

// Returns a new config with the provided params,
// For the default ping interval, pass the param as 0.
func NewConfig(ListenAddr string, BootstrapAddrs []string, PingInterval time.Duration) Config {
	cfg := Config{
		ListenAddr:     ListenAddr,
		BootstrapAddrs: BootstrapAddrs,
		PingInterval:   PingInterval,
	}

	cfg.defaults()
	return cfg
}

// Provides default config with ping interval as 1s.
func (c *Config) defaults() {
	if c.PingInterval == 0 {
		c.PingInterval = ping
	}
	c.GossipInterval = gossip
	c.FailCheckInterval = fail

	c.DeadInterval = dead
	c.SuspectInterval = suspect
}

package node

import "time"

var suspect = time.Duration(3)
var dead = time.Duration(6)
var fail = time.Duration(5)
var gossip = time.Duration(5)

// It provides configuration to the nodes, with the listen addr, bootstrap nodes.
// PingInterval denotes a way to perodically check the peer conn.
// Suspect and Dead Interval denotes the time duration over which we consider network threshold as the failure of communication.
type Config struct {
	ListenAddr     string
	BootstrapAddrs []string

	PingInterval      time.Duration
	SuspectInterval   time.Duration
	DeadInterval      time.Duration
	FailCheckInterval time.Duration
	GossipInterval    time.Duration
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
		c.PingInterval = 10 * time.Second
	}

	c.SuspectInterval = suspect * c.PingInterval
	c.DeadInterval = dead * c.PingInterval
	c.FailCheckInterval = fail
	c.GossipInterval = gossip
}

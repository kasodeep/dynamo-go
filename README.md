# Small Dynamo

## Cluster Setup

1. `message` provides a structed way to handle type, and the types can be extended.
2. `codec` provides a way to encode and decode messages in a structured way.
3. `peer` represents the single node connection and it has tcp implementation.
4. `transport` provides a to listen and dial connections to other nodes, to have peers.

### TCP Transport

- `tcp_transport` provides a TCP implementation of the transport layer, allowing nodes to communicate over TCP connections.
- `tcp_peer` represents a peer connection over TCP. It used mutex for concurrent write and bufio optimization.

### Registry

- `registry` abstracts the map of peers and provides thread-safe access to the peer connections.
- It allows for adding, removing, and retrieving peers in a concurrent environment.
- It uses consistent hashing using `treemap` to create and update the hash ring, plus maintains id to peer mapping for efficient lookups.

### Consistent Hashing

- `treemap` provides custom implementation of red black tree, which is balanced bst, with O(log n) for insert, delete, and search.

### Router

- `router` maps the message types to their corresponding handlers, allowing for organized message processing.
- The handler function takes the peer and the msg as arguments.

## Node

`node` is the main struct representing a node in the cluster. It contains:
- Configuration (`Config`)
- Logger (`slog.Logger`)
- Transport layer (`transport.Transport`)
- Registry of peers (`registry.Registry`)
- Membership table (`membership.Table`)
- Router for message handling (`router.Router`)
- Context and wait group for managing goroutines.

### Start

- Starts the listener and go routine to handle incoming connections. In turn spawns a servConn to read messages from the connection and route them to the appropriate handler.
- Dials to bootstrap nodes to join the cluster and sends handshake messages. In turns spawns servConn.

### Convergence

- Each node sends a handshake message to existing one, which in turn receives it back. (Registry updated)
- Not doing `indirect ping` in gossip, which allows us to check other nodes.

#### Loops

- Starts a ping loop, failure loop and gossip loop.

### Config

`Config` struct defines the configuration for a node, including:
- Listen address, Bootstrap nodes, intervals for now.

## Trade Offs

- By keeping the HTTP server “dumb” (random/round-robin), it doesn’t need cluster topology, so failures of primary nodes don’t break routing—any node can take over as coordinator.
- With sloppy quorum and hinted handoff, writes still succeed by going to healthy replicas and syncing later.
- The tradeoff is a bit of extra latency and internal hops, but you gain a more resilient and loosely coupled system, which fits well unless you’re building something ultra latency-sensitive like trading systems.

### Definitions

| Concept    | Meaning                                        |
| ---------- | ---------------------------------------------- |
| Peer       | “I have a TCP connection to this node”         |
| Registry   | “I can route requests to these nodes”          |
| Membership | “I believe these nodes are alive/suspect/dead” |

Fix logs
timings
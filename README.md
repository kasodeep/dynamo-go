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

- `registry` contains ring using `treemap` and the cluster using a simple map.
- Each function provides a way to modify the node or peer belonging the ring or cluster.
- Add and RemoveIfMatch works with both cluster and ring.
- ForEach, Len, RandomSubset() works with the cluster.
- NextFrom is important function and works with ring.

### Consistent Hashing

- `treemap` provides custom implementation of red black tree, which is balanced bst, with O(log n) for insert, delete, and search.
- We had the obsession to push, so we learned it, insert, rotation, and queries are clear.
- Trying to understand how delete works.

### Router

- `router` maps the message types to their corresponding handlers, allowing for organized message processing.

### Member

- `member` represents the node in the cluster, and stores the state or liveliness of that node.
- Updated when we receive a msg, or during handshake.
- Majorly works with gossip to send the state and failure loop to mark the nodes as Suspect or Alive.

## Node

- `node` is the main struct representing a node in the cluster.
- TODO: Ensure we have TLS for trusted end user (sender).

### Graceful Shutdown

## Protocols

    For all protocols assume two server x (started 1st) and y are talking to each other.
    For each protocol, we have two states to maintain, the registry and the table.
    If we have a peer with ID in the registry, they will be in the table (either alive or dead).

### Handshake

### Gossip Protocol

### Quorum Consensus

### Hinted Handoff

### Rebalancing


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

| Type category                     | Copy behavior       |
| --------------------------------- | ------------------- |
| `int`, `bool`, `struct` (no refs) | ✅ deep copy         |
| `*T` (pointer)                    | ❗ shared            |
| `[]T` (slice)                     | ❗ shared            |
| `map[K]V`                         | ❗ shared            |
| `chan T`                          | ❗ shared            |
| `time.Time`                       | ✅ safe (value type) |

-> Log (store)
-> Compaction
-> Handoff
-> Rebalancing


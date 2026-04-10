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

### Router

- `router` maps the message types to their corresponding handlers, allowing for organized message processing.
- The handler function takes the peer and the msg as arguments.

## Node

### Put

1. It receives a PutObjectMessage, we will not add to registry the peer since the first msg is not handshake from the server (load balancer).
2. It performs a write-ahead log (WAL) to ensure durability of the data.
3. It updates the in-memory data structure with the new value.
4. It replicates the data to other nodes in the cluster for fault tolerance. W < N.
5. It sends an acknowledgment back to the client once the write operation is successful.
6. We need Flush(), periodically or on pressure, to ensure the data is written to disk.

### Get

1. It receives a GetObjectMessage, we will not add to registry the peer since the first msg is not handshake from the server (load balancer).
2. It checks the in-memory data structure for the requested key.
3. It calls other N high rank nodes, and then returns (if multiple) values, it returns the most recent one based on the timestamp.
4. It returns the value to the client if found, or an error if the key does not exist.

## Server

- `server` managers the load balancing job, recieves incoming http get and put requests, and forwards them to the appropriate nodes based on the consistent hashing ring.

### Think
- At the end server knows which node to call. now once it calls that node via http or via tcp?
- well, we do listen and so use tcp, we get the new message types GetObjectMessage and PutObjectMessage.
- responsibility wise, server manages consistent hashing for load balancing.
- the node, on write performs wal, in memory update, and replication. on read, it performs in memory read, and if not found, it performs disk read.
- then we have failure detection, handling that via two concepts. but that is last first the top stuff.

## Trade Offs

- By keeping the HTTP server “dumb” (random/round-robin), it doesn’t need cluster topology, so failures of primary nodes don’t break routing—any node can take over as coordinator.
- With sloppy quorum and hinted handoff, writes still succeed by going to healthy replicas and syncing later.
- The tradeoff is a bit of extra latency and internal hops, but you gain a more resilient and loosely coupled system, which fits well unless you’re building something ultra latency-sensitive like trading systems.
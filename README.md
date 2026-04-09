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

### Node

TODO:
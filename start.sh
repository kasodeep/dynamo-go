#!/usr/bin/env bash
set -euo pipefail

# Kill any old processes on these ports (optional safety)
kill_port() {
  lsof -ti tcp:$1 | xargs -r kill -9 || true
}

kill_port 4001
kill_port 4002
kill_port 4003

echo "Starting nodes..."

# Start node 1 (no bootstrap)
go run main.go -addr=:4001 > node1.log 2>&1 &

sleep 1

# Start node 2 (bootstraps to node 1)
go run main.go -addr=:4002 -bootstrap=:4001 > node2.log 2>&1 &

sleep 1

# Start node 3 (bootstraps to node 1)
go run main.go -addr=:4003 -bootstrap=:4001 > node3.log 2>&1 &

echo "Nodes started."
echo "Logs:"
echo "  node1.log"
echo "  node2.log"
echo "  node3.log"

echo ""
echo "Use: tail -f node1.log (or node2.log, node3.log) to view logs"
echo "Press Ctrl+C to stop this script (nodes will keep running)"
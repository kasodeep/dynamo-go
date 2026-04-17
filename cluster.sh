#!/usr/bin/env bash

# Strict mode:
# -e  → exit on error
# -u  → undefined vars are errors
# -o pipefail → fail if any piped command fails
set -euo pipefail

# ---- CONFIG ----
BASE_PORT=4001
DEFAULT_NODES=3
BIN="./bin/node"        # compiled binary (preferred)
LOG_DIR="./logs"

mkdir -p "$LOG_DIR"

# ---- UTIL: Kill process on a port (graceful → force) ----
kill_port() {
  local port=$1

  # Get PIDs listening on this port
  PIDS=$(lsof -ti tcp:$port || true)

  if [ -n "$PIDS" ]; then
    echo "🔻 Stopping port $port (PID: $PIDS)"

    # Try graceful shutdown first
    kill $PIDS || true
    sleep 0.5

    # Force kill if still alive
    kill -9 $PIDS 2>/dev/null || true
  fi
}

# ---- UTIL: Start a node ----
start_node() {
  local port=$1
  local bootstrap=$2
  local logfile="$LOG_DIR/node$port.log"
  local pidfile="$LOG_DIR/node$port.pid"

  echo "🚀 Starting node :$port"

  # Ensure log file exists
  : > "$logfile"

  # Choose execution mode:
  if [ -f "$BIN" ]; then
    CMD="$BIN -addr=:$port"
    if [ -n "$bootstrap" ]; then
      CMD="$CMD -bootstrap=$bootstrap"
    fi
  else
    # fallback to go run
    CMD="go run ./cmd/node -addr=:$port"
    if [ -n "$bootstrap" ]; then
      CMD="$CMD -bootstrap=$bootstrap"
    fi
  fi

  # Run in background with logging
  eval "$CMD" >> "$logfile" 2>&1 &

  pid=$!
  echo $pid > "$pidfile"

  # Give process time to start
  sleep 0.5

  # Detect crash immediately
  if ! kill -0 $pid 2>/dev/null; then
    echo "❌ Node :$port crashed!"
    echo "---- LOG ($logfile) ----"
    cat "$logfile"
    exit 1
  fi

  echo "✅ Node :$port started (PID $pid)"
}

# ---- COMMAND HANDLER ----
case "${1:-start}" in

# ---- START DEFAULT CLUSTER ----
start)
  echo "Starting $DEFAULT_NODES nodes..."

  # Cleanup old processes
  for ((i=0; i<DEFAULT_NODES; i++)); do
    port=$((BASE_PORT + i))
    kill_port $port
  done

  # Start seed node (no bootstrap)
  start_node 4001 ""

  sleep 1

  # Start remaining nodes
  for ((i=1; i<DEFAULT_NODES; i++)); do
    port=$((BASE_PORT + i))
    start_node $port ":4001"
    sleep 0.5
  done

  echo ""
  echo "📡 Cluster started"
  echo "Logs: $LOG_DIR/node*.log"
  ;;

# ---- ADD NODE ----
add)
  port=${2:-}

  if [ -z "$port" ]; then
    echo "Usage: ./cluster.sh add 4004"
    exit 1
  fi

  kill_port $port
  start_node $port ":4001"
  ;;

# ---- REMOVE NODE ----
remove)
  port=${2:-}

  if [ -z "$port" ]; then
    echo "Usage: ./cluster.sh remove 4002"
    exit 1
  fi

  kill_port $port

  # Remove PID + log (optional)
  rm -f "$LOG_DIR/node$port.pid"

  echo "🗑️ Removed node :$port"
  ;;

# ---- STOP ALL ----
stop)
  echo "Stopping all nodes..."

  for port in {4001..4010}; do
    kill_port $port
  done

  echo "🛑 All nodes stopped"
  ;;

# ---- VIEW LOGS ----
logs)
  echo "📜 Tailing logs..."
  tail -f $LOG_DIR/node*.log
  ;;

# ---- STATUS ----
status)
  echo "Node status:"
  for port in {4001..4010}; do
    if lsof -i :$port >/dev/null 2>&1; then
      echo "✅ :$port running"
    fi
  done
  ;;

# ---- HELP ----
*)
  echo "Usage:"
  echo "  ./cluster.sh start        # start default cluster (3 nodes)"
  echo "  ./cluster.sh add 4004     # add node"
  echo "  ./cluster.sh remove 4002  # remove node"
  echo "  ./cluster.sh stop         # stop all nodes"
  echo "  ./cluster.sh logs         # tail logs"
  echo "  ./cluster.sh status       # show running nodes"
  ;;
esac
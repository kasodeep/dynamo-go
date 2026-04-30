#!/usr/bin/env bash

set -euo pipefail

# ---- CONFIG ----
BASE_PORT=4001
DEFAULT_NODES=3
BIN="./bin/node"
LOG_DIR="./logs"
STATE_FILE="./cluster.state"

mkdir -p "$LOG_DIR"
touch "$STATE_FILE"

# ---- UTIL: Read state ----
get_nodes() {
  cat "$STATE_FILE"
}

# ---- UTIL: Add node to state ----
add_node_state() {
  echo "$1" >> "$STATE_FILE"
}

# ---- UTIL: Remove node from state ----
remove_node_state() {
  grep -v "^$1$" "$STATE_FILE" > "$STATE_FILE.tmp" || true
  mv "$STATE_FILE.tmp" "$STATE_FILE"
}

# ---- UTIL: Clear state ----
clear_state() {
  : > "$STATE_FILE"
}

# ---- UTIL: Build bootstrap list ----
build_bootstrap() {
  local current_port=$1
  local peers=()

  while read -r port; do
    if [ -n "$port" ] && [ "$port" != "$current_port" ]; then
      peers+=(":$port")
    fi
  done < "$STATE_FILE"

  (IFS=,; echo "${peers[*]}")
}

# ---- UTIL: Start node ----
start_node() {
  local port=$1
  local bootstrap=$2

  local logfile="$LOG_DIR/node$port.log"
  local pidfile="$LOG_DIR/node$port.pid"

  echo "🚀 Starting node :$port"
  : > "$logfile"

  if [ -f "$BIN" ]; then
    CMD="$BIN -addr=:$port"
  else
    CMD="go run ./cmd/node -addr=:$port"
  fi

  if [ -n "$bootstrap" ]; then
    CMD="$CMD -bootstrap=$bootstrap"
  fi

  echo "   ↳ bootstrap: ${bootstrap:-<none>}"

  eval "$CMD" >> "$logfile" 2>&1 &

  pid=$!
  echo $pid > "$pidfile"

  sleep 0.5

  if ! kill -0 $pid 2>/dev/null; then
    echo "❌ Node :$port crashed!"
    cat "$logfile"
    exit 1
  fi

  # Only add to state AFTER successful start
  add_node_state "$port"

  echo "✅ Node :$port started (PID $pid)"
}

# ---- UTIL: Stop node ----
stop_node() {
  local port=$1
  local pidfile="$LOG_DIR/node$port.pid"

  if [ -f "$pidfile" ]; then
    pid=$(cat "$pidfile")
    echo "🔻 Stopping node :$port (PID $pid)"

    kill "$pid" 2>/dev/null || true
    sleep 0.5
    kill -9 "$pid" 2>/dev/null || true

    rm -f "$pidfile"
  fi

  remove_node_state "$port"
}

# ---- COMMAND HANDLER ----
case "${1:-start}" in

start)
  echo "Starting $DEFAULT_NODES nodes..."

  clear_state

  # Start seed node
  start_node 4001 ""

  # Start rest deterministically
  for ((i=1; i<DEFAULT_NODES; i++)); do
    port=$((BASE_PORT + i))
    bootstrap=$(build_bootstrap "$port")
    start_node "$port" "$bootstrap"
  done

  echo ""
  echo "📡 Cluster started"
  ;;

add)
  port=${2:-}
  if [ -z "$port" ]; then
    echo "Usage: ./cluster.sh add 4004"
    exit 1
  fi

  bootstrap=$(build_bootstrap "$port")
  start_node "$port" "$bootstrap"
  ;;

remove)
  port=${2:-}
  if [ -z "$port" ]; then
    echo "Usage: ./cluster.sh remove 4002"
    exit 1
  fi

  stop_node "$port"
  echo "🗑️ Removed node :$port"
  ;;

stop)
  echo "Stopping all nodes..."

  while read -r port; do
    [ -n "$port" ] && stop_node "$port"
  done < "$STATE_FILE"

  clear_state
  echo "🛑 All nodes stopped"
  ;;

logs)
  tail -f $LOG_DIR/node*.log
  ;;

status)
  echo "Tracked nodes:"
  get_nodes | sed 's/^/✅ :/'
  ;;

*)
  echo "Usage:"
  echo "  ./cluster.sh start"
  echo "  ./cluster.sh add 4004"
  echo "  ./cluster.sh remove 4002"
  echo "  ./cluster.sh stop"
  echo "  ./cluster.sh logs"
  echo "  ./cluster.sh status"
  ;;
esac
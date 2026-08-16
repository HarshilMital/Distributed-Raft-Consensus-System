#!/usr/bin/env bash
# Start, stop and inspect a five node Raft cluster on this machine.
#
#   ./run_cluster.sh start      # launch nodes 1-5 on ports 50051-50055
#   ./run_cluster.sh stop       # kill them
#   ./run_cluster.sh kill 3     # kill one node, to watch the cluster recover
#   ./run_cluster.sh status     # show which nodes are running
#   ./run_cluster.sh clean      # remove all persisted state
set -euo pipefail

cd "$(dirname "$0")"

NODES=5
PYTHON=${PYTHON:-python3}
PID_DIR=.pids

start_node() {
  local id=$1
  mkdir -p "$PID_DIR" "logs_node_$id"
  "$PYTHON" raft_node.py --id "$id" >"logs_node_$id/stdout.log" 2>&1 &
  echo $! >"$PID_DIR/node_$id.pid"
  echo "started node $id (pid $!)"
}

stop_node() {
  local id=$1
  local pidfile="$PID_DIR/node_$id.pid"
  if [[ -f $pidfile ]]; then
    kill "$(cat "$pidfile")" 2>/dev/null || true
    rm -f "$pidfile"
    echo "stopped node $id"
  else
    echo "node $id is not running"
  fi
}

case "${1:-start}" in
  start)
    for id in $(seq 1 $NODES); do start_node "$id"; done
    echo "Cluster starting; a leader is usually elected within ~10s."
    echo "Follow along with:  tail -f logs_node_*/dump.txt"
    ;;
  stop)
    for id in $(seq 1 $NODES); do stop_node "$id"; done
    ;;
  kill)
    stop_node "${2:?usage: ./run_cluster.sh kill <node-id>}"
    ;;
  restart)
    start_node "${2:?usage: ./run_cluster.sh restart <node-id>}"
    ;;
  status)
    for id in $(seq 1 $NODES); do
      pidfile="$PID_DIR/node_$id.pid"
      if [[ -f $pidfile ]] && kill -0 "$(cat "$pidfile")" 2>/dev/null; then
        echo "node $id: running (pid $(cat "$pidfile"))"
      else
        echo "node $id: down"
      fi
    done
    ;;
  clean)
    rm -rf logs_node_* "$PID_DIR"
    echo "removed all node state"
    ;;
  *)
    echo "usage: $0 {start|stop|kill <id>|restart <id>|status|clean}" >&2
    exit 1
    ;;
esac

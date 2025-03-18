#!/bin/bash
# run_simulation.sh
# This script launches the Byzantine simulation by running four concurrent Go processes.
# One process is the commander and three are lieutenants.
# Logs for each node are stored in the "logs" directory.

# Create logs directory if it doesn't exist
mkdir -p logs

# Array to store process IDs
pids=()

# Cleanup function to terminate all background processes on exit
cleanup() {
  echo "Terminating all background processes..."
  kill "${pids[@]}"
  exit 0
}
trap cleanup SIGINT SIGTERM

# Node 1: Commander (honest)
go run server/main.go \
  -id=1 \
  -port=":50051" \
  -peers=":50052,:50053,:50054" \
  -commander \
  -order="Attack" \
  -t=1 \
  -timeout=5 > logs/node1.log 2>&1 &
pids+=($!)

# Node 2: Lieutenant (honest)
go run server/main.go \
  -id=2 \
  -port=":50052" \
  -peers=":50051,:50053,:50054" \
  -t=1 \
  -timeout=5 > logs/node2.log 2>&1 &
pids+=($!)

# Node 3: Lieutenant (traitor)
go run server/main.go \
  -id=3 \
  -port=":50053" \
  -peers=":50051,:50052,:50054" \
  -t=1 \
  -traitor \
  -timeout=5 > logs/node3.log 2>&1 &
pids+=($!)

# Node 4: Lieutenant (honest)
go run server/main.go \
  -id=4 \
  -port=":50054" \
  -peers=":50051,:50052,:50053" \
  -t=1 \
  -timeout=5 > logs/node4.log 2>&1 &
pids+=($!)

# Wait for all background processes to finish.
wait

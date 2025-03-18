#!/bin/bash
set -e

# --------------------------
# Configuration parameters
# --------------------------
LB_PORT=50051
LB_ADDR="localhost:${LB_PORT}"
ETCD_ENDPOINTS="localhost:2379"

NUM_BACKENDS=10       # Number of backend servers to start
NUM_CLIENTS=100       # Number of concurrent client requests
TEST_DURATION=60      # Test duration in seconds

# Client parameters (can be overridden via command-line arguments)
POLICY=${1:-PICK_FIRST}
OPERATION=${2:-loop}
A_PARAM=${3:-50}
B_PARAM=${4:-0}

echo "Scale test configuration:"
echo "  LB_ADDR         : ${LB_ADDR}"
echo "  ETCD_ENDPOINTS  : ${ETCD_ENDPOINTS}"
echo "  NUM_BACKENDS    : ${NUM_BACKENDS}"
echo "  NUM_CLIENTS     : ${NUM_CLIENTS}"
echo "  TEST_DURATION   : ${TEST_DURATION} seconds"
echo "  Client Policy   : ${POLICY}"
echo "  Client Operation: ${OPERATION}"
echo "  Parameter A     : ${A_PARAM}"
echo "  Parameter B     : ${B_PARAM}"
echo ""

# --------------------------
# Start LB server
# --------------------------
echo "Starting LB server on port ${LB_PORT}..."
go run server/lb_server.go -port=${LB_PORT} -etcd=${ETCD_ENDPOINTS} > lb_server.log 2>&1 &
LB_SERVER_PID=$!
echo "LB server PID: ${LB_SERVER_PID}"
sleep 3

# --------------------------
# Start Backend servers
# --------------------------
echo "Starting ${NUM_BACKENDS} backend servers..."
BACKEND_PIDS=()
for ((i=0; i<NUM_BACKENDS; i++)); do
    PORT=$((50052 + i))
    echo "  Starting backend server on port ${PORT}..."
    go run server/backend_server.go -port=${PORT} -lb=${LB_ADDR} > backend_${PORT}.log 2>&1 &
    BACKEND_PIDS+=($!)
    sleep 1
done

# --------------------------
# Launch Client processes
# --------------------------
echo "Launching ${NUM_CLIENTS} client requests..."
CLIENT_PIDS=()
for ((i=0; i<NUM_CLIENTS; i++)); do
    CLIENT_OUT="client_${i}.out"
    go run client/main.go -lb=${LB_ADDR} -policy=${POLICY} -operation=${OPERATION} -a=${A_PARAM} -b=${B_PARAM} > "${CLIENT_OUT}" 2>&1 &
    CLIENT_PIDS+=($!)
    sleep 0.1
done

echo "Test running for ${TEST_DURATION} seconds..."
sleep ${TEST_DURATION}

echo "Test complete. Terminating client processes..."
for pid in "${CLIENT_PIDS[@]}"; do
    kill ${pid} 2>/dev/null || true
done

# --------------------------
# Combine client outputs into CSV
# --------------------------
RESULTS_CSV="results.csv"
echo "timestamp,client_id,policy,operation,a,b,response_time_ms,backend_address,backend_load" > ${RESULTS_CSV}
for ((i=0; i<NUM_CLIENTS; i++)); do
    CLIENT_OUT="client_${i}.out"
    if [[ -f "${CLIENT_OUT}" ]]; then
        cat "${CLIENT_OUT}" >> ${RESULTS_CSV}
    fi
done

echo "Results saved to ${RESULTS_CSV}"

cleanup() {
    echo "Cleaning up..."
    kill ${LB_SERVER_PID} || true
    for pid in "${BACKEND_PIDS[@]}"; do
        kill ${pid} || true
    done
    pkill -f "go run client/main.go" || true
    exit 0
}
trap cleanup SIGINT SIGTERM
cleanup

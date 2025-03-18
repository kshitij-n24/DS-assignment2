#!/bin/bash
# run_tests.sh - Launches LB server, multiple backend servers, and runs the scale test client.
# Ensure that etcd is running on localhost:2379 before running this script.

# Start the LB server.
echo "Starting Load Balancer server..."
go run server/lb_server.go -port=50051 -etcd_endpoints=localhost:2379 &
LB_PID=$!
sleep 3

# Launch multiple backend servers.
NUM_BACKENDS=3
for (( i=0; i<$NUM_BACKENDS; i++ )); do
    PORT=$((50052 + i))
    echo "Starting backend server on port $PORT..."
    go run server/backend_server.go -port=$PORT -lb=localhost:50051 &
done
sleep 5

# Run the scale test client.
echo "Starting scale test client..."
go run client/main.go -lb=localhost:50051 -policy=ROUND_ROBIN -operation=add -a=10 -b=20 -scale=true -num_requests=100 -concurrency=10

# Cleanup: kill the LB server.
echo "Terminating Load Balancer server..."
kill $LB_PID

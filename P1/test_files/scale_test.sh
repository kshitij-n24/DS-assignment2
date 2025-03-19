#!/bin/bash

# Usage Instructions
usage() {
    echo "Usage: $0 [load_balancing_strategy] [num_backend_servers] [num_clients] [operation] [num_arg_1] [num_arg_2]"
    echo ""
    echo "Example:"
    echo "  $0 ROUND_ROBIN 12 100"
    echo ""
    echo "Arguments:"
    echo "  load_balancing_strategy   - Load balancing strategy (e.g., ROUND_ROBIN, LEAST_CONN)"
    echo "  num_backend_servers       - Number of backend servers (e.g., 10-15)"
    echo "  num_clients               - Number of concurrent clients (e.g., 100)"
    echo "  Operation                 - Type of operation performed (add, multiply, loop)"
    echo "  num_arg_1                 - Numeric or integer argument 1 required for operation"
    echo "  num_arg_2                 - Numeric or integer argument 2 required for operation (loop will ignore this argument)"
    echo ""
    exit 1
}

# If the user uses the -h flag or no arguments, show the usage
if [[ "$1" == "-h" || "$#" -gt 3 ]]; then
    usage
fi

# Default values for optional arguments
LB_STRATEGY=${1:-"ROUND_ROBIN"}  # Default to ROUND_ROBIN if not provided
NUM_BACKENDS=${2:-10}  # Default to 10 backend servers if not provided
NUM_CLIENTS=${3:-100}  # Default to 100 clients if not provided
OP_LOAD=${4:-"add"}  # Default to add operation if not provided
NUM_ARG_1=${5:-25}  # Default to 25 if not provided
NUM_ARG_2=${6:-15}  # Default to 15 clients if not provided

# Validate that NUM_BACKENDS and NUM_CLIENTS are integers
if ! [[ "$NUM_BACKENDS" =~ ^[0-9]+$ ]] || ! [[ "$NUM_CLIENTS" =~ ^[0-9]+$ ]] || ! [[ "$NUM_ARG_1" =~ ^[0-9]+$ ]] || ! [[ "$NUM_ARG_2" =~ ^[0-9]+$ ]]; then
    echo "Error: Number of backend servers and clients must be integers."
    usage
fi

# Print starting information
echo "=========================================="
echo "Starting Load Balancer Performance Test"  
echo "=========================================="
echo "Load Balancing Strategy:   $LB_STRATEGY"
echo "Number of Backend Servers: $NUM_BACKENDS"
echo "Number of Clients:         $NUM_CLIENTS"
echo "Operation:                 $OP_LOAD"
echo "Argument number 1:         $NUM_ARG_1"
echo "Argument number 2:         $NUM_ARG_2"
echo "=========================================="

# Check if Go is installed
if ! command -v go &> /dev/null; then
    echo "Error: Go is not installed. Please install Go and try again."
    exit 1
fi

# Clean up any previous log/CSV files.
rm -f lb.log backend_*.log client.log combined.log metrics.csv server_ports.csv

echo "Server Number,Port" > server_ports.csv

# Start the LB server with the specified strategy.
echo "Starting Load Balancer server on port 50051..."
go run server/lb_server.go -port=50051 -etcd_endpoints=localhost:2379 > lb.log 2>&1 &
LB_PID=$!
sleep 3

# Launch multiple backend servers and record their PIDs.
BACKEND_PIDS=()
for (( i=0; i<$NUM_BACKENDS; i++ )); do
    PORT=$((50052 + i))
    
    # Check if port is in use
    if lsof -i :$PORT &>/dev/null; then
        echo "Error: Port $PORT is already in use. Skipping this backend server."
        continue
    fi

    echo "Starting backend server on port $PORT..."
    go run server/backend_server.go -port=$PORT -lb=localhost:50051 > backend_${PORT}.log 2>&1 &
    BACKEND_PIDS+=($!)

    # Record the server number and port to the CSV file
    echo "$((i + 1)),$PORT" >> server_ports.csv
done
sleep 5

# Run the scale test client with the given number of clients.
echo "Starting $NUM_CLIENTS concurrent clients..."
CLIENT_PIDS=()
for (( i=0; i<$NUM_CLIENTS; i++ )); do
    go run client/main.go -lb=localhost:50051 -policy=$LB_STRATEGY -operation=$OP_LOAD -a=$NUM_ARG_1 -b=$NUM_ARG_2 -scale=true -num_requests=1 -concurrency=1 >> client.log 2>&1 &
    CLIENT_PIDS+=($!)
done

# Wait for all clients to finish
echo "Waiting for all clients to finish..."
for pid in "${CLIENT_PIDS[@]}"; do
    wait "$pid"
done
echo "All client processes completed."  # Ensure all client processes are finished before continuing

# Allow a brief moment for logs to flush.
sleep 5


# Cleanup: Terminate the LB and backend servers.
echo "Stopping Load Balancer server..."
# Use kill -9 (SIGKILL) to forcefully terminate LB if it's not responding to a normal kill
if kill "$LB_PID" 2>/dev/null; then
    echo "Load Balancer server terminated successfully."
else
    echo "Error: Unable to terminate Load Balancer server. Trying force kill (SIGKILL)..."
    kill -SIGTERM "$LB_PID" 2>/dev/null
    if ! kill -0 "$LB_PID" 2>/dev/null; then
        echo "Load Balancer server terminated forcefully."
    else
        echo "Error: Unable to terminate Load Balancer server even with force kill."
    fi
fi

echo "Stopping backend servers..."
for pid in "${BACKEND_PIDS[@]}"; do
    # Try to kill the backend server gracefully
    if kill -SIGTERM "$pid" 2>/dev/null; then
        echo "Backend server process $pid terminated gracefully."
    else
        echo "Backend server process $pid not terminated gracefully. Trying force kill..."
        # Force kill the backend server if not terminated gracefully
        kill -9 "$pid" 2>/dev/null
        if ! kill -0 "$pid" 2>/dev/null; then
            echo "Backend server process $pid terminated forcefully."
        else
            echo "Error: Unable to terminate backend server process $pid even after force kill."
        fi
    fi
done

ps aux | grep '/tmp/go-build' | grep -v 'grep' | awk '{print $2}' | xargs kill -9

echo "All servers stopped successfully..."

# Extract and clean up metrics from logs
echo "Extracting metrics..."

grep '\[METRICS\]:' client.log | awk '
BEGIN {
    print "Total Requests,Successful,Total Time,Average Latency,Throughput,Backend Server"
}
{
    total_requests = successful = total_time = avg_latency = throughput = backend_server = ""

    n = split($0, fields, ",")

    for (i = 1; i <= n; i++) {
        gsub(/^ +| +$/, "", fields[i])

        if (fields[i] ~ /Total Requests:/) { split(fields[i], temp, ":"); total_requests = temp[2] }
        if (fields[i] ~ /Successful:/) { split(fields[i], temp, ":"); successful = temp[2] }
        if (fields[i] ~ /Total Time:/) { split(fields[i], temp, ":"); total_time = temp[2]; gsub(/[a-zA-Z]/, "", total_time) }
        if (fields[i] ~ /Average Latency:/) { split(fields[i], temp, ":"); avg_latency = temp[2]; gsub(/[a-zA-Z]/, "", avg_latency) }
        if (fields[i] ~ /Throughput:/) { split(fields[i], temp, ":"); throughput = temp[2]; gsub(/[a-zA-Z\/]+/, "", throughput) }
        if (fields[i] ~ /Using backend server:/) { split(fields[i], temp, ":"); backend_server = temp[2] }
    }

    gsub(/^ +| +$/, "", total_requests)
    gsub(/^ +| +$/, "", successful)
    gsub(/^ +| +$/, "", total_time)
    gsub(/^ +| +$/, "", avg_latency)
    gsub(/^ +| +$/, "", throughput)
    gsub(/^ +| +$/, "", backend_server)

    print total_requests "," successful "," total_time "," avg_latency "," throughput "," backend_server
}' > metrics.csv

echo "Metrics extracted to metrics.csv and server_ports.csv"
echo "=========================================="
echo "Test complete."
echo "=========================================="

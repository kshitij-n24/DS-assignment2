
# Load Balancing System with gRPC

## Dependencies Installation

Before running the system, you need to install the necessary dependencies:

1. **Go**: Install Go from the official website: https://golang.org/doc/install
2. **gRPC**: Follow the installation guide for gRPC: https://grpc.io/blog/installation/
3. **etcd**: Install etcd for dynamic server discovery:
   - Download etcd from https://etcd.io/docs/v3.5.0/ and follow the instructions.
4. **Protocol Buffers**: Install protobuf for compiling `.proto` files:
   - Download protobuf from https://protobuf.dev/download/protobuf-3-21-0/ and follow the installation guide.

### Verify the installation of Go and gRPC:
Run the following commands to verify that Go and gRPC are installed correctly:
```bash
go version
grpc --version
```

---

## Method to Running the System

To run the system, ensure you have the necessary dependencies installed, including Go and gRPC. Follow the instructions below to start the system:

### 1. Set up etcd:
Install and start an etcd instance, which will be used for dynamic server discovery.

```bash
docker run -d --name etcd -p 2379:2379 quay.io/coreos/etcd:v3.5.0
```

### 2. Start the Load Balancer:
The Load Balancer should be started first. It listens on port `50051`.

```bash
go run server/lb_server.go -port=50051 -etcd_endpoints=localhost:2379
```

### 3. Start Backend Servers:
Backend servers should be started next. You can start multiple backend servers with different ports.

```bash
go run server/backend_server.go -port=50052 -lb=localhost:50051
```

### 4. Start the Client:
To perform a single request or to run a scale test with multiple concurrent clients, run the following:

```bash
go run client/main.go -lb=localhost:50051 -policy=ROUND_ROBIN -operation=add -a=10 -b=20
```

To perform a scale test, use the scale test script:

```bash
./scale_test.sh ROUND_ROBIN 10 100 add 10 20
```

## Tests Performed

- **Single Request**: Validated that the client can query the LB and successfully receive results from the selected backend server.
- **Scale Test**: Simulated high load with multiple clients, measuring response times, throughput, and system utilization under different load balancing strategies.

## Input/Output Format

**Input**:  
The client accepts the following inputs:
- `lb`: Load Balancer address (default: `localhost:50051`)
- `policy`: Load balancing policy (`PICK_FIRST`, `ROUND_ROBIN`, `LEAST_LOAD`)
- `operation`: Arithmetic operation (`add`, `multiply`, `hanoi`)
- `a`: Operand 1 (integer)
- `b`: Operand 2 (integer)

**Output**:  
The output displays the result of the computation, along with latency information:

```bash
Compute result: 30 (latency: 0.021s)
```

## Example Input and Output

**Example Input**:
```bash
go run client/main.go -lb=localhost:50051 -policy=ROUND_ROBIN -operation=add -a=10 -b=20
```

**Example Output**:
```bash
Using backend server: localhost:50052
Compute result: 30 (latency: 0.021s)
```

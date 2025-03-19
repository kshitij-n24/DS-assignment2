
# Byzantine Fault Tolerance Simulation

## Introduction

This project simulates the Byzantine Fault Tolerance (BFT) problem, where a group of generals (represented by nodes) must agree on a decision ("Attack" or "Retreat") despite the presence of traitors (malicious generals). The system uses gRPC to enable communication between nodes and implements a consensus algorithm to ensure that all honest generals reach the same decision.

## Dependencies

To run the simulation, the following dependencies must be installed:

- [Go](https://golang.org/dl/) (version 1.16 or higher)
- [gRPC](https://grpc.io/docs/languages/go/installation/) (via `go get` command)
- [Protocol Buffers](https://protobuf.dev/getting-started/)
  - Install Protobuf compiler (`protoc`) from the official site.
  - Run `protoc --go_out=. --go-grpc_out=. protofiles/byzantine.proto` to generate Go files from the `.proto` definition.

## Running the System

1. Clone the repository:

```bash
git clone <repository_url>
cd <repository_directory>
```

2. Run the simulation by executing the `run_simulation.sh` script:

```bash
./run_simulation.sh
```

This will launch four nodes (one commander and three lieutenants) and simulate the consensus process over multiple rounds. Logs will be stored in the `logs/` directory.

## Tests Performed

- **Basic Functionality**: Ensured that nodes can communicate, propagate orders, and reach consensus.
- **Byzantine Fault Simulation**: Tested scenarios with traitor nodes to verify that the system reaches consensus despite the presence of faulty generals.
- **Majority Voting**: Verified that the majority voting mechanism works correctly for decision-making.

## Input/Output Format

- **Input**: The simulation requires the following input parameters:
  - `id`: Unique identifier for each node (general).
  - `port`: The port on which the node listens.
  - `peers`: Comma-separated list of peer node addresses.
  - `commander`: Whether the node is the commander (set to true for the commander).
  - `traitor`: Whether the node is a traitor (set to true for traitor nodes).
  - `order`: The initial order of the commander (either "Attack" or "Retreat").
  - `t`: The maximum number of traitors in the system.
  - `timeout`: The timeout duration for waiting for messages.

- **Output**: The system outputs logs for each node during every round of the simulation, showing the order received, the decision made, and the final consensus decision.

### Example Input/Output

#### Input:
```
-id=1 -port=":50051" -peers=":50052,:50053,:50054" -commander -order="Attack" -t=1
-id=2 -port=":50052" -peers=":50051,:50053,:50054" -t=1
-id=3 -port=":50053" -peers=":50051,:50052,:50054" -t=1 -traitor
-id=4 -port=":50054" -peers=":50051,:50052,:50053" -t=1
```

#### Output:
```
Node 1: Round 1 - Commander Broadcast
Node 1: Round 2 - Cross-Verification
Node 2: Round 2 - Cross-Verification
Node 3: Round 2 - Cross-Verification
Node 4: Round 2 - Cross-Verification
Node 1: FINAL decision: Attack
```


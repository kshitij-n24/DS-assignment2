
# Payment Gateway System

This project implements a miniature version of Stripe, a payment gateway that interfaces with bank servers to manage transactions. The system supports features like secure authentication, idempotent payments, offline payments, two-phase commit (2PC), and logging for system monitoring.

## Dependency Installation

1. **Install gRPC**:
   Follow the installation guide for your chosen language (Go or Python) from the official [gRPC website](https://grpc.io/docs/).

2. **Install Dependencies for Go**:
   - Install `google.golang.org/grpc` and `github.com/golang/protobuf`:
   ```bash
   go get google.golang.org/grpc
   go get github.com/golang/protobuf
   ```

3. **Install Dependencies for Python** (if using Python):
   - Install `grpcio` and `grpcio-tools`:
   ```bash
   pip install grpcio grpcio-tools
   ```

4. **Install SQLite** (for local database storage):
   - Install SQLite3 library for Go or Python.

## Running the System

1. **Generate Protobuf Files**:
   Ensure the `payment.proto` file is compiled into the appropriate Go or Python code using the `protoc` tool.
   
2. **Run the Bank Servers**:
   Start each bank server on different ports (e.g., BankA on port 50051, BankB on port 50052).
   ```bash
   go run bank_server.go --name BankA --port 50051
   go run bank_server.go --name BankB --port 50052
   ```

3. **Run the Payment Gateway Server**:
   Start the payment gateway server.
   ```bash
   go run payment_gateway.go
   ```

4. **Run the Client**:
   Start the client to interact with the payment gateway.
   ```bash
   go run client.go
   ```

## Tests Performed

- **Client Authentication**: Verified that clients can register and authenticate correctly with valid credentials.
- **Balance Checking**: Ensured that clients can fetch their balance after authentication.
- **Payment Processing**: Tested both successful and failed payment transactions, including idempotent payments.
- **Offline Payments**: Simulated offline payments and confirmed they are queued and retried upon reconnection.
- **2PC Transactions**: Tested the two-phase commit (2PC) protocol to ensure atomic payments between bank servers.

## Input/Output Format

- **Input**: Clients interact with the system via command-line commands like `register`, `auth`, `balance`, `pay`, and `search`.
- **Output**: The system responds with messages such as `Registration successful`, `Balance fetched`, `Payment processed`, etc.

## Example Input and Output

**Example 1: Client Registration**
```bash
> register user1 password123 bank123 1000
[CLIENT] Registration successful for user user1
```

**Example 2: Payment**
```bash
> pay BankB recipient123 200
[CLIENT] Payment Response: Payment processed successfully
```

For detailed instructions and additional features, refer to the full documentation or help menu in the client.

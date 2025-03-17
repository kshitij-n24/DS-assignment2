package main

import (
  "context"
  "crypto/tls"
  "crypto/x509"
  "fmt"
  "io/ioutil"
  "log"
  "net"
  "os"
  "os/signal"
  "sync"
  "syscall"
  "time"

  "google.golang.org/grpc"
  "google.golang.org/grpc/credentials"
  "google.golang.org/grpc/metadata"

  pb "github.com/kshitij-n24/DS-assignment2/P3/protofiles"
)

// PaymentGatewayServer implements the PaymentGateway service.
type PaymentGatewayServer struct {
  pb.UnimplementedPaymentGatewayServer

  // In-memory user credentials and balances.
  userCreds    map[string]string  // username -> password
  userBalances map[string]float64 // username -> balance

  // Processed transactions map (idempotency key -> PaymentResponse).
  processedTransactions map[string]*pb.PaymentResponse

  // Bank connections (bank name -> BankServiceClient).
  bankConns map[string]pb.BankServiceClient

  mu sync.Mutex
}

// NewPaymentGatewayServer initializes the server with dummy data.
func NewPaymentGatewayServer() *PaymentGatewayServer {
  return &PaymentGatewayServer{
    userCreds: map[string]string{
      "alice": "password1",
      "bob":   "password2",
    },
    userBalances: map[string]float64{
      "alice": 1000.0,
      "bob":   500.0,
    },
    processedTransactions: make(map[string]*pb.PaymentResponse),
    bankConns:             make(map[string]pb.BankServiceClient),
  }
}

// Authenticate validates credentials and returns a token.
func (s *PaymentGatewayServer) Authenticate(ctx context.Context, req *pb.AuthRequest) (*pb.AuthResponse, error) {
  log.Printf("[AUTH] Received authentication request for user: %s", req.Username)
  s.mu.Lock()
  password, exists := s.userCreds[req.Username]
  s.mu.Unlock()

  if !exists || password != req.Password {
    return &pb.AuthResponse{
      Success: false,
      Message: "Invalid credentials",
    }, nil
  }
  // Create a simple token. (For production, use JWT or similar.)
  token := req.Username + "-token"
  log.Printf("[AUTH] User %s authenticated successfully. Token: %s", req.Username, token)
  return &pb.AuthResponse{
    Success: true,
    Token:   token,
    Message: "Authentication successful",
  }, nil
}

// GetBalance returns the balance for the authenticated user.
func (s *PaymentGatewayServer) GetBalance(ctx context.Context, req *pb.BalanceRequest) (*pb.BalanceResponse, error) {
  username, err := extractUsername(req.Token)
  if err != nil {
    return &pb.BalanceResponse{Balance: 0, Message: err.Error()}, nil
  }

  s.mu.Lock()
  balance, exists := s.userBalances[username]
  s.mu.Unlock()
  if !exists {
    return &pb.BalanceResponse{Balance: 0, Message: "User not found"}, nil
  }
  log.Printf("[BALANCE] User %s has balance: %.2f", username, balance)
  return &pb.BalanceResponse{Balance: balance, Message: "Balance fetched successfully"}, nil
}

// ProcessPayment performs a 2PC payment process and handles idempotency.
func (s *PaymentGatewayServer) ProcessPayment(ctx context.Context, req *pb.PaymentRequest) (*pb.PaymentResponse, error) {
  // Validate token.
  username, err := extractUsername(req.Token)
  if err != nil {
    return &pb.PaymentResponse{Success: false, Message: err.Error()}, nil
  }

  // Validate idempotency key.
  if req.IdempotencyKey == "" {
    return &pb.PaymentResponse{Success: false, Message: "Missing idempotency key"}, nil
  }

  // Check idempotency.
  s.mu.Lock()
  if res, exists := s.processedTransactions[req.IdempotencyKey]; exists {
    s.mu.Unlock()
    log.Printf("[PAYMENT] Duplicate payment request detected for key: %s", req.IdempotencyKey)
    return res, nil
  }
  s.mu.Unlock()

  // Validate sufficient funds.
  s.mu.Lock()
  senderBalance, exists := s.userBalances[username]
  s.mu.Unlock()
  if !exists {
    return &pb.PaymentResponse{Success: false, Message: "User not found"}, nil
  }
  if senderBalance < req.Amount {
    return &pb.PaymentResponse{Success: false, Message: "Insufficient funds"}, nil
  }

  // Ensure bank connections are available.
  senderBank, ok := s.bankConns["BankA"]
  if !ok {
    return &pb.PaymentResponse{Success: false, Message: "Sender bank connection not available"}, nil
  }
  receiverBank, ok := s.bankConns[req.ToBank]
  if !ok {
    return &pb.PaymentResponse{Success: false, Message: "Receiver bank connection not available"}, nil
  }

  // 2PC: Prepare Phase.
  transactionID := fmt.Sprintf("%s-%d", req.IdempotencyKey, time.Now().UnixNano())
  prepareReq := &pb.BankTransactionRequest{
    TransactionId: transactionID,
    Amount:        req.Amount,
  }

  // Use a child context with timeout.
  ctx2, cancel := context.WithTimeout(context.Background(), 5*time.Second)
  defer cancel()

  log.Printf("[2PC] Initiating prepare phase for transaction %s", transactionID)
  senderPrep, err := senderBank.PrepareTransaction(ctx2, prepareReq)
  if err != nil || !senderPrep.Success {
    log.Printf("[2PC] Sender bank prepare failed for transaction %s: %v", transactionID, err)
    senderBank.AbortTransaction(context.Background(), prepareReq)
    return &pb.PaymentResponse{Success: false, Message: "Sender bank preparation failed"}, nil
  }

  receiverPrep, err := receiverBank.PrepareTransaction(ctx2, prepareReq)
  if err != nil || !receiverPrep.Success {
    log.Printf("[2PC] Receiver bank prepare failed for transaction %s: %v", transactionID, err)
    senderBank.AbortTransaction(context.Background(), prepareReq)
    receiverBank.AbortTransaction(context.Background(), prepareReq)
    return &pb.PaymentResponse{Success: false, Message: "Receiver bank preparation failed"}, nil
  }

  // 2PC: Commit Phase.
  log.Printf("[2PC] Commit phase for transaction %s", transactionID)
  senderCommit, err := senderBank.CommitTransaction(ctx2, prepareReq)
  if err != nil || !senderCommit.Success {
    log.Printf("[2PC] Sender bank commit failed for transaction %s: %v", transactionID, err)
    senderBank.AbortTransaction(context.Background(), prepareReq)
    receiverBank.AbortTransaction(context.Background(), prepareReq)
    return &pb.PaymentResponse{Success: false, Message: "Sender bank commit failed"}, nil
  }

  receiverCommit, err := receiverBank.CommitTransaction(ctx2, prepareReq)
  if err != nil || !receiverCommit.Success {
    log.Printf("[2PC] Receiver bank commit failed for transaction %s: %v", transactionID, err)
    // In a real system, compensation logic would be needed.
    return &pb.PaymentResponse{Success: false, Message: "Receiver bank commit failed"}, nil
  }

  // Deduct funds from sender.
  s.mu.Lock()
  s.userBalances[username] -= req.Amount
  s.mu.Unlock()
  log.Printf("[PAYMENT] Payment of %.2f processed for user %s. New balance: %.2f", req.Amount, username, s.userBalances[username])

  response := &pb.PaymentResponse{Success: true, Message: "Payment processed successfully"}

  // Save response for idempotency.
  s.mu.Lock()
  s.processedTransactions[req.IdempotencyKey] = response
  s.mu.Unlock()

  return response, nil
}

// extractUsername validates token format and extracts the username.
func extractUsername(token string) (string, error) {
  if len(token) <= 6 || token[len(token)-6:] != "-token" {
    return "", fmt.Errorf("Invalid token format")
  }
  return token[:len(token)-6], nil
}

// ----- Interceptors -----

// loggingInterceptor logs every incoming request and its response.
func loggingInterceptor(
  ctx context.Context,
  req interface{},
  info *grpc.UnaryServerInfo,
  handler grpc.UnaryHandler,
) (interface{}, error) {
  start := time.Now()
  log.Printf("[INTERCEPTOR] Request: Method=%s; Time=%s; Request=%+v", info.FullMethod, start.Format(time.RFC3339), req)
  resp, err := handler(ctx, req)
  log.Printf("[INTERCEPTOR] Response: Method=%s; Duration=%s; Response=%+v; Error=%v", info.FullMethod, time.Since(start), resp, err)
  return resp, err
}

// authInterceptor enforces the presence of an authorization token in metadata (except for Authenticate).
func authInterceptor(
  ctx context.Context,
  req interface{},
  info *grpc.UnaryServerInfo,
  handler grpc.UnaryHandler,
) (interface{}, error) {
  if info.FullMethod == "/payment.PaymentGateway/Authenticate" {
    return handler(ctx, req)
  }
  md, ok := metadata.FromIncomingContext(ctx)
  if !ok {
    return nil, fmt.Errorf("Missing metadata in context")
  }
  tokens := md.Get("authorization")
  if len(tokens) == 0 || tokens[0] == "" {
    return nil, fmt.Errorf("Missing authorization token")
  }
  // In production, token validation (e.g. JWT verification) would occur here.
  return handler(ctx, req)
}

func main() {
  // Load server TLS credentials for accepting incoming connections.
  certFile := "../certificates/server.crt"
  keyFile := "../certificates/server.key"
  serverCreds, err := credentials.NewServerTLSFromFile(certFile, keyFile)
  if err != nil {
    log.Fatalf("[STARTUP] Failed to load TLS credentials from %s and %s: %v", certFile, keyFile, err)
  }

  // Create gRPC server with TLS and interceptors.
  opts := []grpc.ServerOption{
    grpc.Creds(serverCreds),
    grpc.ChainUnaryInterceptor(authInterceptor, loggingInterceptor),
  }
  grpcServer := grpc.NewServer(opts...)

  // Initialize the Payment Gateway service.
  pgServer := NewPaymentGatewayServer()

  // --- Create client TLS credentials for dialing Bank servers ---
  // Load CA certificate to trust Bank server certificates.
  caCertPath := "../certificates/ca.crt"
  caCert, err := ioutil.ReadFile(caCertPath)
  if err != nil {
    log.Fatalf("[STARTUP] Failed to read CA certificate: %v", err)
  }
  caCertPool := x509.NewCertPool()
  if !caCertPool.AppendCertsFromPEM(caCert) {
    log.Fatalf("[STARTUP] Failed to append CA certificate")
  }
  // Use "localhost" as the expected server name (must match SAN in server certificate).
  clientCreds := credentials.NewTLS(&tls.Config{
    RootCAs:    caCertPool,
    ServerName: "localhost",
  })

  // Connect to bank servers using client TLS credentials.
  bankNames := []string{"BankA", "BankB"}
  for _, bank := range bankNames {
    conn, err := grpc.Dial("localhost:50051", grpc.WithTransportCredentials(clientCreds))
    if err != nil {
      log.Printf("[BANK-CONNECT] Failed to connect to bank %s: %v", bank, err)
      continue
    }
    pgServer.bankConns[bank] = pb.NewBankServiceClient(conn)
    log.Printf("[BANK-CONNECT] Connected to bank %s", bank)
  }

  // Register the PaymentGateway service.
  pb.RegisterPaymentGatewayServer(grpcServer, pgServer)

  // Start listening.
  listener, err := net.Listen("tcp", ":50060")
  if err != nil {
    log.Fatalf("[STARTUP] Failed to listen on :50060: %v", err)
  }
  log.Println("[STARTUP] Payment Gateway server is listening on :50060")

  // Handle graceful shutdown.
  go func() {
    ch := make(chan os.Signal, 1)
    signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
    <-ch
    log.Println("[SHUTDOWN] Shutting down Payment Gateway server...")
    grpcServer.GracefulStop()
    os.Exit(0)
  }()

  if err := grpcServer.Serve(listener); err != nil {
    log.Fatalf("[RUNTIME] Failed to serve: %v", err)
  }
}

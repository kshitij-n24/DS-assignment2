package main

import (
  "context"
  "crypto/tls"
  "crypto/x509"
  "encoding/json"
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

// PGData represents the persistent data for the Payment Gateway.
type PGData struct {
  UserBalances         map[string]float64             `json:"user_balances"`
  ProcessedTransactions map[string]*pb.PaymentResponse `json:"processed_transactions"`
}

// PaymentGatewayServer implements the PaymentGateway service.
type PaymentGatewayServer struct {
  pb.UnimplementedPaymentGatewayServer

  userCreds             map[string]string            // username -> password
  userBalances          map[string]float64           // username -> balance
  processedTransactions map[string]*pb.PaymentResponse // idempotency key -> PaymentResponse
  bankConns             map[string]pb.BankServiceClient

  mu       sync.Mutex
  dataFile string
}

// NewPaymentGatewayServer creates a new Payment Gateway server and loads persistent data.
func NewPaymentGatewayServer(dataFile string) *PaymentGatewayServer {
  s := &PaymentGatewayServer{
    userCreds: map[string]string{
      "alice": "password1",
      "bob":   "password2",
    },
    // default balances; these may be overwritten if data is loaded
    userBalances: map[string]float64{
      "alice": 1000.0,
      "bob":   500.0,
    },
    processedTransactions: make(map[string]*pb.PaymentResponse),
    bankConns:             make(map[string]pb.BankServiceClient),
    dataFile:              dataFile,
  }
  s.loadData()
  return s
}

// loadData loads persistent data from the JSON file.
func (s *PaymentGatewayServer) loadData() {
  s.mu.Lock()
  defer s.mu.Unlock()
  data, err := ioutil.ReadFile(s.dataFile)
  if err != nil {
    log.Printf("[PERSISTENCE] No existing data file; starting fresh: %v", err)
    return
  }
  var pgData PGData
  if err := json.Unmarshal(data, &pgData); err != nil {
    log.Printf("[PERSISTENCE] Error unmarshaling data: %v", err)
    return
  }
  s.userBalances = pgData.UserBalances
  s.processedTransactions = pgData.ProcessedTransactions
  log.Printf("[PERSISTENCE] Data loaded successfully")
}

// saveData writes persistent data to the JSON file.
func (s *PaymentGatewayServer) saveData() {
  s.mu.Lock()
  defer s.mu.Unlock()
  pgData := PGData{
    UserBalances:         s.userBalances,
    ProcessedTransactions: s.processedTransactions,
  }
  data, err := json.MarshalIndent(pgData, "", "  ")
  if err != nil {
    log.Printf("[PERSISTENCE] Error marshaling data: %v", err)
    return
  }
  err = ioutil.WriteFile(s.dataFile, data, 0644)
  if err != nil {
    log.Printf("[PERSISTENCE] Error writing data file: %v", err)
  } else {
    log.Printf("[PERSISTENCE] Data saved successfully")
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
  username, err := extractUsername(req.Token)
  if err != nil {
    return &pb.PaymentResponse{Success: false, Message: err.Error()}, nil
  }
  if req.IdempotencyKey == "" {
    return &pb.PaymentResponse{Success: false, Message: "Missing idempotency key"}, nil
  }
  s.mu.Lock()
  if res, exists := s.processedTransactions[req.IdempotencyKey]; exists {
    s.mu.Unlock()
    log.Printf("[PAYMENT] Duplicate payment request detected for key: %s", req.IdempotencyKey)
    return res, nil
  }
  s.mu.Unlock()

  s.mu.Lock()
  senderBalance, exists := s.userBalances[username]
  s.mu.Unlock()
  if !exists {
    return &pb.PaymentResponse{Success: false, Message: "User not found"}, nil
  }
  if senderBalance < req.Amount {
    return &pb.PaymentResponse{Success: false, Message: "Insufficient funds"}, nil
  }

  // Use BankA as sender and the bank specified in req.ToBank as receiver.
  senderBank, ok := s.bankConns["BankA"]
  if !ok {
    return &pb.PaymentResponse{Success: false, Message: "Sender bank connection not available"}, nil
  }
  receiverBank, ok := s.bankConns[req.ToBank]
  if !ok {
    return &pb.PaymentResponse{Success: false, Message: "Receiver bank connection not available"}, nil
  }

  transactionID := fmt.Sprintf("%s-%d", req.IdempotencyKey, time.Now().UnixNano())
  prepareReq := &pb.BankTransactionRequest{
    TransactionId: transactionID,
    Amount:        req.Amount,
  }

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
    return &pb.PaymentResponse{Success: false, Message: "Receiver bank commit failed"}, nil
  }

  s.mu.Lock()
  s.userBalances[username] -= req.Amount
  s.mu.Unlock()
  log.Printf("[PAYMENT] Payment of %.2f processed for user %s. New balance: %.2f", req.Amount, username, s.userBalances[username])

  response := &pb.PaymentResponse{Success: true, Message: "Payment processed successfully"}
  s.mu.Lock()
  s.processedTransactions[req.IdempotencyKey] = response
  s.mu.Unlock()
  s.saveData() // persist the updated data

  return response, nil
}

func extractUsername(token string) (string, error) {
  if len(token) <= 6 || token[len(token)-6:] != "-token" {
    return "", fmt.Errorf("Invalid token format")
  }
  return token[:len(token)-6], nil
}

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
  return handler(ctx, req)
}

func main() {
  // Set up file logging.
  logDir := "../logs"
  os.MkdirAll(logDir, 0755)
  logFile, err := os.OpenFile(logDir+"/pg_server.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
  if err != nil {
    log.Fatalf("Failed to open log file: %v", err)
  }
  defer logFile.Close()
  log.SetOutput(logFile)

  // Load TLS credentials for serving.
  certFile := "../certificates/server.crt"
  keyFile := "../certificates/server.key"
  serverCreds, err := credentials.NewServerTLSFromFile(certFile, keyFile)
  if err != nil {
    log.Fatalf("[STARTUP] Failed to load TLS credentials from %s and %s: %v", certFile, keyFile, err)
  }

  opts := []grpc.ServerOption{
    grpc.Creds(serverCreds),
    grpc.ChainUnaryInterceptor(authInterceptor, loggingInterceptor),
  }
  grpcServer := grpc.NewServer(opts...)

  // Create Payment Gateway server with persistence.
  pgDataFile := "../data/pg_data.json"
  os.MkdirAll("../data", 0755)
  pgServer := NewPaymentGatewayServer(pgDataFile)

  // Set up client TLS credentials for dialing bank servers.
  caCertPath := "../certificates/ca.crt"
  caCert, err := ioutil.ReadFile(caCertPath)
  if err != nil {
    log.Fatalf("[STARTUP] Failed to read CA certificate: %v", err)
  }
  caCertPool := x509.NewCertPool()
  if !caCertPool.AppendCertsFromPEM(caCert) {
    log.Fatalf("[STARTUP] Failed to append CA certificate")
  }
  clientCreds := credentials.NewTLS(&tls.Config{
    RootCAs:    caCertPool,
    ServerName: "localhost",
  })

  // Connect to two bank servers on different ports.
  bankConnections := map[string]string{
    "BankA": "localhost:50051",
    "BankB": "localhost:50052",
  }
  for bank, addr := range bankConnections {
    conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(clientCreds))
    if err != nil {
      log.Printf("[BANK-CONNECT] Failed to connect to bank %s at %s: %v", bank, addr, err)
      continue
    }
    pgServer.bankConns[bank] = pb.NewBankServiceClient(conn)
    log.Printf("[BANK-CONNECT] Connected to bank %s at %s", bank, addr)
  }

  pb.RegisterPaymentGatewayServer(grpcServer, pgServer)

  listener, err := net.Listen("tcp", ":50060")
  if err != nil {
    log.Fatalf("[STARTUP] Failed to listen on :50060: %v", err)
  }
  log.Println("[STARTUP] Payment Gateway server is listening on :50060")

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

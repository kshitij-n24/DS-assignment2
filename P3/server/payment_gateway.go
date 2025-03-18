package main

import (
  "context"
  "crypto/tls"
  "crypto/x509"
  "encoding/json"
  "flag"
  "fmt"
  "io"
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

  pb "github.com/<user>/DS-assignment2/P3/protofiles"
)

// PGData represents the persistent data for the Payment Gateway.
type PGData struct {
  UserBalances          map[string]float64            `json:"user_balances"`
  ProcessedTransactions map[string]*pb.PaymentResponse `json:"processed_transactions"`
  UserCreds             map[string]string             `json:"user_creds"`
}

// PaymentGatewayServer implements the PaymentGateway service.
type PaymentGatewayServer struct {
  pb.UnimplementedPaymentGatewayServer

  userCreds             map[string]string              // username -> password
  userBalances          map[string]float64             // username -> balance
  processedTransactions map[string]*pb.PaymentResponse // idempotency key -> PaymentResponse
  bankConns             map[string]pb.BankServiceClient

  mu          sync.Mutex
  dataFile    string
  bankTimeout time.Duration // configurable timeout for bank transactions
}

func NewPaymentGatewayServer(dataFile string, bankTimeout time.Duration) *PaymentGatewayServer {
  s := &PaymentGatewayServer{
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
    dataFile:              dataFile,
    bankTimeout:           bankTimeout,
  }
  s.loadData()
  return s
}

// loadData loads persistent data from the JSON file.
// If any of the maps are nil, they are reinitialized.
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
  if pgData.UserBalances == nil {
    pgData.UserBalances = make(map[string]float64)
  }
  if pgData.UserCreds == nil {
    pgData.UserCreds = make(map[string]string)
  }
  if pgData.ProcessedTransactions == nil {
    pgData.ProcessedTransactions = make(map[string]*pb.PaymentResponse)
  }
  s.userBalances = pgData.UserBalances
  s.userCreds = pgData.UserCreds
  s.processedTransactions = pgData.ProcessedTransactions
  log.Printf("[PERSISTENCE] Data loaded successfully")
}

// saveData writes persistent data to the JSON file.
func (s *PaymentGatewayServer) saveData() {
  s.mu.Lock()
  defer s.mu.Unlock()
  pgData := PGData{
    UserBalances:          s.userBalances,
    ProcessedTransactions: s.processedTransactions,
    UserCreds:             s.userCreds,
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

// RegisterClient registers a new client with bank account details.
func (s *PaymentGatewayServer) RegisterClient(ctx context.Context, req *pb.RegisterClientRequest) (*pb.RegisterClientResponse, error) {
  s.mu.Lock()
  defer s.mu.Unlock()
  if _, exists := s.userCreds[req.Username]; exists {
    return &pb.RegisterClientResponse{
      Success: false,
      Message: "User already registered",
    }, nil
  }
  s.userCreds[req.Username] = req.Password
  s.userBalances[req.Username] = req.InitialBalance
  log.Printf("[REGISTER] User %s registered with initial balance %.2f and bank account %s", req.Username, req.InitialBalance, req.BankAccount)
  s.saveData()
  return &pb.RegisterClientResponse{
    Success: true,
    Message: "Registration successful",
  }, nil
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

  // Prepare phase.
  prepareCtx, cancel := context.WithTimeout(context.Background(), s.bankTimeout)
  defer cancel()
  log.Printf("[2PC] Initiating prepare phase for transaction %s", transactionID)
  senderPrep, err := senderBank.PrepareTransaction(prepareCtx, prepareReq)
  if err != nil || !senderPrep.Success {
    log.Printf("[2PC] Sender bank prepare failed for transaction %s: %v", transactionID, err)
    senderBank.AbortTransaction(context.Background(), prepareReq)
    return &pb.PaymentResponse{Success: false, Message: "Sender bank preparation failed"}, nil
  }

  receiverPrep, err := receiverBank.PrepareTransaction(prepareCtx, prepareReq)
  if err != nil || !receiverPrep.Success {
    log.Printf("[2PC] Receiver bank prepare failed for transaction %s: %v", transactionID, err)
    senderBank.AbortTransaction(context.Background(), prepareReq)
    receiverBank.AbortTransaction(context.Background(), prepareReq)
    return &pb.PaymentResponse{Success: false, Message: "Receiver bank preparation failed"}, nil
  }

  // Commit phase: perform commits concurrently.
  commitCtx, cancelCommit := context.WithTimeout(context.Background(), s.bankTimeout)
  defer cancelCommit()

  type commitResult struct {
    resp *pb.BankTransactionResponse
    err  error
  }
  resultCh := make(chan commitResult, 2)

  go func() {
    resp, err := senderBank.CommitTransaction(commitCtx, prepareReq)
    resultCh <- commitResult{resp: resp, err: err}
  }()

  go func() {
    resp, err := receiverBank.CommitTransaction(commitCtx, prepareReq)
    resultCh <- commitResult{resp: resp, err: err}
  }()

  var senderCommit, receiverCommit *pb.BankTransactionResponse
  for i := 0; i < 2; i++ {
    res := <-resultCh
    if res.err != nil || !res.resp.Success {
      log.Printf("[2PC] Commit phase failed for transaction %s: %v", transactionID, res.err)
      senderBank.AbortTransaction(context.Background(), prepareReq)
      receiverBank.AbortTransaction(context.Background(), prepareReq)
      return &pb.PaymentResponse{Success: false, Message: "Bank commit failed"}, nil
    }
    if senderCommit == nil {
      senderCommit = res.resp
    } else {
      receiverCommit = res.resp
    }
  }

  log.Printf("[2PC] Both banks committed transaction %s: Sender: %s; Receiver: %s",
    transactionID, senderCommit.Message, receiverCommit.Message)

  s.mu.Lock()
  s.userBalances[username] -= req.Amount
  s.mu.Unlock()
  log.Printf("[PAYMENT] Payment of %.2f processed for user %s. New balance: %.2f", req.Amount, username, s.userBalances[username])

  response := &pb.PaymentResponse{Success: true, Message: "Payment processed successfully"}
  s.mu.Lock()
  s.processedTransactions[req.IdempotencyKey] = response
  s.mu.Unlock()
  s.saveData()

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
  // Allow registration and authentication without token.
  if info.FullMethod == "/payment.PaymentGateway/Authenticate" || info.FullMethod == "/payment.PaymentGateway/RegisterClient" {
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
  bankTimeoutFlag := flag.Int("bank_timeout", 5, "Timeout (in seconds) for bank transactions")
  flag.Parse()

  // Set up file logging.
  logDir := "../logs"
  if _, err := os.Stat(logDir); os.IsNotExist(err) {
    if err := os.MkdirAll(logDir, 0755); err != nil {
      log.Fatalf("Failed to create log directory: %v", err)
    }
  }
  logFilePath := logDir + "/pg_server.log"
  logFile, err := os.OpenFile(logFilePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
  if err != nil {
    log.Fatalf("Failed to open log file: %v", err)
  }
  mw := io.MultiWriter(os.Stdout, logFile)
  log.SetOutput(mw)

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

  // Create data directory if it does not exist.
  dataDir := "../data"
  if _, err := os.Stat(dataDir); os.IsNotExist(err) {
    if err := os.MkdirAll(dataDir, 0755); err != nil {
      log.Fatalf("Failed to create data directory: %v", err)
    }
  }
  pgDataFile := dataDir + "/pg_data.json"
  bankTimeout := time.Duration(*bankTimeoutFlag) * time.Second
  pgServer := NewPaymentGatewayServer(pgDataFile, bankTimeout)

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

  // Connect to bank servers and store the connections for later shutdown.
  bankConnections := map[string]string{
    "BankA": "localhost:50051",
    "BankB": "localhost:50052",
  }
  var bankConns []*grpc.ClientConn
  for bank, addr := range bankConnections {
    conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(clientCreds))
    if err != nil {
      log.Printf("[BANK-CONNECT] Failed to connect to bank %s at %s: %v", bank, addr, err)
      continue
    }
    pgServer.bankConns[bank] = pb.NewBankServiceClient(conn)
    bankConns = append(bankConns, conn)
    log.Printf("[BANK-CONNECT] Connected to bank %s at %s", bank, addr)
  }

  pb.RegisterPaymentGatewayServer(grpcServer, pgServer)

  listener, err := net.Listen("tcp", ":50060")
  if err != nil {
    log.Fatalf("[STARTUP] Failed to listen on :50060: %v", err)
  }
  log.Println("[STARTUP] Payment Gateway server is listening on :50060")

  // Handle shutdown gracefully.
  go func() {
    ch := make(chan os.Signal, 1)
    signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
    <-ch
    log.Println("[SHUTDOWN] Shutting down Payment Gateway server...")
    // Close all bank connections.
    for _, conn := range bankConns {
      conn.Close()
    }
    grpcServer.GracefulStop()
    os.Exit(0)
  }()

  if err := grpcServer.Serve(listener); err != nil {
    log.Fatalf("[RUNTIME] Failed to serve: %v", err)
  }
}

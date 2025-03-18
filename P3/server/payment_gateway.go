package main

import (
  "context"
  "crypto/tls"
  "crypto/x509"
  "database/sql"
  "flag"
  "fmt"
  "io"
  "log"
  "net"
  "os"
  "os/signal"
  "strings"
  "syscall"
  "time"

  _ "github.com/mattn/go-sqlite3" // SQLite driver

  "google.golang.org/grpc"
  "google.golang.org/grpc/credentials"
  "google.golang.org/grpc/metadata"

  pb "github.com/kshitij-n24/DS-assignment2/P3/protofiles"
)

type PaymentGatewayServer struct {
  pb.UnimplementedPaymentGatewayServer
  db          *sql.DB
  bankConns   map[string]pb.BankServiceClient
  bankTimeout time.Duration
}

func NewPaymentGatewayServer(db *sql.DB, bankTimeout time.Duration) *PaymentGatewayServer {
  return &PaymentGatewayServer{
    db:          db,
    bankConns:   make(map[string]pb.BankServiceClient),
    bankTimeout: bankTimeout,
  }
}

func initDB(db *sql.DB) error {
  accountsTable := `
  CREATE TABLE IF NOT EXISTS accounts (
    username TEXT PRIMARY KEY,
    password TEXT,
    bank_account TEXT,
    balance REAL
  );`
  _, err := db.Exec(accountsTable)
  if err != nil {
    return err
  }
  transactionsTable := `
  CREATE TABLE IF NOT EXISTS processed_transactions (
    idempotency_key TEXT PRIMARY KEY,
    success INTEGER,
    message TEXT
  );`
  _, err = db.Exec(transactionsTable)
  return err
}

func (s *PaymentGatewayServer) RegisterClient(ctx context.Context, req *pb.RegisterClientRequest) (*pb.RegisterClientResponse, error) {
  req.Username = strings.TrimSpace(req.Username)
  req.Password = strings.TrimSpace(req.Password)
  req.BankAccount = strings.TrimSpace(req.BankAccount)
  if req.Username == "" || req.Password == "" || req.BankAccount == "" {
    return &pb.RegisterClientResponse{Success: false, Message: "Username, password, and bank account must be non-empty"}, nil
  }
  _, err := s.db.ExecContext(ctx, "INSERT INTO accounts(username, password, bank_account, balance) VALUES (?, ?, ?, ?)",
    req.Username, req.Password, req.BankAccount, req.InitialBalance)
  if err != nil {
    if strings.Contains(err.Error(), "UNIQUE") {
      return &pb.RegisterClientResponse{Success: false, Message: "User already registered"}, nil
    }
    return &pb.RegisterClientResponse{Success: false, Message: "Registration error"}, err
  }
  log.Printf("[REGISTER] User %s registered with initial balance %.2f and bank account %s", req.Username, req.InitialBalance, req.BankAccount)
  return &pb.RegisterClientResponse{Success: true, Message: "Registration successful"}, nil
}

func (s *PaymentGatewayServer) Authenticate(ctx context.Context, req *pb.AuthRequest) (*pb.AuthResponse, error) {
  req.Username = strings.TrimSpace(req.Username)
  req.Password = strings.TrimSpace(req.Password)
  if req.Username == "" || req.Password == "" {
    return &pb.AuthResponse{Success: false, Message: "Username and password must be non-empty"}, nil
  }
  var storedPassword string
  err := s.db.QueryRowContext(ctx, "SELECT password FROM accounts WHERE username = ?", req.Username).Scan(&storedPassword)
  if err != nil {
    return &pb.AuthResponse{Success: false, Message: "Invalid credentials"}, nil
  }
  if storedPassword != req.Password {
    return &pb.AuthResponse{Success: false, Message: "Invalid credentials"}, nil
  }
  token := req.Username + "-token"
  log.Printf("[AUTH] User %s authenticated successfully. Token: %s", req.Username, token)
  return &pb.AuthResponse{Success: true, Token: token, Message: "Authentication successful"}, nil
}

func (s *PaymentGatewayServer) GetBalance(ctx context.Context, req *pb.BalanceRequest) (*pb.BalanceResponse, error) {
  username, err := extractUsername(req.Token)
  if err != nil {
    return &pb.BalanceResponse{Balance: 0, Message: err.Error()}, nil
  }
  var balance float64
  err = s.db.QueryRowContext(ctx, "SELECT balance FROM accounts WHERE username = ?", username).Scan(&balance)
  if err != nil {
    return &pb.BalanceResponse{Balance: 0, Message: "User not found"}, nil
  }
  log.Printf("[BALANCE] User %s has balance: %.2f", username, balance)
  return &pb.BalanceResponse{Balance: balance, Message: "Balance fetched successfully"}, nil
}

func (s *PaymentGatewayServer) SearchAccounts(ctx context.Context, req *pb.SearchAccountsRequest) (*pb.SearchAccountsResponse, error) {
  query := "%" + strings.TrimSpace(req.Query) + "%"
  rows, err := s.db.QueryContext(ctx, "SELECT username, bank_account, balance FROM accounts WHERE username LIKE ? OR bank_account LIKE ?", query, query)
  if err != nil {
    return &pb.SearchAccountsResponse{}, err
  }
  defer rows.Close()
  var accounts []*pb.AccountInfo
  for rows.Next() {
    var username, bankAccount string
    var balance float64
    if err := rows.Scan(&username, &bankAccount, &balance); err != nil {
      continue
    }
    accounts = append(accounts, &pb.AccountInfo{
      Username:    username,
      BankAccount: bankAccount,
      Balance:     balance,
    })
  }
  return &pb.SearchAccountsResponse{Accounts: accounts}, nil
}

func (s *PaymentGatewayServer) getStoredResponse(key string) (*pb.PaymentResponse, error) {
  var successInt int
  var message string
  err := s.db.QueryRow("SELECT success, message FROM processed_transactions WHERE idempotency_key = ?", key).
    Scan(&successInt, &message)
  if err == sql.ErrNoRows {
    return nil, nil
  }
  if err != nil {
    return nil, err
  }
  return &pb.PaymentResponse{Success: successInt == 1, Message: message}, nil
}

func (s *PaymentGatewayServer) storeResponse(key string, resp *pb.PaymentResponse) error {
  _, err := s.db.Exec("INSERT INTO processed_transactions (idempotency_key, success, message) VALUES (?, ?, ?)",
    key, boolToInt(resp.Success), resp.Message)
  return err
}

func boolToInt(b bool) int {
  if b {
    return 1
  }
  return 0
}

func (s *PaymentGatewayServer) ProcessPayment(ctx context.Context, req *pb.PaymentRequest) (*pb.PaymentResponse, error) {
  username, err := extractUsername(req.Token)
  if err != nil {
    return &pb.PaymentResponse{Success: false, Message: err.Error()}, nil
  }
  req.IdempotencyKey = strings.TrimSpace(req.IdempotencyKey)
  req.RecipientAccount = strings.TrimSpace(req.RecipientAccount)
  if req.IdempotencyKey == "" {
    return &pb.PaymentResponse{Success: false, Message: "Missing idempotency key"}, nil
  }
  if req.RecipientAccount == "" {
    return &pb.PaymentResponse{Success: false, Message: "Missing recipient account"}, nil
  }
  stored, err := s.getStoredResponse(req.IdempotencyKey)
  if err != nil {
    log.Printf("[DB] Error checking idempotency key: %v", err)
  } else if stored != nil {
    log.Printf("[PAYMENT] Duplicate payment request detected for key: %s", req.IdempotencyKey)
    return stored, nil
  }
  // Begin SQL transaction for atomic balance update.
  tx, err := s.db.BeginTx(ctx, nil)
  if err != nil {
    return &pb.PaymentResponse{Success: false, Message: "Internal error"}, err
  }
  var senderBalance float64
  err = tx.QueryRowContext(ctx, "SELECT balance FROM accounts WHERE username = ?", username).Scan(&senderBalance)
  if err != nil {
    tx.Rollback()
    return &pb.PaymentResponse{Success: false, Message: "User not found"}, nil
  }
  if senderBalance < req.Amount {
    tx.Rollback()
    return &pb.PaymentResponse{Success: false, Message: "Insufficient funds"}, nil
  }
  _, err = tx.ExecContext(ctx, "UPDATE accounts SET balance = balance - ? WHERE username = ?", req.Amount, username)
  if err != nil {
    tx.Rollback()
    return &pb.PaymentResponse{Success: false, Message: "Failed to update balance"}, nil
  }
  if err = tx.Commit(); err != nil {
    return &pb.PaymentResponse{Success: false, Message: "Failed to commit balance update"}, nil
  }
  // 2PC with banks.
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
    TransactionId:    transactionID,
    Amount:           req.Amount,
    RecipientAccount: req.RecipientAccount,
  }
  prepareCtx, cancel := context.WithTimeout(context.Background(), s.bankTimeout)
  defer cancel()
  log.Printf("[2PC] Initiating prepare phase for transaction %s (recipient: %s)", transactionID, req.RecipientAccount)
  senderPrep, err := senderBank.PrepareTransaction(prepareCtx, prepareReq)
  if err != nil || !senderPrep.Success {
    log.Printf("[2PC] Sender bank prepare failed for transaction %s: %v", transactionID, err)
    senderBank.AbortTransaction(context.Background(), prepareReq)
    response := &pb.PaymentResponse{Success: false, Message: "Sender bank preparation failed"}
    s.storeResponse(req.IdempotencyKey, response)
    return response, nil
  }
  receiverPrep, err := receiverBank.PrepareTransaction(prepareCtx, prepareReq)
  if err != nil || !receiverPrep.Success {
    log.Printf("[2PC] Receiver bank prepare failed for transaction %s: %v", transactionID, err)
    senderBank.AbortTransaction(context.Background(), prepareReq)
    receiverBank.AbortTransaction(context.Background(), prepareReq)
    response := &pb.PaymentResponse{Success: false, Message: "Receiver bank preparation failed"}
    s.storeResponse(req.IdempotencyKey, response)
    return response, nil
  }
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
      response := &pb.PaymentResponse{Success: false, Message: "Bank commit failed"}
      s.storeResponse(req.IdempotencyKey, response)
      return response, nil
    }
    if senderCommit == nil {
      senderCommit = res.resp
    } else {
      receiverCommit = res.resp
    }
  }
  log.Printf("[2PC] Both banks committed transaction %s: Sender: %s; Receiver: %s",
    transactionID, senderCommit.Message, receiverCommit.Message)
  response := &pb.PaymentResponse{Success: true, Message: "Payment processed successfully"}
  s.storeResponse(req.IdempotencyKey, response)
  return response, nil
}

func extractUsername(token string) (string, error) {
  if len(token) <= 6 || token[len(token)-6:] != "-token" {
    return "", fmt.Errorf("Invalid token format")
  }
  return token[:len(token)-6], nil
}

func loggingInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
  start := time.Now()
  log.Printf("[INTERCEPTOR] Request: Method=%s; Time=%s; Request=%+v", info.FullMethod, start.Format(time.RFC3339), req)
  resp, err := handler(ctx, req)
  log.Printf("[INTERCEPTOR] Response: Method=%s; Duration=%s; Response=%+v; Error=%v", info.FullMethod, time.Since(start), resp, err)
  return resp, err
}

func authInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
  // Allow registration, authentication, and search without token.
  if info.FullMethod == "/payment.PaymentGateway/Authenticate" ||
    info.FullMethod == "/payment.PaymentGateway/RegisterClient" ||
    info.FullMethod == "/payment.PaymentGateway/SearchAccounts" {
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
  db, err := sql.Open("sqlite3", "../data/pg.db")
  if err != nil {
    log.Fatalf("Failed to open SQLite database: %v", err)
  }
  if err := initDB(db); err != nil {
    log.Fatalf("Failed to initialize database: %v", err)
  }
  serverCreds, err := credentials.NewServerTLSFromFile("../certificates/server.crt", "../certificates/server.key")
  if err != nil {
    log.Fatalf("[STARTUP] Failed to load TLS credentials: %v", err)
  }
  opts := []grpc.ServerOption{
    grpc.Creds(serverCreds),
    grpc.ChainUnaryInterceptor(authInterceptor, loggingInterceptor),
  }
  grpcServer := grpc.NewServer(opts...)
  pgServer := NewPaymentGatewayServer(db, time.Duration(*bankTimeoutFlag)*time.Second)
  caCert, err := os.ReadFile("../certificates/ca.crt")
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
  go func() {
    ch := make(chan os.Signal, 1)
    signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
    <-ch
    log.Println("[SHUTDOWN] Shutting down Payment Gateway server...")
    for _, conn := range bankConns {
      conn.Close()
    }
    done := make(chan struct{})
    go func() {
      grpcServer.GracefulStop()
      close(done)
    }()
    select {
    case <-done:
      log.Println("[SHUTDOWN] Graceful stop completed.")
    case <-time.After(10 * time.Second):
      log.Println("[SHUTDOWN] Graceful stop timed out, forcing shutdown.")
      grpcServer.Stop()
    }
    os.Exit(0)
  }()
  if err := grpcServer.Serve(listener); err != nil {
    log.Fatalf("[RUNTIME] Failed to serve: %v", err)
  }
}

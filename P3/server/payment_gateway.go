package main

import (
  "context"
  "crypto/tls"
  "fmt"
  "io/ioutil"
  "log"
  "net"
  "sync"
  "time"

  "google.golang.org/grpc"
  "google.golang.org/grpc/credentials"
  "google.golang.org/grpc/metadata"

  pb "github.com/kshitij-n24/DS-assignment2/P3/protofiles"
)

// PaymentGatewayServer implements the PaymentGateway service.
type PaymentGatewayServer struct {
  pb.UnimplementedPaymentGatewayServer
  userCreds             map[string]string            // username -> password
  userBalances          map[string]float64           // username -> balance
  processedTransactions map[string]*pb.PaymentResponse // idempotency key -> response
  bankConns             map[string]pb.BankServiceClient
  mu                    sync.Mutex
}

// NewPaymentGatewayServer initializes the service with dummy data.
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

// Authenticate validates the username and password.
func (s *PaymentGatewayServer) Authenticate(ctx context.Context, req *pb.AuthRequest) (*pb.AuthResponse, error) {
  log.Printf("Authenticate request for user: %s", req.Username)
  pwd, exists := s.userCreds[req.Username]
  if !exists || pwd != req.Password {
    return &pb.AuthResponse{
      Success: false,
      Message: "Invalid credentials",
    }, nil
  }
  // Generate a simple token (for production use JWT or similar).
  token := req.Username + "-token"
  return &pb.AuthResponse{
    Success: true,
    Token:   token,
    Message: "Authentication successful",
  }, nil
}

// GetBalance returns the user’s balance. (In a real system, the token would be validated.)
func (s *PaymentGatewayServer) GetBalance(ctx context.Context, req *pb.BalanceRequest) (*pb.BalanceResponse, error) {
  // For simplicity, assume token format is "username-token".
  username := ""
  if len(req.Token) > 6 && req.Token[len(req.Token)-6:] == "-token" {
    username = req.Token[:len(req.Token)-6]
  } else {
    return &pb.BalanceResponse{
      Balance: 0,
      Message: "Invalid token format",
    }, nil
  }
  s.mu.Lock()
  balance, exists := s.userBalances[username]
  s.mu.Unlock()
  if !exists {
    return &pb.BalanceResponse{
      Balance: 0,
      Message: "User not found",
    }, nil
  }
  return &pb.BalanceResponse{
    Balance: balance,
    Message: "Balance fetched successfully",
  }, nil
}

// ProcessPayment performs a 2-phase commit (2PC) to process the payment and checks idempotency.
func (s *PaymentGatewayServer) ProcessPayment(ctx context.Context, req *pb.PaymentRequest) (*pb.PaymentResponse, error) {
  // Extract username from token.
  token := req.Token
  username := ""
  if len(token) > 6 && token[len(token)-6:] == "-token" {
    username = token[:len(token)-6]
  } else {
    return &pb.PaymentResponse{
      Success: false,
      Message: "Invalid token",
    }, nil
  }

  // Idempotency check.
  s.mu.Lock()
  if res, exists := s.processedTransactions[req.IdempotencyKey]; exists {
    s.mu.Unlock()
    log.Printf("Idempotency key found, returning cached response")
    return res, nil
  }
  s.mu.Unlock()

  // Check that the user has sufficient funds.
  s.mu.Lock()
  senderBalance, exists := s.userBalances[username]
  s.mu.Unlock()
  if !exists || senderBalance < req.Amount {
    return &pb.PaymentResponse{
      Success: false,
      Message: "Insufficient funds",
    }, nil
  }

  // For demonstration, assume the sender bank is "BankA" and the receiver bank is given by req.ToBank.
  senderBankClient, ok := s.bankConns["BankA"]
  if !ok {
    return &pb.PaymentResponse{
      Success: false,
      Message: "Sender bank connection not available",
    }, nil
  }
  receiverBankClient, ok := s.bankConns[req.ToBank]
  if !ok {
    return &pb.PaymentResponse{
      Success: false,
      Message: "Receiver bank connection not available",
    }, nil
  }

  // 2PC: Prepare Phase.
  transactionID := fmt.Sprintf("%s-%d", req.IdempotencyKey, time.Now().UnixNano())
  prepareReq := &pb.BankTransactionRequest{
    TransactionId: transactionID,
    Amount:        req.Amount,
  }

  ctx2, cancel := context.WithTimeout(context.Background(), 5*time.Second)
  defer cancel()

  // Call Prepare on the sender bank.
  senderPrepare, err := senderBankClient.PrepareTransaction(ctx2, prepareReq)
  if err != nil || !senderPrepare.Success {
    // Abort if prepare fails.
    senderBankClient.AbortTransaction(context.Background(), prepareReq)
    return &pb.PaymentResponse{
      Success: false,
      Message: "Sender bank preparation failed",
    }, nil
  }
  // Call Prepare on the receiver bank.
  receiverPrepare, err := receiverBankClient.PrepareTransaction(ctx2, prepareReq)
  if err != nil || !receiverPrepare.Success {
    senderBankClient.AbortTransaction(context.Background(), prepareReq)
    receiverBankClient.AbortTransaction(context.Background(), prepareReq)
    return &pb.PaymentResponse{
      Success: false,
      Message: "Receiver bank preparation failed",
    }, nil
  }

  // 2PC: Commit Phase.
  senderCommit, err := senderBankClient.CommitTransaction(ctx2, prepareReq)
  if err != nil || !senderCommit.Success {
    senderBankClient.AbortTransaction(context.Background(), prepareReq)
    receiverBankClient.AbortTransaction(context.Background(), prepareReq)
    return &pb.PaymentResponse{
      Success: false,
      Message: "Sender bank commit failed",
    }, nil
  }
  receiverCommit, err := receiverBankClient.CommitTransaction(ctx2, prepareReq)
  if err != nil || !receiverCommit.Success {
    // In a real system, you would execute compensation logic.
    return &pb.PaymentResponse{
      Success: false,
      Message: "Receiver bank commit failed",
    }, nil
  }

  // Update the sender’s balance.
  s.mu.Lock()
  s.userBalances[username] -= req.Amount
  s.mu.Unlock()

  response := &pb.PaymentResponse{
    Success: true,
    Message: "Payment processed successfully",
  }
  // Save response using the idempotency key.
  s.mu.Lock()
  s.processedTransactions[req.IdempotencyKey] = response
  s.mu.Unlock()

  return response, nil
}

// ----- Interceptors -----

// loggingInterceptor logs every incoming request and its response.
func loggingInterceptor(
  ctx context.Context,
  req interface{},
  info *grpc.UnaryServerInfo,
  handler grpc.UnaryHandler,
) (resp interface{}, err error) {
  start := time.Now()
  log.Printf("Request - Method:%s; Time:%s; Req:%v", info.FullMethod, start.Format(time.RFC3339), req)
  resp, err = handler(ctx, req)
  log.Printf("Response - Method:%s; Duration:%s; Resp:%v; Err:%v", info.FullMethod, time.Since(start), resp, err)
  return resp, err
}

// authInterceptor enforces that (except for Authenticate) a valid token exists in the metadata.
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
    return nil, fmt.Errorf("missing metadata")
  }
  tokens := md.Get("authorization")
  if len(tokens) == 0 || tokens[0] == "" {
    return nil, fmt.Errorf("missing authorization token")
  }
  // In production, validate the token (e.g. by verifying a JWT).
  return handler(ctx, req)
}

func main() {
  // Load TLS credentials (the certificate and key files should be in the certificate folder).
  certFile := "../../certificate/server.crt"
  keyFile := "../../certificate/server.key"
  creds, err := credentials.NewServerTLSFromFile(certFile, keyFile)
  if err != nil {
    log.Fatalf("Failed to load TLS credentials: %v", err)
  }

  // Set up gRPC server options with TLS and the interceptors.
  opts := []grpc.ServerOption{
    grpc.Creds(creds),
    grpc.UnaryInterceptor(
      grpc.ChainUnaryInterceptor(authInterceptor, loggingInterceptor),
    ),
  }
  grpcServer := grpc.NewServer(opts...)

  pgServer := NewPaymentGatewayServer()

  // For demonstration, we assume that bank servers are running and available.
  // Here we create client connections for two banks (for example, "BankA" and "BankB").
  // (In our demo, we only start one Bank server on port 50051.)
  bankNames := []string{"BankA", "BankB"}
  for _, bank := range bankNames {
    // For simplicity we reuse the same TLS credentials.
    conn, err := grpc.Dial("localhost:50051", grpc.WithTransportCredentials(creds))
    if err != nil {
      log.Printf("Failed to connect to bank server %s: %v", bank, err)
      continue
    }
    pgServer.bankConns[bank] = pb.NewBankServiceClient(conn)
  }

  // Register the PaymentGateway service.
  pb.RegisterPaymentGatewayServer(grpcServer, pgServer)

  lis, err := net.Listen("tcp", ":50060")
  if err != nil {
    log.Fatalf("Failed to listen: %v", err)
  }
  log.Printf("Payment Gateway server listening on :50060")
  if err := grpcServer.Serve(lis); err != nil {
    log.Fatalf("Failed to serve: %v", err)
  }
}

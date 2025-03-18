package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"log"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"

	pb "github.com/<user>/DS-assignment2/P3/protofiles"
)

var (
	offlineQueue = struct {
		sync.Mutex
		queue []*pb.PaymentRequest
	}{}
	pgClient pb.PaymentGatewayClient
	pgConn   *grpc.ClientConn
	clientMu sync.Mutex
)

// connectPaymentGateway continuously attempts to establish a connection to the Payment Gateway.
func connectPaymentGateway(creds credentials.TransportCredentials) {
	for {
		clientMu.Lock()
		// If we already have a valid client, no need to reconnect.
		if pgClient != nil {
			clientMu.Unlock()
			time.Sleep(10 * time.Second)
			continue
		}
		clientMu.Unlock()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		conn, err := grpc.DialContext(ctx, "localhost:50060", grpc.WithTransportCredentials(creds), grpc.WithBlock())
		cancel()

		clientMu.Lock()
		if err == nil {
			pgConn = conn
			pgClient = pb.NewPaymentGatewayClient(conn)
			log.Println("[CLIENT] Payment Gateway connection established")
		} else {
			log.Printf("[CLIENT] Failed to connect to Payment Gateway: %v", err)
			pgClient = nil
		}
		clientMu.Unlock()
		time.Sleep(10 * time.Second)
	}
}

// registerClient registers a new client with the Payment Gateway.
func registerClient(username, password, bankAccount string, initialBalance float64) {
	clientMu.Lock()
	currentClient := pgClient
	clientMu.Unlock()
	if currentClient == nil {
		log.Println("[CLIENT] Cannot register client: Payment Gateway offline")
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req := &pb.RegisterClientRequest{
		Username:       username,
		Password:       password,
		BankAccount:    bankAccount,
		InitialBalance: initialBalance,
	}
	resp, err := currentClient.RegisterClient(ctx, req)
	if err != nil {
		log.Printf("[CLIENT] Registration error for user %s: %v", username, err)
		return
	}
	if !resp.Success {
		log.Printf("[CLIENT] Registration failed for user %s: %s", username, resp.Message)
	} else {
		log.Printf("[CLIENT] Registration successful for user %s", username)
	}
}

func main() {
	// Load CA certificate.
	caCertPath := "../certificates/ca.crt"
	caCert, err := ioutil.ReadFile(caCertPath)
	if err != nil {
		log.Fatalf("[CLIENT] Could not read CA certificate from %s: %v", caCertPath, err)
	}
	caCertPool := x509.NewCertPool()
	if !caCertPool.AppendCertsFromPEM(caCert) {
		log.Fatalf("[CLIENT] Failed to append CA certificate")
	}
	creds := credentials.NewTLS(&tls.Config{
		RootCAs: caCertPool,
	})

	// Start background goroutine to establish connection.
	go connectPaymentGateway(creds)

	// Wait for connection establishment.
	time.Sleep(5 * time.Second)

	// Optionally register a new client (e.g., "charlie").
	registerClient("charlie", "password3", "BankA", 300.0)

	// Authenticate existing client "alice".
	var token string
	clientMu.Lock()
	currentClient := pgClient
	clientMu.Unlock()
	if currentClient != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		authResp, err := currentClient.Authenticate(ctx, &pb.AuthRequest{
			Username: "alice",
			Password: "password1",
		})
		cancel()
		if err != nil || !authResp.Success {
			log.Printf("[CLIENT] Authentication failed: %v", err)
			token = "alice-token" // Fallback token.
		} else {
			token = authResp.Token
		}
	} else {
		log.Println("[CLIENT] Payment Gateway offline at startup. Using fallback token 'alice-token'")
		token = "alice-token"
	}
	fmt.Printf("[CLIENT] Using token: %s\n", token)

	// Build outgoing context with token.
	baseCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	md := metadata.New(map[string]string{"authorization": token})
	ctxWithMD := metadata.NewOutgoingContext(baseCtx, md)

	// Attempt to get balance if connected.
	clientMu.Lock()
	currentClient = pgClient
	clientMu.Unlock()
	if currentClient != nil {
		balResp, err := currentClient.GetBalance(ctxWithMD, &pb.BalanceRequest{Token: token})
		if err != nil {
			log.Printf("[CLIENT] Error fetching balance: %v", err)
		} else {
			fmt.Printf("[CLIENT] Current Balance: %.2f (%s)\n", balResp.Balance, balResp.Message)
		}
	} else {
		fmt.Println("[CLIENT] Cannot fetch balance: Payment Gateway offline")
	}

	// Create a payment request.
	paymentKey := "unique-payment-key-123"
	paymentReq := &pb.PaymentRequest{
		Token:          token,
		ToBank:         "BankB", // Using BankB for this payment.
		Amount:         100.0,
		IdempotencyKey: paymentKey,
	}

	// Process the payment; if it fails, it will be queued.
	processPayment(ctxWithMD, paymentReq)

	// Start a background goroutine to retry queued payments.
	go retryOfflinePayments(token)

	// Block forever.
	select {}
}

func processPayment(ctx context.Context, req *pb.PaymentRequest) {
	clientMu.Lock()
	currentClient := pgClient
	clientMu.Unlock()
	if currentClient == nil {
		log.Printf("[CLIENT] Payment Gateway offline. Queuing payment with key %s.", req.IdempotencyKey)
		queuePayment(req)
		return
	}
	resp, err := currentClient.ProcessPayment(ctx, req)
	if err != nil {
		log.Printf("[CLIENT] Payment processing failed: %v. Queuing payment with key %s.", err, req.IdempotencyKey)
		clientMu.Lock()
		pgClient = nil
		clientMu.Unlock()
		queuePayment(req)
		return
	}
	if !resp.Success {
		log.Printf("[CLIENT] Payment processing failed: %s. Queuing payment with key %s.", resp.Message, req.IdempotencyKey)
		queuePayment(req)
		return
	}
	fmt.Printf("[CLIENT] Payment Response: %s\n", resp.Message)

	// Idempotency check: resend the same payment.
	resp2, err := currentClient.ProcessPayment(ctx, req)
	if err != nil {
		log.Printf("[CLIENT] Payment processing retry failed: %v. Queuing payment with key %s.", err, req.IdempotencyKey)
		clientMu.Lock()
		pgClient = nil
		clientMu.Unlock()
		queuePayment(req)
		return
	}
	if !resp2.Success {
		log.Printf("[CLIENT] Payment processing retry failed: %s. Queuing payment with key %s.", resp2.Message, req.IdempotencyKey)
		queuePayment(req)
		return
	}
	fmt.Printf("[CLIENT] Payment Response on idempotent retry: %s\n", resp2.Message)
}

func queuePayment(req *pb.PaymentRequest) {
	offlineQueue.Lock()
	defer offlineQueue.Unlock()
	for _, r := range offlineQueue.queue {
		if r.IdempotencyKey == req.IdempotencyKey {
			log.Printf("[CLIENT] Payment with key %s is already queued", req.IdempotencyKey)
			return
		}
	}
	offlineQueue.queue = append(offlineQueue.queue, req)
	log.Printf("[CLIENT] Queued payment with key %s for offline processing", req.IdempotencyKey)
}

func retryOfflinePayments(token string) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		clientMu.Lock()
		currentClient := pgClient
		clientMu.Unlock()
		if currentClient == nil {
			log.Println("[CLIENT] Still offline; cannot process queued payments.")
			continue
		}
		offlineQueue.Lock()
		if len(offlineQueue.queue) == 0 {
			offlineQueue.Unlock()
			continue
		}
		log.Println("[CLIENT] Retrying offline queued payments...")
		var remaining []*pb.PaymentRequest
		for _, req := range offlineQueue.queue {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			md := metadata.New(map[string]string{"authorization": token})
			ctx = metadata.NewOutgoingContext(ctx, md)
			resp, err := currentClient.ProcessPayment(ctx, req)
			cancel()
			if err != nil || !resp.Success {
				log.Printf("[CLIENT] Retry failed for payment key %s: %v", req.IdempotencyKey, err)
				remaining = append(remaining, req)
				continue
			}
			log.Printf("[CLIENT] Queued payment with key %s processed successfully", req.IdempotencyKey)
		}
		offlineQueue.queue = remaining
		offlineQueue.Unlock()
	}
}

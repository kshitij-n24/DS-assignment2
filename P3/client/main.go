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

	pb "github.com/kshitij-n24/DS-assignment2/P3/protofiles"
)

// paymentQueue holds payment requests that failed to be processed (simulating offline mode).
var paymentQueue = struct {
	sync.Mutex
	queue []*pb.PaymentRequest
}{}

func main() {
	// Load the CA certificate to verify the server.
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
		// Note: For production do not use InsecureSkipVerify.
	})

	// Connect to the Payment Gateway server.
	conn, err := grpc.Dial("localhost:50060", grpc.WithTransportCredentials(creds))
	if err != nil {
		log.Fatalf("[CLIENT] Failed to connect to Payment Gateway server: %v", err)
	}
	defer conn.Close()
	client := pb.NewPaymentGatewayClient(conn)
	log.Println("[CLIENT] Connected to Payment Gateway server on localhost:50060")

	// Create a context with timeout.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// --- Step 1: Authenticate ---
	authResp, err := client.Authenticate(ctx, &pb.AuthRequest{
		Username: "alice",
		Password: "password1",
	})
	if err != nil {
		log.Fatalf("[CLIENT] Authentication error: %v", err)
	}
	if !authResp.Success {
		log.Fatalf("[CLIENT] Authentication failed: %s", authResp.Message)
	}
	token := authResp.Token
	fmt.Printf("[CLIENT] Authenticated successfully. Token: %s\n", token)

	// Prepare a metadata context with token.
	md := metadata.New(map[string]string{"authorization": token})
	ctxWithMD := metadata.NewOutgoingContext(ctx, md)

	// --- Step 2: Get Balance ---
	balResp, err := client.GetBalance(ctxWithMD, &pb.BalanceRequest{Token: token})
	if err != nil {
		log.Fatalf("[CLIENT] Error fetching balance: %v", err)
	}
	fmt.Printf("[CLIENT] Current Balance: %.2f (%s)\n", balResp.Balance, balResp.Message)

	// --- Step 3: Process Payment ---
	paymentKey := "unique-payment-key-123"
	paymentAmount := 100.0
	paymentReq := &pb.PaymentRequest{
		Token:          token,
		ToBank:         "BankA", // For demo, use BankA as receiver.
		Amount:         paymentAmount,
		IdempotencyKey: paymentKey,
	}

	// Try to process payment.
	processPayment(ctxWithMD, client, paymentReq)

	// Start a background goroutine to retry queued payments.
	go retryQueuedPayments(client, token)

	// For demo purposes, wait enough time to observe retries.
	time.Sleep(60 * time.Second)
}

// processPayment attempts to send a payment request. On failure (simulated as connectivity error),
// it queues the payment for retry.
func processPayment(ctx context.Context, client pb.PaymentGatewayClient, req *pb.PaymentRequest) {
	resp, err := client.ProcessPayment(ctx, req)
	if err != nil {
		log.Printf("[CLIENT] Payment processing error: %v", err)
		// For network-related errors, queue the payment.
		queuePayment(req)
		return
	}
	fmt.Printf("[CLIENT] Payment Response: %s\n", resp.Message)
	// Also try idempotency by re-sending.
	resp2, err := client.ProcessPayment(ctx, req)
	if err != nil {
		log.Printf("[CLIENT] Payment processing error on retry: %v", err)
		queuePayment(req)
		return
	}
	fmt.Printf("[CLIENT] Payment Response on idempotent retry: %s\n", resp2.Message)
}

// queuePayment adds the payment request to the offline queue.
func queuePayment(req *pb.PaymentRequest) {
	paymentQueue.Lock()
	defer paymentQueue.Unlock()
	// Check for duplicates.
	for _, r := range paymentQueue.queue {
		if r.IdempotencyKey == req.IdempotencyKey {
			log.Printf("[CLIENT] Payment with key %s is already queued", req.IdempotencyKey)
			return
		}
	}
	paymentQueue.queue = append(paymentQueue.queue, req)
	log.Printf("[CLIENT] Payment with key %s queued for offline processing", req.IdempotencyKey)
}

// retryQueuedPayments periodically attempts to process queued payments.
func retryQueuedPayments(client pb.PaymentGatewayClient, token string) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		paymentQueue.Lock()
		if len(paymentQueue.queue) == 0 {
			paymentQueue.Unlock()
			continue
		}
		log.Println("[CLIENT] Retrying queued payments...")
		remaining := []*pb.PaymentRequest{}
		for _, req := range paymentQueue.queue {
			// Create a fresh context with metadata for each retry.
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			md := metadata.New(map[string]string{"authorization": token})
			ctx = metadata.NewOutgoingContext(ctx, md)
			resp, err := client.ProcessPayment(ctx, req)
			cancel()
			if err != nil {
				log.Printf("[CLIENT] Retry failed for payment key %s: %v", req.IdempotencyKey, err)
				remaining = append(remaining, req)
				continue
			}
			if resp.Success {
				log.Printf("[CLIENT] Queued payment with key %s processed successfully", req.IdempotencyKey)
			} else {
				log.Printf("[CLIENT] Queued payment with key %s failed: %s", req.IdempotencyKey, resp.Message)
				remaining = append(remaining, req)
			}
		}
		paymentQueue.queue = remaining
		paymentQueue.Unlock()
	}
}

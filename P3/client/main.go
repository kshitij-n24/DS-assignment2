package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"

	pb "github.com/kshitij-n24/DS-assignment2/P3/protofiles"
)

func main() {
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

	conn, err := grpc.Dial("localhost:50060", grpc.WithTransportCredentials(creds))
	if err != nil {
		log.Fatalf("[CLIENT] Failed to connect to Payment Gateway server: %v", err)
	}
	defer conn.Close()
	client := pb.NewPaymentGatewayClient(conn)
	log.Println("[CLIENT] Connected to Payment Gateway server on localhost:50060")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

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

	md := metadata.New(map[string]string{"authorization": token})
	ctxWithMD := metadata.NewOutgoingContext(ctx, md)

	balResp, err := client.GetBalance(ctxWithMD, &pb.BalanceRequest{Token: token})
	if err != nil {
		log.Fatalf("[CLIENT] Error fetching balance: %v", err)
	}
	fmt.Printf("[CLIENT] Current Balance: %.2f (%s)\n", balResp.Balance, balResp.Message)

	// IMPORTANT: To simulate two different banks, set ToBank to "BankB"
	paymentKey := "unique-payment-key-123"
	paymentReq := &pb.PaymentRequest{
		Token:          token,
		ToBank:         "BankB",
		Amount:         100.0,
		IdempotencyKey: paymentKey,
	}

	payResp, err := client.ProcessPayment(ctxWithMD, paymentReq)
	if err != nil {
		log.Fatalf("[CLIENT] Payment processing error: %v", err)
	}
	fmt.Printf("[CLIENT] Payment Response: %s\n", payResp.Message)

	payResp2, err := client.ProcessPayment(ctxWithMD, paymentReq)
	if err != nil {
		log.Fatalf("[CLIENT] Payment processing error on retry: %v", err)
	}
	fmt.Printf("[CLIENT] Payment Response on idempotent retry: %s\n", payResp2.Message)
}

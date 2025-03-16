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

	pb "protofiles/payment"
)

func main() {
	// Load CA certificate to verify the server certificate.
	caCert, err := ioutil.ReadFile("../certificate/ca.crt")
	if err != nil {
		log.Fatalf("Could not read CA certificate: %v", err)
	}
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)

	creds := credentials.NewTLS(&tls.Config{
		RootCAs: caCertPool,
	})

	// Connect to the Payment Gateway server.
	conn, err := grpc.Dial("localhost:50060", grpc.WithTransportCredentials(creds))
	if err != nil {
		log.Fatalf("Did not connect: %v", err)
	}
	defer conn.Close()
	client := pb.NewPaymentGatewayClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Authenticate with the Payment Gateway.
	authResp, err := client.Authenticate(ctx, &pb.AuthRequest{
		Username: "alice",
		Password: "password1",
	})
	if err != nil {
		log.Fatalf("Authentication failed: %v", err)
	}
	if !authResp.Success {
		log.Fatalf("Authentication failed: %s", authResp.Message)
	}
	token := authResp.Token
	fmt.Printf("Authenticated. Token: %s\n", token)

	// Create a metadata context with the token.
	md := metadata.New(map[string]string{"authorization": token})
	ctx = metadata.NewOutgoingContext(ctx, md)

	// Get balance.
	balResp, err := client.GetBalance(ctx, &pb.BalanceRequest{
		Token: token,
	})
	if err != nil {
		log.Fatalf("Could not get balance: %v", err)
	}
	fmt.Printf("Balance: %.2f\n", balResp.Balance)

	// Process a payment.
	paymentKey := "unique-payment-key-123"
	payResp, err := client.ProcessPayment(ctx, &pb.PaymentRequest{
		Token:          token,
		ToBank:         "BankA", // For demo purposes, we use BankA as the receiver.
		Amount:         100.0,
		IdempotencyKey: paymentKey,
	})
	if err != nil {
		log.Fatalf("Payment processing failed: %v", err)
	}
	fmt.Printf("Payment Response: %s\n", payResp.Message)

	// Repeat the payment with the same idempotency key to demonstrate idempotency.
	payResp2, err := client.ProcessPayment(ctx, &pb.PaymentRequest{
		Token:          token,
		ToBank:         "BankA",
		Amount:         100.0,
		IdempotencyKey: paymentKey,
	})
	if err != nil {
		log.Fatalf("Payment processing failed: %v", err)
	}
	fmt.Printf("Payment Response (idempotent call): %s\n", payResp2.Message)
}

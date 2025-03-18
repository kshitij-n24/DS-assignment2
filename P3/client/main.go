package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"

	pb "github.com/kshitij-n24/DS-assignment2/P3/protofiles"
)

var (
	offlineQueue = struct {
		sync.Mutex
		queue []*pb.PaymentRequest
	}{}
	pgClient pb.PaymentGatewayClient
	pgConn   *grpc.ClientConn
	clientMu sync.Mutex

	// currentToken holds the token of the last authenticated user.
	currentToken string
)

func init() {
	// Seed the random number generator for idempotency key generation.
	rand.Seed(time.Now().UnixNano())
}

// generateIdempotencyKey creates a unique idempotency key.
func generateIdempotencyKey() string {
	return fmt.Sprintf("key-%d-%d", time.Now().UnixNano(), rand.Intn(10000))
}

// connectPaymentGateway continuously attempts to establish a connection to the Payment Gateway.
func connectPaymentGateway(creds credentials.TransportCredentials) {
	for {
		clientMu.Lock()
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
		clientMu.Lock()
		pgClient = nil
		clientMu.Unlock()
		return
	}
	if !resp.Success {
		log.Printf("[CLIENT] Registration failed for user %s: %s", username, resp.Message)
	} else {
		log.Printf("[CLIENT] Registration successful for user %s", username)
	}
}

// authenticate performs authentication with the Payment Gateway.
func authenticate(username, password string) {
	clientMu.Lock()
	currentClient := pgClient
	clientMu.Unlock()
	if currentClient == nil {
		log.Println("[CLIENT] Cannot authenticate: Payment Gateway offline")
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := currentClient.Authenticate(ctx, &pb.AuthRequest{
		Username: username,
		Password: password,
	})
	if err != nil || !resp.Success {
		log.Printf("[CLIENT] Authentication failed for user %s: %v", username, err)
		clientMu.Lock()
		pgClient = nil
		clientMu.Unlock()
		return
	}
	currentToken = resp.Token
	log.Printf("[CLIENT] Authentication successful. Token: %s", currentToken)
}

// getBalance fetches the balance for the currently authenticated user.
func getBalance() {
	if currentToken == "" {
		log.Println("[CLIENT] Please authenticate first using the auth command.")
		return
	}
	clientMu.Lock()
	currentClient := pgClient
	clientMu.Unlock()
	if currentClient == nil {
		log.Println("[CLIENT] Cannot fetch balance: Payment Gateway offline")
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	md := metadata.New(map[string]string{"authorization": currentToken})
	ctx = metadata.NewOutgoingContext(ctx, md)
	resp, err := currentClient.GetBalance(ctx, &pb.BalanceRequest{Token: currentToken})
	if err != nil {
		log.Printf("[CLIENT] Error fetching balance: %v", err)
		return
	}
	fmt.Printf("[CLIENT] Current Balance: %.2f (%s)\n", resp.Balance, resp.Message)
}

// processPayment sends a payment request to the Payment Gateway.
// If no idempotency key is provided, it is generated automatically.
func processPayment(toBank string, amount float64, providedKey string) {
	if currentToken == "" {
		log.Println("[CLIENT] Please authenticate first using the auth command.")
		return
	}
	clientMu.Lock()
	currentClient := pgClient
	clientMu.Unlock()
	if currentClient == nil {
		log.Printf("[CLIENT] Payment Gateway offline. Queuing payment.")
		req := &pb.PaymentRequest{
			Token:  currentToken,
			ToBank: toBank,
			Amount: amount,
		}
		// Auto-generate key if not provided.
		if providedKey == "" {
			req.IdempotencyKey = generateIdempotencyKey()
		} else {
			req.IdempotencyKey = providedKey
		}
		queuePayment(req)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	md := metadata.New(map[string]string{"authorization": currentToken})
	ctx = metadata.NewOutgoingContext(ctx, md)
	req := &pb.PaymentRequest{
		Token:  currentToken,
		ToBank: toBank,
		Amount: amount,
	}
	if providedKey == "" {
		req.IdempotencyKey = generateIdempotencyKey()
	} else {
		req.IdempotencyKey = providedKey
	}
	resp, err := currentClient.ProcessPayment(ctx, req)
	if err != nil {
		log.Printf("[CLIENT] Payment processing failed: %v. Queuing payment.", err)
		clientMu.Lock()
		pgClient = nil
		clientMu.Unlock()
		queuePayment(req)
		return
	}
	if !resp.Success {
		log.Printf("[CLIENT] Payment processing failed: %s. Queuing payment.", resp.Message)
		queuePayment(req)
		return
	}
	fmt.Printf("[CLIENT] Payment Response: %s\n", resp.Message)
}

// queuePayment adds a payment request to the offline queue.
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

// retryOfflinePayments periodically retries queued payments.
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

// printHelp displays the list of available commands.
func printHelp() {
	helpText := `
Available commands:
  register <username> <password> <bankAccount> <initialBalance>   Register a new client.
  auth <username> <password>                                       Authenticate and obtain a token.
  balance                                                          Get the current balance.
  pay <to_bank> <amount> [idempotency_key]                           Make a payment (idempotency key is optional).
  help                                                             Display this help message.
  exit                                                             Exit the client.
`
	fmt.Println(helpText)
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

	// Start offline payment retry goroutine.
	go retryOfflinePayments(currentToken)

	// Read user commands from stdin.
	scanner := bufio.NewScanner(os.Stdin)
	fmt.Println("Interactive Payment Client")
	printHelp()
	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			break
		}
		line := scanner.Text()
		if len(strings.TrimSpace(line)) == 0 {
			continue
		}
		tokens := strings.Fields(line)
		command := strings.ToLower(tokens[0])
		switch command {
		case "help":
			printHelp()
		case "exit":
			fmt.Println("Exiting client.")
			os.Exit(0)
		case "register":
			if len(tokens) != 5 {
				fmt.Println("Usage: register <username> <password> <bankAccount> <initialBalance>")
				continue
			}
			initialBalance, err := strconv.ParseFloat(tokens[4], 64)
			if err != nil {
				fmt.Println("Invalid initialBalance value.")
				continue
			}
			registerClient(tokens[1], tokens[2], tokens[3], initialBalance)
		case "auth":
			if len(tokens) != 3 {
				fmt.Println("Usage: auth <username> <password>")
				continue
			}
			authenticate(tokens[1], tokens[2])
		case "balance":
			getBalance()
		case "pay":
			// Support both: pay <to_bank> <amount> [idempotency_key]
			if len(tokens) < 3 || len(tokens) > 4 {
				fmt.Println("Usage: pay <to_bank> <amount> [idempotency_key]")
				continue
			}
			amount, err := strconv.ParseFloat(tokens[2], 64)
			if err != nil {
				fmt.Println("Invalid amount value.")
				continue
			}
			providedKey := ""
			if len(tokens) == 4 {
				providedKey = tokens[3]
			}
			processPayment(tokens[1], amount, providedKey)
		default:
			fmt.Println("Unknown command. Type 'help' for available commands.")
		}
	}
}

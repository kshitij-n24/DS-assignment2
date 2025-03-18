package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"log"
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

	currentToken string
)

func generateIdempotencyKey() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("key-%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b)
}

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

func registerClient(username, password, bankAccount string, initialBalance float64) {
	if strings.TrimSpace(username) == "" || strings.TrimSpace(password) == "" || strings.TrimSpace(bankAccount) == "" {
		log.Println("[CLIENT] Username, password, and bank account must be non-empty.")
		return
	}
	clientMu.Lock()
	currentClient := pgClient
	clientMu.Unlock()
	if currentClient == nil {
		log.Println("[CLIENT] Cannot register client: Payment Gateway offline")
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
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

func authenticate(username, password string) {
	if strings.TrimSpace(username) == "" || strings.TrimSpace(password) == "" {
		log.Println("[CLIENT] Username and password must be non-empty.")
		return
	}
	clientMu.Lock()
	currentClient := pgClient
	clientMu.Unlock()
	if currentClient == nil {
		log.Println("[CLIENT] Cannot authenticate: Payment Gateway offline")
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
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

func searchAccounts(query string) {
	// Allow search even if not authenticated.
	clientMu.Lock()
	currentClient := pgClient
	clientMu.Unlock()
	if currentClient == nil {
		log.Println("[CLIENT] Cannot search accounts: Payment Gateway offline")
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// Even if token is missing, SearchAccounts is allowed per our interceptor.
	req := &pb.SearchAccountsRequest{Query: query}
	resp, err := currentClient.SearchAccounts(ctx, req)
	if err != nil {
		log.Printf("[CLIENT] Search error: %v", err)
		return
	}
	if len(resp.Accounts) == 0 {
		fmt.Println("No accounts found.")
		return
	}
	fmt.Println("Accounts found:")
	for _, acc := range resp.Accounts {
		fmt.Printf("Username: %s, BankAccount: %s, Balance: %.2f\n", acc.Username, acc.BankAccount, acc.Balance)
	}
}

func processPayment(toBank, recipientAccount string, amount float64, providedKey string) {
	if strings.TrimSpace(toBank) == "" || strings.TrimSpace(recipientAccount) == "" {
		log.Println("[CLIENT] Bank and recipient account must be non-empty.")
		return
	}
	if amount <= 0 {
		fmt.Println("Invalid amount value. Must be a positive number.")
		return
	}
	clientMu.Lock()
	currentClient := pgClient
	clientMu.Unlock()
	if currentClient == nil {
		log.Printf("[CLIENT] Payment Gateway offline. Queuing payment.")
		req := &pb.PaymentRequest{
			Token:            currentToken,
			ToBank:           toBank,
			Amount:           amount,
			RecipientAccount: recipientAccount,
		}
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
		Token:            currentToken,
		ToBank:           toBank,
		Amount:           amount,
		RecipientAccount: recipientAccount,
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
		log.Printf("[CLIENT] Payment processing failed: %s. Not queuing transaction.", resp.Message)
		return
	}
	fmt.Printf("[CLIENT] Payment Response: %s\n", resp.Message)
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
			// Update token from global if available.
			if currentToken != "" {
				req.Token = currentToken
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			md := metadata.New(map[string]string{"authorization": req.Token})
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

func printHelp() {
	helpText := `
Available commands:
  register <username> <password> <bankAccount> <initialBalance>   Register a new client.
  auth <username> <password>                                       Authenticate and obtain a token.
  balance                                                          Get the current balance.
  pay <to_bank> <recipient_account> <amount> [idempotency_key]       Make a payment (recipient account is required; idempotency key is optional).
  search [query]                                                   List all accounts if query is omitted, or search accounts by username or bank account.
  help                                                             Display this help message.
  exit                                                             Exit the client.
`
	fmt.Println(helpText)
}

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

	go connectPaymentGateway(creds)
	go retryOfflinePayments(currentToken)

	time.Sleep(1 * time.Second)
	fmt.Println("Interactive Payment Client")
	printHelp()
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		fmt.Print("> ")
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
			initialBalance, err := strconv.ParseFloat(strings.TrimSpace(tokens[4]), 64)
			if err != nil || initialBalance <= 0 {
				fmt.Println("Invalid initialBalance value. Must be a positive number.")
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
			// Expected: pay <to_bank> <recipient_account> <amount> [idempotency_key]
			if len(tokens) < 4 || len(tokens) > 5 {
				fmt.Println("Usage: pay <to_bank> <recipient_account> <amount> [idempotency_key]")
				continue
			}
			amount, err := strconv.ParseFloat(strings.TrimSpace(tokens[3]), 64)
			if err != nil || amount <= 0 {
				fmt.Println("Invalid amount value. Must be a positive number.")
				continue
			}
			providedKey := ""
			if len(tokens) == 5 {
				providedKey = tokens[4]
			}
			processPayment(tokens[1], tokens[2], amount, providedKey)
		case "search":
			query := ""
			if len(tokens) > 1 {
				query = strings.Join(tokens[1:], " ")
			}
			searchAccounts(query)
		default:
			fmt.Println("Unknown command. Type 'help' for available commands.")
		}
	}
}

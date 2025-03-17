package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	pb "github.com/kshitij-n24/DS-assignment2/P3/protofiles"
)

// BankServer implements the BankService.
type BankServer struct {
	pb.UnimplementedBankServiceServer

	name                 string
	preparedTransactions map[string]float64 // transactionID -> amount
	mu                   sync.Mutex
}

// NewBankServer creates a new bank server instance.
func NewBankServer(name string) *BankServer {
	return &BankServer{
		name:                 name,
		preparedTransactions: make(map[string]float64),
	}
}

// PrepareTransaction simulates the prepare phase.
func (bs *BankServer) PrepareTransaction(ctx context.Context, req *pb.BankTransactionRequest) (*pb.BankTransactionResponse, error) {
	bs.mu.Lock()
	defer bs.mu.Unlock()
	// Simulate preparation. In production, you might check account limits, etc.
	bs.preparedTransactions[req.TransactionId] = req.Amount
	log.Printf("[%s] Prepared transaction %s for amount %.2f", bs.name, req.TransactionId, req.Amount)
	return &pb.BankTransactionResponse{
		Success: true,
		Message: fmt.Sprintf("Transaction prepared at %s", bs.name),
	}, nil
}

// CommitTransaction commits a prepared transaction.
func (bs *BankServer) CommitTransaction(ctx context.Context, req *pb.BankTransactionRequest) (*pb.BankTransactionResponse, error) {
	bs.mu.Lock()
	defer bs.mu.Unlock()
	amount, exists := bs.preparedTransactions[req.TransactionId]
	if !exists {
		log.Printf("[%s] Commit failed: transaction %s not found", bs.name, req.TransactionId)
		return &pb.BankTransactionResponse{
			Success: false,
			Message: "Transaction not found for commit",
		}, nil
	}
	delete(bs.preparedTransactions, req.TransactionId)
	log.Printf("[%s] Committed transaction %s for amount %.2f", bs.name, req.TransactionId, amount)
	return &pb.BankTransactionResponse{
		Success: true,
		Message: fmt.Sprintf("Transaction committed at %s", bs.name),
	}, nil
}

// AbortTransaction aborts a prepared transaction.
func (bs *BankServer) AbortTransaction(ctx context.Context, req *pb.BankTransactionRequest) (*pb.BankTransactionResponse, error) {
	bs.mu.Lock()
	defer bs.mu.Unlock()
	if _, exists := bs.preparedTransactions[req.TransactionId]; exists {
		delete(bs.preparedTransactions, req.TransactionId)
		log.Printf("[%s] Aborted transaction %s", bs.name, req.TransactionId)
	} else {
		log.Printf("[%s] Abort called on unknown transaction %s", bs.name, req.TransactionId)
	}
	return &pb.BankTransactionResponse{
		Success: true,
		Message: fmt.Sprintf("Transaction aborted at %s", bs.name),
	}, nil
}

func main() {
	// Updated certificate directory is "certificates".
	certFile := "../certificates/server.crt"
	keyFile := "../certificates/server.key"
	creds, err := credentials.NewServerTLSFromFile(certFile, keyFile)
	if err != nil {
		log.Fatalf("[STARTUP] Failed to load TLS credentials from %s and %s: %v", certFile, keyFile, err)
	}

	opts := []grpc.ServerOption{grpc.Creds(creds)}
	grpcServer := grpc.NewServer(opts...)

	// Initialize the bank server.
	bankName := "BankA"
	bankServer := NewBankServer(bankName)
	pb.RegisterBankServiceServer(grpcServer, bankServer)

	listener, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("[STARTUP] Failed to listen on :50051: %v", err)
	}
	log.Printf("[STARTUP] %s Bank server is listening on :50051", bankName)

	// Handle graceful shutdown.
	go func() {
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
		<-ch
		log.Printf("[SHUTDOWN] Shutting down %s Bank server...", bankName)
		grpcServer.GracefulStop()
		os.Exit(0)
	}()

	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("[RUNTIME] Failed to serve: %v", err)
	}
}

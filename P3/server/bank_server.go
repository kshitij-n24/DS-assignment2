package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"net"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	pb "github.com/kshitij-n24/DS-assignment2/P3/protofiles"
)

// BankServer implements the BankService.
type BankServer struct {
	pb.UnimplementedBankServiceServer
	name                 string
	preparedTransactions map[string]float64 // transaction ID -> amount
	mu                   sync.Mutex
}

// NewBankServer creates a new BankServer.
func NewBankServer(name string) *BankServer {
	return &BankServer{
		name:                 name,
		preparedTransactions: make(map[string]float64),
	}
}

// PrepareTransaction simulates a prepare step.
func (bs *BankServer) PrepareTransaction(ctx context.Context, req *pb.BankTransactionRequest) (*pb.BankTransactionResponse, error) {
	bs.mu.Lock()
	defer bs.mu.Unlock()
	// For demonstration, always “prepare” successfully.
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
	}
	log.Printf("[%s] Aborted transaction %s", bs.name, req.TransactionId)
	return &pb.BankTransactionResponse{
		Success: true,
		Message: fmt.Sprintf("Transaction aborted at %s", bs.name),
	}, nil
}

func main() {
	// Load TLS credentials.
	certFile := "../../certificate/server.crt"
	keyFile := "../../certificate/server.key"
	creds, err := credentials.NewServerTLSFromFile(certFile, keyFile)
	if err != nil {
		log.Fatalf("Failed to load TLS credentials: %v", err)
	}

	opts := []grpc.ServerOption{grpc.Creds(creds)}
	grpcServer := grpc.NewServer(opts...)

	// In this example the bank server is named "BankA".
	bankName := "BankA"
	bankServer := NewBankServer(bankName)
	pb.RegisterBankServiceServer(grpcServer, bankServer)

	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}
	log.Printf("%s Bank server listening on :50051", bankName)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}

package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	pb "github.com/kshitij-n24/DS-assignment2/P3/protofiles"
)

type BankServer struct {
	pb.UnimplementedBankServiceServer

	name                 string
	preparedTransactions map[string]float64 // transactionID -> amount
}

func NewBankServer(name string) *BankServer {
	return &BankServer{
		name:                 name,
		preparedTransactions: make(map[string]float64),
	}
}

func (bs *BankServer) PrepareTransaction(ctx context.Context, req *pb.BankTransactionRequest) (*pb.BankTransactionResponse, error) {
	// No extra mutex is needed here if this server is single-threaded per RPC.
	if _, exists := bs.preparedTransactions[req.TransactionId]; exists {
		log.Printf("[%s] Duplicate prepare for transaction %s", bs.name, req.TransactionId)
	}
	bs.preparedTransactions[req.TransactionId] = req.Amount
	log.Printf("[%s] Prepared transaction %s for amount %.2f", bs.name, req.TransactionId, req.Amount)
	return &pb.BankTransactionResponse{
		Success: true,
		Message: fmt.Sprintf("Transaction prepared at %s", bs.name),
	}, nil
}

func (bs *BankServer) CommitTransaction(ctx context.Context, req *pb.BankTransactionRequest) (*pb.BankTransactionResponse, error) {
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

func (bs *BankServer) AbortTransaction(ctx context.Context, req *pb.BankTransactionRequest) (*pb.BankTransactionResponse, error) {
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
	bankName := flag.String("name", "BankA", "Name of the bank")
	port := flag.String("port", "50051", "Port for the bank server to listen on")
	flag.Parse()

	// Set up file logging.
	logDir := "../logs"
	if _, err := os.Stat(logDir); os.IsNotExist(err) {
		if err := os.MkdirAll(logDir, 0755); err != nil {
			log.Fatalf("Failed to create log directory: %v", err)
		}
	}
	logFilePath := fmt.Sprintf("%s/%s_server.log", logDir, *bankName)
	// Clean up log file on startup.
	logFile, err := os.OpenFile(logFilePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	if err != nil {
		log.Fatalf("Failed to open log file: %v", err)
	}
	mw := io.MultiWriter(os.Stdout, logFile)
	log.SetOutput(mw)

	certFile := "../certificates/server.crt"
	keyFile := "../certificates/server.key"
	creds, err := credentials.NewServerTLSFromFile(certFile, keyFile)
	if err != nil {
		log.Fatalf("[STARTUP] Failed to load TLS credentials from %s and %s: %v", certFile, keyFile, err)
	}

	opts := []grpc.ServerOption{grpc.Creds(creds)}
	grpcServer := grpc.NewServer(opts...)

	bankServer := NewBankServer(*bankName)
	pb.RegisterBankServiceServer(grpcServer, bankServer)

	addr := ":" + *port
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("[STARTUP] Failed to listen on %s: %v", addr, err)
	}
	log.Printf("[STARTUP] %s Bank server is listening on %s", *bankName, addr)

	go func() {
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
		<-ch
		log.Printf("[SHUTDOWN] Shutting down %s Bank server...", *bankName)
		grpcServer.GracefulStop()
		os.Exit(0)
	}()

	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("[RUNTIME] Failed to serve: %v", err)
	}
}

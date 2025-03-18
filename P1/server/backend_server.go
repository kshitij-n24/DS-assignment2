package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	pb "github.com/kshitij-n24/DS-assignment2/P1/protofiles/backend"
	lbpb "github.com/kshitij-n24/DS-assignment2/P1/protofiles/lb"
	"google.golang.org/grpc"
)

// computationalServer implements the ComputationalService.
type computationalServer struct {
	pb.UnimplementedComputationalServiceServer
}

// ComputeTask performs a compute operation. Supported operations: "add", "multiply", and "loop".
// For "loop", parameter A is used as a multiplier for a high iteration count.
func (s *computationalServer) ComputeTask(ctx context.Context, req *pb.ComputeTaskRequest) (*pb.ComputeTaskResponse, error) {
	var result int64
	switch req.Operation {
	case "add":
		result = int64(req.A) + int64(req.B)
	case "multiply":
		result = int64(req.A) * int64(req.B)
	case "loop":
		n := int(req.A)
		if n < 1 {
			return nil, fmt.Errorf("parameter 'a' must be at least 1")
		}
		iterations := n * 10000000 // High iteration count
		var sum int64 = 0
		for i := 0; i < iterations; i++ {
			sum += int64(i % 100)
		}
		result = sum % 1000000
	default:
		return nil, fmt.Errorf("unsupported operation")
	}

	// Simulate computation delay.
	time.Sleep(500 * time.Millisecond)
	log.Printf("Computed %s on %d and %d: %d", req.Operation, req.A, req.B, result)
	return &pb.ComputeTaskResponse{Result: int32(result)}, nil
}

// registerWithLB repeatedly registers this backend with the LB server.
func registerWithLB(lbAddress, backendAddress string) {
	for {
		conn, err := grpc.Dial(lbAddress, grpc.WithInsecure())
		if err != nil {
			log.Printf("Failed to connect to LB server: %v. Retrying in 5 seconds...", err)
			time.Sleep(5 * time.Second)
			continue
		}
		client := lbpb.NewLoadBalancerClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		req := &lbpb.RegisterBackendRequest{ServerAddress: backendAddress}
		res, err := client.RegisterBackend(ctx, req)
		cancel()
		conn.Close()
		if err != nil || !res.Success {
			log.Printf("Failed to register with LB server: %v. Retrying in 5 seconds...", err)
			time.Sleep(5 * time.Second)
			continue
		}
		log.Printf("Successfully registered with LB at %s", lbAddress)
		break
	}
}

// reportLoadPeriodically reports a simulated load to the LB server every 5 seconds.
func reportLoadPeriodically(lbAddress, backendAddress string) {
	for {
		conn, err := grpc.Dial(lbAddress, grpc.WithInsecure())
		if err != nil {
			log.Printf("Failed to connect to LB server for load reporting: %v. Retrying in 5 seconds...", err)
			time.Sleep(5 * time.Second)
			continue
		}
		client := lbpb.NewLoadBalancerClient(conn)
		// Simulate a load value between 0 and 120.
		load := int32(rand.Intn(120))
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_, err = client.ReportLoad(ctx, &lbpb.ReportLoadRequest{
			ServerAddress: backendAddress,
			Load:          load,
		})
		cancel()
		conn.Close()
		if err != nil {
			log.Printf("Error reporting load: %v", err)
		} else {
			log.Printf("Reported load %d for backend %s", load, backendAddress)
		}
		time.Sleep(5 * time.Second)
	}
}

func main() {
	var backendPort int
	var lbAddress string
	flag.IntVar(&backendPort, "port", 50052, "Backend server port")
	flag.StringVar(&lbAddress, "lb", "localhost:50051", "Load Balancer address")
	flag.Parse()

	backendAddress := fmt.Sprintf("localhost:%d", backendPort)
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", backendPort))
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	pb.RegisterComputationalServiceServer(grpcServer, &computationalServer{})
	log.Printf("Backend server listening on %s", backendAddress)

	go registerWithLB(lbAddress, backendAddress)
	go reportLoadPeriodically(lbAddress, backendAddress)

	// Graceful shutdown on SIGINT/SIGTERM.
	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
		<-c
		log.Println("Backend server shutting down gracefully...")
		grpcServer.GracefulStop()
		os.Exit(0)
	}()

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}

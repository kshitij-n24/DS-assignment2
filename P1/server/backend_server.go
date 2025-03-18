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
	"syscall"
	"time"

	pb "github.com/<user>/DS-assignment2/P1/protofiles/backend"
	lbpb "github.com/<user>/DS-assignment2/P1/protofiles/lb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// computationalServer implements the ComputationalService.
type computationalServer struct {
	pb.UnimplementedComputationalServiceServer
}

// hanoi recursively computes the number of moves needed to solve Tower of Hanoi for n disks.
func hanoi(n int) int {
	if n == 1 {
		return 1
	}
	return 2*hanoi(n-1) + 1
}

// ComputeTask performs the compute operation. Supported operations are "add", "multiply", and "hanoi".
// For "hanoi", field A is used as the number of disks (must be between 1 and 25); field B is ignored.
func (s *computationalServer) ComputeTask(ctx context.Context, req *pb.ComputeTaskRequest) (*pb.ComputeTaskResponse, error) {
	var result int64
	switch req.Operation {
	case "add":
		result = int64(req.A) + int64(req.B)
	case "multiply":
		result = int64(req.A) * int64(req.B)
	case "hanoi":
		n := int(req.A)
		if n < 1 {
			return nil, status.Error(codes.InvalidArgument, "number of disks must be at least 1")
		}
		if n > 25 {
			return nil, status.Error(codes.InvalidArgument, "too many disks, maximum allowed is 25")
		}
		result = int64(hanoi(n))
	default:
		return nil, status.Error(codes.InvalidArgument, "unsupported operation")
	}

	// Simulate a computational delay.
	time.Sleep(500 * time.Millisecond)
	log.Printf("Computed %s on %d and %d: %d", req.Operation, req.A, req.B, result)
	return &pb.ComputeTaskResponse{Result: int32(result)}, nil
}

// registerWithLB repeatedly registers this backend server with the LB server.
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

// reportLoadPeriodically simulates load reporting to the LB server.
func reportLoadPeriodically(lbAddress, backendAddress string) {
	for {
		conn, err := grpc.Dial(lbAddress, grpc.WithInsecure())
		if err != nil {
			log.Printf("Failed to connect to LB server for load reporting: %v. Retrying in 5 seconds...", err)
			time.Sleep(5 * time.Second)
			continue
		}
		client := lbpb.NewLoadBalancerClient(conn)
		// Simulate a load value (some may exceed [0,100] to test clamping).
		load := int32(rand.Intn(120))
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_, err = client.ReportLoad(ctx, &lbpb.ReportLoadRequest{ServerAddress: backendAddress, Load: load})
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

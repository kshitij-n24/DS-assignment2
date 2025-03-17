package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
	"time"

	pb "github.com/kshitij-n24/DS-assignment2/P1/protofiles/backend"
	lbpb "github.com/kshitij-n24/DS-assignment2/P1/protofiles/lb"
	"google.golang.org/grpc"
)

// computationalServer implements the ComputationalService.
type computationalServer struct {
	pb.UnimplementedComputationalServiceServer
}

// ComputeTask performs a simple arithmetic computation.
func (s *computationalServer) ComputeTask(ctx context.Context, req *pb.ComputeTaskRequest) (*pb.ComputeTaskResponse, error) {
	var result int32
	switch req.Operation {
	case "add":
		result = req.A + req.B
	case "multiply":
		result = req.A * req.B
	default:
		result = 0
	}

	// Simulate a computational delay.
	time.Sleep(500 * time.Millisecond)
	log.Printf("Computed %s on %d and %d: %d", req.Operation, req.A, req.B, result)
	return &pb.ComputeTaskResponse{Result: result}, nil
}

// registerWithLB repeatedly tries to register this backend server with the LB server.
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
		// Dial the LB server each time to ensure a fresh connection.
		conn, err := grpc.Dial(lbAddress, grpc.WithInsecure())
		if err != nil {
			log.Printf("Failed to connect to LB server for load reporting: %v. Retrying in 5 seconds...", err)
			time.Sleep(5 * time.Second)
			continue
		}

		client := lbpb.NewLoadBalancerClient(conn)

		// Simulate a load value between 0 and 100.
		load := int32(rand.Intn(100))
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
	// Flags for backend server port and LB server address.
	var backendPort int
	var lbAddress string
	flag.IntVar(&backendPort, "port", 50052, "Backend server port")
	flag.StringVar(&lbAddress, "lb", "localhost:50051", "Load Balancer address")
	flag.Parse()

	backendAddress := fmt.Sprintf("localhost:%d", backendPort)

	// Start gRPC server for the ComputationalService.
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", backendPort))
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	pb.RegisterComputationalServiceServer(grpcServer, &computationalServer{})
	log.Printf("Backend server listening on %s", backendAddress)

	// Register with the LB server and start load reporting in background.
	go registerWithLB(lbAddress, backendAddress)
	go reportLoadPeriodically(lbAddress, backendAddress)

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}

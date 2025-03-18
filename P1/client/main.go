package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	backendpb "github.com/<user>/DS-assignment2/P1/protofiles/backend"
	lbpb "github.com/<user>/DS-assignment2/P1/protofiles/lb"

	"google.golang.org/grpc"
)

const (
	retryAttempts = 3
	retryDelay    = 2 * time.Second
)

func dialWithRetries(address string, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	var conn *grpc.ClientConn
	var err error
	for i := 0; i < retryAttempts; i++ {
		conn, err = grpc.Dial(address, opts...)
		if err == nil {
			return conn, nil
		}
		log.Printf("Attempt %d: Failed to dial %s: %v", i+1, address, err)
		time.Sleep(retryDelay)
	}
	return nil, err
}

func main() {
	var lbAddress string
	var policy string
	var op string
	var a, b int
	flag.StringVar(&lbAddress, "lb", "localhost:50051", "Load Balancer address")
	flag.StringVar(&policy, "policy", "PICK_FIRST", "Balancing policy: PICK_FIRST, ROUND_ROBIN, LEAST_LOAD")
	flag.StringVar(&op, "operation", "add", "Operation for compute task: add, multiply, hanoi")
	flag.IntVar(&a, "a", 10, "First operand (or number of disks for hanoi)")
	flag.IntVar(&b, "b", 20, "Second operand (ignored for hanoi)")
	flag.Parse()

	// Map policy string to proto enum.
	var balPolicy lbpb.BalancingPolicy
	switch policy {
	case "PICK_FIRST":
		balPolicy = lbpb.BalancingPolicy_PICK_FIRST
	case "ROUND_ROBIN":
		balPolicy = lbpb.BalancingPolicy_ROUND_ROBIN
	case "LEAST_LOAD":
		balPolicy = lbpb.BalancingPolicy_LEAST_LOAD
	default:
		log.Printf("Unknown policy %s, defaulting to PICK_FIRST", policy)
		balPolicy = lbpb.BalancingPolicy_PICK_FIRST
	}

	// Connect to the LB server with retries.
	lbConn, err := dialWithRetries(lbAddress, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Failed to connect to LB server: %v", err)
	}
	defer lbConn.Close()

	lbClient := lbpb.NewLoadBalancerClient(lbConn)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Query the LB server for the best backend.
	getReq := &lbpb.GetBestServerRequest{Policy: balPolicy}
	getRes, err := lbClient.GetBestServer(ctx, getReq)
	if err != nil {
		log.Fatalf("Failed to get best server from LB: %v", err)
	}

	if getRes.ServerAddress == "" {
		log.Fatalf("No available backend servers")
	}

	log.Printf("Using backend server: %s", getRes.ServerAddress)

	// Connect to the selected backend server with retries.
	backendConn, err := dialWithRetries(getRes.ServerAddress, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Failed to connect to backend server: %v", err)
	}
	defer backendConn.Close()

	backendClient := backendpb.NewComputationalServiceClient(backendConn)
	compCtx, compCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer compCancel()

	req := &backendpb.ComputeTaskRequest{
		A:         int32(a),
		B:         int32(b),
		Operation: op,
	}
	start := time.Now()
	res, err := backendClient.ComputeTask(compCtx, req)
	if err != nil {
		log.Fatalf("ComputeTask failed: %v", err)
	}
	latency := time.Since(start)
	fmt.Printf("Compute result: %d (latency: %v)\n", res.Result, latency)
}

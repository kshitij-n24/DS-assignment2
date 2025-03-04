package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	backendpb "github.com/kshitij-n24/DS-assignment2/P1/protofiles/backend"
	lbpb "github.com/kshitij-n24/DS-assignment2/P1/protofiles/lb"

	"google.golang.org/grpc"
)

func main() {
	var lbAddress string
	var policy string
	flag.StringVar(&lbAddress, "lb", "localhost:50051", "Load Balancer address")
	flag.StringVar(&policy, "policy", "PICK_FIRST", "Balancing policy: PICK_FIRST, ROUND_ROBIN, LEAST_LOAD")
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

	// Connect to the LB server.
	lbConn, err := grpc.Dial(lbAddress, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Failed to connect to LB server: %v", err)
	}
	defer lbConn.Close()

	lbClient := lbpb.NewLoadBalancerClient(lbConn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
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

	// Connect to the selected backend server.
	backendConn, err := grpc.Dial(getRes.ServerAddress, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Failed to connect to backend server: %v", err)
	}
	defer backendConn.Close()

	backendClient := backendpb.NewComputationalServiceClient(backendConn)
	// Send a sample compute task (e.g., adding 10 + 20).
	compCtx, compCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer compCancel()

	req := &backendpb.ComputeTaskRequest{
		A:         10,
		B:         20,
		Operation: "add",
	}
	res, err := backendClient.ComputeTask(compCtx, req)
	if err != nil {
		log.Fatalf("ComputeTask failed: %v", err)
	}

	fmt.Printf("Compute result: %d\n", res.Result)
}

package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	pb "github.com/kshitij-n24/DS-assignment2/P1/protofiles/backend"
	lbpb "github.com/kshitij-n24/DS-assignment2/P1/protofiles/lb"
	"google.golang.org/grpc"
)

const (
	retryAttempts = 3
	retryDelay    = 2 * time.Second
)

// dialWithRetries attempts to establish a connection to the given address.
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

// performComputeTask performs the full sequence of querying the LB and then sending a compute task.
func performComputeTask(lbAddress string, balPolicy lbpb.BalancingPolicy, op string, a, b int) (time.Duration, int32, error) {
	// Connect to the LB server.
	lbConn, err := dialWithRetries(lbAddress, grpc.WithInsecure())
	if err != nil {
		return 0, 0, fmt.Errorf("failed to connect to LB server: %v", err)
	}
	defer lbConn.Close()

	lbClient := lbpb.NewLoadBalancerClient(lbConn)
	var timeout time.Duration
	// CHANGED: Increase timeout if using PICK_FIRST so that the LB can wait for the backend to become free.
	if balPolicy == lbpb.BalancingPolicy_PICK_FIRST {
		timeout = 10 * time.Second
	} else {
		timeout = 2 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	getReq := &lbpb.GetBestServerRequest{Policy: balPolicy}
	getRes, err := lbClient.GetBestServer(ctx, getReq)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to get best server from LB: %v", err)
	}

	if getRes.ServerAddress == "" {
		return 0, 0, fmt.Errorf("no available backend servers")
	}

	log.Printf("Using backend server: %s", getRes.ServerAddress)

	// Connect to the selected backend server.
	backendConn, err := dialWithRetries(getRes.ServerAddress, grpc.WithInsecure())
	if err != nil {
		return 0, 0, fmt.Errorf("failed to connect to backend server: %v", err)
	}
	defer backendConn.Close()

	backendClient := pb.NewComputationalServiceClient(backendConn)
	compCtx, compCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer compCancel()

	req := &pb.ComputeTaskRequest{
		A:         int32(a),
		B:         int32(b),
		Operation: op,
	}
	start := time.Now()
	res, err := backendClient.ComputeTask(compCtx, req)
	latency := time.Since(start)
	if err != nil {
		return latency, 0, fmt.Errorf("ComputeTask failed: %v", err)
	}
	return latency, res.Result, nil
}

func main() {
	// Common flags.
	var lbAddress string
	var policy string
	var op string
	var a, b int
	flag.StringVar(&lbAddress, "lb", "localhost:50051", "Load Balancer address")
	flag.StringVar(&policy, "policy", "PICK_FIRST", "Balancing policy: PICK_FIRST, ROUND_ROBIN, LEAST_LOAD")
	flag.StringVar(&op, "operation", "add", "Operation for compute task: add, multiply, hanoi")
	flag.IntVar(&a, "a", 10, "First operand (or number of disks for hanoi)")
	flag.IntVar(&b, "b", 20, "Second operand (ignored for hanoi)")

	// Scale test flags.
	var scaleTest bool
	var numRequests int
	var concurrency int
	flag.BoolVar(&scaleTest, "scale", false, "Enable scale test mode (multiple concurrent requests)")
	flag.IntVar(&numRequests, "num_requests", 100, "Total number of requests in scale test")
	flag.IntVar(&concurrency, "concurrency", 10, "Number of concurrent goroutines in scale test")
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

	// Single request mode.
	if !scaleTest {
		latency, result, err := performComputeTask(lbAddress, balPolicy, op, a, b)
		if err != nil {
			log.Fatalf("Error: %v", err)
		}
		fmt.Printf("Compute result: %d (latency: %v)\n", result, latency)
		os.Exit(0)
	}

	// Scale test mode.
	log.Printf("Starting scale test with %d total requests at concurrency %d", numRequests, concurrency)
	var wg sync.WaitGroup
	latencyCh := make(chan time.Duration, numRequests)
	errCh := make(chan error, numRequests)

	startTotal := time.Now()
	sem := make(chan struct{}, concurrency)
	for i := 0; i < numRequests; i++ {
		wg.Add(1)
		sem <- struct{}{}
		go func(reqID int) {
			defer wg.Done()
			lat, _, err := performComputeTask(lbAddress, balPolicy, op, a, b)
			if err != nil {
				errCh <- fmt.Errorf("request %d: %v", reqID, err)
			} else {
				latencyCh <- lat
			}
			<-sem
		}(i)
	}
	wg.Wait()
	totalDuration := time.Since(startTotal)
	close(latencyCh)
	close(errCh)

	// Compute metrics.
	var sumLatency time.Duration
	count := 0
	for lat := range latencyCh {
		sumLatency += lat
		count++
	}
	avgLatency := time.Duration(0)
	if count > 0 {
		avgLatency = sumLatency / time.Duration(count)
	}
	throughput := float64(count) / totalDuration.Seconds()

	// Log errors if any.
	for err := range errCh {
		log.Printf("Error encountered: %v", err)
	}

	// Final aggregated metric output.
	log.Printf("[METRICS]: Scale Test Completed - Total Requests: %d, Successful: %d, Total Time: %v, Average Latency: %v, Throughput: %.2f req/sec",
		numRequests, count, totalDuration, avgLatency, throughput)
}

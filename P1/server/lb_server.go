package main

import (
	"context"
	"log"
	"net"
	"sync"

	pb "github.com/kshitij-n24/DS-assignment2/P1/protofiles"

	"google.golang.org/grpc"
)

// BackendInfo holds the address and current load of a backend server.
type BackendInfo struct {
	Address string
	Load    int32
}

// LoadBalancerServer implements the LoadBalancer gRPC service.
type LoadBalancerServer struct {
	pb.UnimplementedLoadBalancerServer
	mu              sync.Mutex
	backends        []BackendInfo
	roundRobinIndex int
}

func NewLoadBalancerServer() *LoadBalancerServer {
	return &LoadBalancerServer{
		backends: make([]BackendInfo, 0),
	}
}

// RegisterBackend allows backend servers to register themselves.
func (s *LoadBalancerServer) RegisterBackend(ctx context.Context, req *pb.RegisterBackendRequest) (*pb.RegisterBackendResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if already registered.
	for _, b := range s.backends {
		if b.Address == req.ServerAddress {
			log.Printf("Backend %s already registered", req.ServerAddress)
			return &pb.RegisterBackendResponse{Success: true}, nil
		}
	}

	s.backends = append(s.backends, BackendInfo{Address: req.ServerAddress, Load: 0})
	log.Printf("Registered backend: %s", req.ServerAddress)
	return &pb.RegisterBackendResponse{Success: true}, nil
}

// ReportLoad updates the load of a registered backend server.
func (s *LoadBalancerServer) ReportLoad(ctx context.Context, req *pb.ReportLoadRequest) (*pb.ReportLoadResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for i, b := range s.backends {
		if b.Address == req.ServerAddress {
			s.backends[i].Load = req.Load
			log.Printf("Updated load for %s: %d", req.ServerAddress, req.Load)
			return &pb.ReportLoadResponse{Success: true}, nil
		}
	}

	log.Printf("Backend %s not found for load reporting", req.ServerAddress)
	return &pb.ReportLoadResponse{Success: false}, nil
}

// GetBestServer selects a backend server based on the requested policy.
func (s *LoadBalancerServer) GetBestServer(ctx context.Context, req *pb.GetBestServerRequest) (*pb.GetBestServerResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.backends) == 0 {
		return &pb.GetBestServerResponse{ServerAddress: ""}, nil
	}

	var selected BackendInfo

	switch req.Policy {
	case pb.BalancingPolicy_PICK_FIRST:
		selected = s.backends[0]
	case pb.BalancingPolicy_ROUND_ROBIN:
		selected = s.backends[s.roundRobinIndex%len(s.backends)]
		s.roundRobinIndex++
	case pb.BalancingPolicy_LEAST_LOAD:
		selected = s.backends[0]
		for _, b := range s.backends {
			if b.Load < selected.Load {
				selected = b
			}
		}
	default:
		// Default to PICK_FIRST if unknown policy.
		selected = s.backends[0]
	}

	log.Printf("Selected backend %s for policy %s", selected.Address, req.Policy.String())
	return &pb.GetBestServerResponse{ServerAddress: selected.Address}, nil
}

func main() {
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	lbServer := NewLoadBalancerServer()
	pb.RegisterLoadBalancerServer(grpcServer, lbServer)
	log.Println("Load Balancer server listening on :50051")
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}

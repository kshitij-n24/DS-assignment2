package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	pb "github.com/kshitij-n24/DS-assignment2/P1/protofiles/lb"
	"go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
)

// BackendInfo holds the address and current load of a backend server.
type BackendInfo struct {
	Address string
	Load    int32
}

// LoadBalancerServer implements the LoadBalancer gRPC service with dynamic server discovery.
type LoadBalancerServer struct {
	pb.UnimplementedLoadBalancerServer
	mu              sync.Mutex
	backends        map[string]BackendInfo    // key: backend address
	leases          map[string]clientv3.LeaseID // key: backend address -> lease id mapping
	roundRobinIndex int
	etcdClient      *clientv3.Client
}

// NewLoadBalancerServer creates a new LB server instance.
func NewLoadBalancerServer(etcdClient *clientv3.Client) *LoadBalancerServer {
	return &LoadBalancerServer{
		backends:   make(map[string]BackendInfo),
		leases:     make(map[string]clientv3.LeaseID),
		etcdClient: etcdClient,
	}
}

// watchBackends continuously watches etcd for changes in backend registration.
func (s *LoadBalancerServer) watchBackends() {
	for {
		ctx := context.Background()
		resp, err := s.etcdClient.Get(ctx, "/backends/", clientv3.WithPrefix())
		if err != nil {
			log.Printf("Error getting backends from etcd: %v", err)
			time.Sleep(5 * time.Second)
			continue
		}
		s.mu.Lock()
		for _, kv := range resp.Kvs {
			load, err := strconv.Atoi(string(kv.Value))
			if err != nil {
				log.Printf("Error converting load for key %s: %v", string(kv.Key), err)
				continue
			}
			address := string(kv.Key)[len("/backends/"):]
			s.backends[address] = BackendInfo{Address: address, Load: int32(load)}
		}
		s.mu.Unlock()

		watchChan := s.etcdClient.Watch(ctx, "/backends/", clientv3.WithPrefix())
		for watchResp := range watchChan {
			if watchResp.Err() != nil {
				log.Printf("Watch error: %v", watchResp.Err())
				break
			}
			for _, ev := range watchResp.Events {
				s.mu.Lock()
				address := string(ev.Kv.Key)[len("/backends/"):]
				switch ev.Type {
				case clientv3.EventTypePut:
					load, err := strconv.Atoi(string(ev.Kv.Value))
					if err != nil {
						log.Printf("Error converting load for backend %s: %v", address, err)
						s.mu.Unlock()
						continue
					}
					s.backends[address] = BackendInfo{Address: address, Load: int32(load)}
					log.Printf("Watcher: Updated backend %s with load %d", address, load)
				case clientv3.EventTypeDelete:
					delete(s.backends, address)
					delete(s.leases, address)
					log.Printf("Watcher: Removed backend %s", address)
				}
				s.mu.Unlock()
			}
		}
		log.Printf("Watch channel closed, restarting watch...")
		time.Sleep(5 * time.Second)
	}
}

// RegisterBackend registers a backend server by writing its info to etcd with a TTL lease.
func (s *LoadBalancerServer) RegisterBackend(ctx context.Context, req *pb.RegisterBackendRequest) (*pb.RegisterBackendResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := "/backends/" + req.ServerAddress

	leaseResp, err := s.etcdClient.Grant(ctx, 10)
	if err != nil {
		log.Printf("Failed to create lease for backend %s: %v", req.ServerAddress, err)
		return &pb.RegisterBackendResponse{Success: false}, err
	}

	_, err = s.etcdClient.Put(ctx, key, "0", clientv3.WithLease(leaseResp.ID))
	if err != nil {
		log.Printf("Failed to register backend %s in etcd: %v", req.ServerAddress, err)
		return &pb.RegisterBackendResponse{Success: false}, err
	}
	s.leases[req.ServerAddress] = leaseResp.ID
	log.Printf("Registered backend: %s", req.ServerAddress)
	return &pb.RegisterBackendResponse{Success: true}, nil
}

// ReportLoad updates the load for a backend in etcd.
func (s *LoadBalancerServer) ReportLoad(ctx context.Context, req *pb.ReportLoadRequest) (*pb.ReportLoadResponse, error) {
	s.mu.Lock()
	leaseID, exists := s.leases[req.ServerAddress]
	s.mu.Unlock()
	if !exists {
		regResp, err := s.RegisterBackend(ctx, &pb.RegisterBackendRequest{ServerAddress: req.ServerAddress})
		if err != nil || !regResp.Success {
			log.Printf("Auto-registration failed for backend %s", req.ServerAddress)
			return &pb.ReportLoadResponse{Success: false}, fmt.Errorf("backend not registered")
		}
		s.mu.Lock()
		leaseID = s.leases[req.ServerAddress]
		s.mu.Unlock()
	}

	load := req.Load
	if load < 0 {
		load = 0
	} else if load > 100 {
		load = 100
	}

	key := "/backends/" + req.ServerAddress
	ctxPut, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	_, err := s.etcdClient.Put(ctxPut, key, fmt.Sprintf("%d", load), clientv3.WithLease(leaseID))
	if err != nil {
		log.Printf("Failed to report load for backend %s: %v", req.ServerAddress, err)
		return &pb.ReportLoadResponse{Success: false}, err
	}
	ctxKA, cancelKA := context.WithTimeout(ctx, 2*time.Second)
	defer cancelKA()
	_, err = s.etcdClient.KeepAliveOnce(ctxKA, leaseID)
	if err != nil {
		log.Printf("Failed to renew lease for backend %s: %v", req.ServerAddress, err)
		return &pb.ReportLoadResponse{Success: false}, err
	}
	log.Printf("Reported load %d for backend %s", load, req.ServerAddress)
	return &pb.ReportLoadResponse{Success: true}, nil
}

// GetBestServer selects a backend based on the requested balancing policy.
func (s *LoadBalancerServer) GetBestServer(ctx context.Context, req *pb.GetBestServerRequest) (*pb.GetBestServerResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	backendsSlice := make([]BackendInfo, 0, len(s.backends))
	for _, b := range s.backends {
		backendsSlice = append(backendsSlice, b)
	}
	if len(backendsSlice) == 0 {
		log.Printf("No backends available")
		return &pb.GetBestServerResponse{ServerAddress: ""}, nil
	}

	var selected BackendInfo
	switch req.Policy {
	case pb.BalancingPolicy_PICK_FIRST:
		selected = backendsSlice[0]
	case pb.BalancingPolicy_ROUND_ROBIN:
		selected = backendsSlice[s.roundRobinIndex%len(backendsSlice)]
		s.roundRobinIndex++
		if s.roundRobinIndex > 1000000 {
			s.roundRobinIndex = 0
		}
	case pb.BalancingPolicy_LEAST_LOAD:
		selected = backendsSlice[0]
		for _, b := range backendsSlice {
			if b.Load < selected.Load {
				selected = b
			}
		}
	default:
		selected = backendsSlice[0]
	}
	log.Printf("Selected backend %s for policy %s", selected.Address, req.Policy.String())
	return &pb.GetBestServerResponse{ServerAddress: selected.Address}, nil
}

func main() {
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	etcdClient, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		log.Fatalf("Failed to create etcd client: %v", err)
	}
	defer etcdClient.Close()

	grpcServer := grpc.NewServer()
	lbServer := NewLoadBalancerServer(etcdClient)
	go lbServer.watchBackends()

	pb.RegisterLoadBalancerServer(grpcServer, lbServer)
	log.Println("Load Balancer server listening on :50051")

	// Graceful shutdown for LB server.
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("Failed to serve: %v", err)
		}
	}()
	<-stop
	log.Println("LB Server shutting down gracefully...")
	grpcServer.GracefulStop()
}

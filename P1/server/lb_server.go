package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	lbpb "github.com/kshitij-n24/DS-assignment2/P1/protofiles/lb"
	"go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
)

// BackendInfo holds the address and current load of a backend server.
type BackendInfo struct {
	Address string
	Load    int32
}

// LoadBalancerServer implements the LoadBalancer gRPC service.
type LoadBalancerServer struct {
	lbpb.UnimplementedLoadBalancerServer
	mu              sync.Mutex
	backends        map[string]BackendInfo    // key: backend address -> info
	leases          map[string]clientv3.LeaseID // key: backend address -> lease id mapping
	orderedBackends []string                  // maintains registration order
	roundRobinIndex int
	etcdClient      *clientv3.Client
}

// NewLoadBalancerServer creates a new LB server instance.
func NewLoadBalancerServer(etcdClient *clientv3.Client) *LoadBalancerServer {
	return &LoadBalancerServer{
		backends:        make(map[string]BackendInfo),
		leases:          make(map[string]clientv3.LeaseID),
		orderedBackends: []string{},
		etcdClient:      etcdClient,
	}
}

func (s *LoadBalancerServer) removeFromOrderedBackends(address string) {
	newOrdered := []string{}
	for _, a := range s.orderedBackends {
		if a != address {
			newOrdered = append(newOrdered, a)
		}
	}
	s.orderedBackends = newOrdered
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
			s.backends[address] = BackendInfo{
				Address: address,
				Load:    int32(load),
			}
			// Ensure registration order is maintained.
			found := false
			for _, a := range s.orderedBackends {
				if a == address {
					found = true
					break
				}
			}
			if !found {
				s.orderedBackends = append(s.orderedBackends, address)
			}
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
					s.backends[address] = BackendInfo{
						Address: address,
						Load:    int32(load),
					}
					log.Printf("[Watcher] Updated backend %s with load %d", address, load)
					found := false
					for _, a := range s.orderedBackends {
						if a == address {
							found = true
							break
						}
					}
					if !found {
						s.orderedBackends = append(s.orderedBackends, address)
					}
				case clientv3.EventTypeDelete:
					delete(s.backends, address)
					delete(s.leases, address)
					s.removeFromOrderedBackends(address)
					log.Printf("[Watcher] Removed backend %s", address)
				}
				s.mu.Unlock()
			}
		}
		log.Printf("Watch channel closed, restarting watch...")
		time.Sleep(5 * time.Second)
	}
}

// RegisterBackend registers a backend server by writing its information to etcd with a lease.
func (s *LoadBalancerServer) RegisterBackend(ctx context.Context, req *lbpb.RegisterBackendRequest) (*lbpb.RegisterBackendResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := "/backends/" + req.ServerAddress

	// Create a new lease with a TTL of 10 seconds.
	leaseResp, err := s.etcdClient.Grant(ctx, 10)
	if err != nil {
		log.Printf("Failed to create lease for backend %s: %v", req.ServerAddress, err)
		return &lbpb.RegisterBackendResponse{Success: false}, err
	}

	_, err = s.etcdClient.Put(ctx, key, "0", clientv3.WithLease(leaseResp.ID))
	if err != nil {
		log.Printf("Failed to register backend %s in etcd: %v", req.ServerAddress, err)
		return &lbpb.RegisterBackendResponse{Success: false}, err
	}
	s.leases[req.ServerAddress] = leaseResp.ID

	found := false
	for _, a := range s.orderedBackends {
		if a == req.ServerAddress {
			found = true
			break
		}
	}
	if !found {
		s.orderedBackends = append(s.orderedBackends, req.ServerAddress)
	}

	log.Printf("Registered backend: %s", req.ServerAddress)
	return &lbpb.RegisterBackendResponse{Success: true}, nil
}

// ReportLoad updates the load of a registered backend in etcd.
func (s *LoadBalancerServer) ReportLoad(ctx context.Context, req *lbpb.ReportLoadRequest) (*lbpb.ReportLoadResponse, error) {
	s.mu.Lock()
	leaseID, exists := s.leases[req.ServerAddress]
	s.mu.Unlock()

	if !exists {
		regResp, err := s.RegisterBackend(ctx, &lbpb.RegisterBackendRequest{ServerAddress: req.ServerAddress})
		if err != nil || !regResp.Success {
			log.Printf("Auto-registration failed for backend %s", req.ServerAddress)
			return &lbpb.ReportLoadResponse{Success: false}, fmt.Errorf("backend not registered")
		}
		s.mu.Lock()
		leaseID = s.leases[req.ServerAddress]
		s.mu.Unlock()
	}

	// Validate and clamp the load value to [0, 100].
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
		return &lbpb.ReportLoadResponse{Success: false}, err
	}
	ctxKA, cancelKA := context.WithTimeout(ctx, 2*time.Second)
	defer cancelKA()
	_, err = s.etcdClient.KeepAliveOnce(ctxKA, leaseID)
	if err != nil {
		log.Printf("Failed to renew lease for backend %s: %v", req.ServerAddress, err)
		return &lbpb.ReportLoadResponse{Success: false}, err
	}
	log.Printf("Reported load %d for backend %s", load, req.ServerAddress)
	return &lbpb.ReportLoadResponse{Success: true}, nil
}

// GetBestServer selects a backend server based on the requested policy.
func (s *LoadBalancerServer) GetBestServer(ctx context.Context, req *lbpb.GetBestServerRequest) (*lbpb.GetBestServerResponse, error) {
	switch req.Policy {
	case lbpb.BalancingPolicy_PICK_FIRST:
		const threshold = 50 // free if load <= 50.
		for {
			var firstAddr string
			var load int32
			var exists bool

			s.mu.Lock()
			if len(s.orderedBackends) == 0 {
				s.mu.Unlock()
				log.Printf("No backends available")
				return &lbpb.GetBestServerResponse{ServerAddress: ""}, nil
			}
			firstAddr = s.orderedBackends[0]
			b, exists := s.backends[firstAddr]
			if exists {
				load = b.Load
			}
			s.mu.Unlock()

			if !exists {
				s.mu.Lock()
				s.removeFromOrderedBackends(firstAddr)
				s.mu.Unlock()
				continue
			}
			if load <= threshold {
				log.Printf("Selected backend %s for policy PICK_FIRST", firstAddr)
				return &lbpb.GetBestServerResponse{ServerAddress: firstAddr}, nil
			}
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(1 * time.Second):
			}
		}
	case lbpb.BalancingPolicy_ROUND_ROBIN:
		s.mu.Lock()
		defer s.mu.Unlock()
		backendsSlice := make([]BackendInfo, 0, len(s.backends))
		for _, b := range s.backends {
			backendsSlice = append(backendsSlice, b)
		}
		if len(backendsSlice) == 0 {
			log.Printf("No backends available")
			return &lbpb.GetBestServerResponse{ServerAddress: ""}, nil
		}
		selected := backendsSlice[s.roundRobinIndex%len(backendsSlice)]
		s.roundRobinIndex++
		if s.roundRobinIndex > 1000000 {
			s.roundRobinIndex = 0
		}
		log.Printf("Selected backend %s for policy ROUND_ROBIN", selected.Address)
		return &lbpb.GetBestServerResponse{ServerAddress: selected.Address}, nil
	case lbpb.BalancingPolicy_LEAST_LOAD:
		s.mu.Lock()
		defer s.mu.Unlock()
		backendsSlice := make([]BackendInfo, 0, len(s.backends))
		for _, b := range s.backends {
			backendsSlice = append(backendsSlice, b)
		}
		if len(backendsSlice) == 0 {
			log.Printf("No backends available")
			return &lbpb.GetBestServerResponse{ServerAddress: ""}, nil
		}
		selected := backendsSlice[0]
		for _, b := range backendsSlice {
			if b.Load < selected.Load {
				selected = b
			}
		}
		log.Printf("Selected backend %s for policy LEAST_LOAD", selected.Address)
		return &lbpb.GetBestServerResponse{ServerAddress: selected.Address}, nil
	default:
		s.mu.Lock()
		defer s.mu.Unlock()
		backendsSlice := make([]BackendInfo, 0, len(s.backends))
		for _, b := range s.backends {
			backendsSlice = append(backendsSlice, b)
		}
		if len(backendsSlice) == 0 {
			log.Printf("No backends available")
			return &lbpb.GetBestServerResponse{ServerAddress: ""}, nil
		}
		selected := backendsSlice[0]
		log.Printf("Selected backend %s for default policy", selected.Address)
		return &lbpb.GetBestServerResponse{ServerAddress: selected.Address}, nil
	}
}

func main() {
	// Flags for LB server.
	var lbPort int
	var etcdEndpointsStr string
	flag.IntVar(&lbPort, "port", 50051, "Load Balancer server port")
	flag.StringVar(&etcdEndpointsStr, "etcd_endpoints", "localhost:2379", "Comma-separated list of etcd endpoints")
	flag.Parse()

	// Parse etcd endpoints.
	etcdEndpoints := strings.Split(etcdEndpointsStr, ",")
	etcdConfig := clientv3.Config{
		Endpoints:   etcdEndpoints,
		DialTimeout: 5 * time.Second,
	}
	etcdClient, err := clientv3.New(etcdConfig)
	if err != nil {
		log.Fatalf("Failed to connect to etcd: %v", err)
	}
	defer etcdClient.Close()

	lbServer := NewLoadBalancerServer(etcdClient)
	go lbServer.watchBackends()

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", lbPort))
	if err != nil {
		log.Fatalf("Failed to listen on port %d: %v", lbPort, err)
	}
	grpcServer := grpc.NewServer()
	lbpb.RegisterLoadBalancerServer(grpcServer, lbServer)
	log.Printf("Load Balancer server listening on port %d", lbPort)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}

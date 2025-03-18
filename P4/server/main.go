package main

import (
	"context"
	"flag"
	"log"
	"math/rand"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/<user>/DS-assignment2/P4/protofiles"
)

// Node represents a general in the Byzantine simulation.
type Node struct {
	id              int32
	isCommander     bool
	isTraitor       bool
	currentDecision string // current decision (order)
	t               int    // maximum traitors; rounds = t+1
	peers           []string
	totalNodes      int // total number of nodes (including self)

	mu           sync.Mutex
	receivedMsgs map[int]map[int32]*pb.OrderMessage // round -> senderId -> OrderMessage
}

// newNode initializes a new Node.
func newNode(id int32, isCommander, isTraitor bool, initOrder string, t int, peers []string) *Node {
	// Remove duplicate and empty addresses from peers list.
	uniquePeers := []string{}
	seen := make(map[string]bool)
	for _, p := range peers {
		p = strings.TrimSpace(p)
		if p != "" && !seen[p] {
			seen[p] = true
			uniquePeers = append(uniquePeers, p)
		}
	}
	return &Node{
		id:              id,
		isCommander:     isCommander,
		isTraitor:       isTraitor,
		currentDecision: initOrder,
		t:               t,
		peers:           uniquePeers,
		totalNodes:      len(uniquePeers) + 1,
		receivedMsgs:    make(map[int]map[int32]*pb.OrderMessage),
	}
}

// waitForMessages waits until at least expected messages are received in the given round,
// or until the timeout expires. Returns an error if timeout reached.
func (node *Node) waitForMessages(round int, expected int, timeout time.Duration) error {
	start := time.Now()
	for {
		node.mu.Lock()
		roundMsgs := node.receivedMsgs[round]
		count := 0
		if roundMsgs != nil {
			count = len(roundMsgs)
		}
		node.mu.Unlock()

		if count >= expected {
			return nil
		}
		if time.Since(start) > timeout {
			log.Printf("Node %d: timeout waiting for messages in round %d; expected %d, got %d", node.id, round, expected, count)
			return nil // proceed with what we have
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// sendMessage dials a peer and sends the order message for the given round.
// It uses a retry (once) if the connection fails.
func (node *Node) sendMessage(peerAddr string, round int, order string) {
	// Establish connection (using insecure connection for demo purposes).
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, peerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		log.Printf("Node %d: failed to connect to %s: %v. Retrying...", node.id, peerAddr, err)
		// Retry once after a short delay.
		time.Sleep(500 * time.Millisecond)
		ctxRetry, cancelRetry := context.WithTimeout(context.Background(), 2*time.Second)
		conn, err = grpc.DialContext(ctxRetry, peerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
		cancelRetry()
		if err != nil {
			log.Printf("Node %d: retry failed to connect to %s: %v", node.id, peerAddr, err)
			return
		}
	}
	defer conn.Close()
	client := pb.NewByzantineServiceClient(conn)

	ctxMsg, cancelMsg := context.WithTimeout(context.Background(), time.Second)
	defer cancelMsg()

	msg := &pb.OrderMessage{
		SenderId: node.id,
		Round:    int32(round),
		Order:    order,
	}
	_, err = client.PropagateOrder(ctxMsg, msg)
	if err != nil {
		log.Printf("Node %d: error sending message to %s: %v", node.id, peerAddr, err)
	}
}

// majorityVote returns "Attack" if the count of "Attack" is greater than "Retreat"; otherwise "Retreat".
func majorityVote(orders []string) string {
	countAttack, countRetreat := 0, 0
	for _, o := range orders {
		if o == "Attack" {
			countAttack++
		} else if o == "Retreat" {
			countRetreat++
		}
	}
	if countAttack > countRetreat {
		return "Attack"
	}
	return "Retreat"
}

// randomOrder randomly returns "Attack" or "Retreat".
func randomOrder() string {
	if rand.Intn(2) == 0 {
		return "Attack"
	}
	return "Retreat"
}

// byzantineServer implements the gRPC server interface.
type byzantineServer struct {
	pb.UnimplementedByzantineServiceServer
	node *Node
}

// PropagateOrder is invoked remotely to deliver an order message.
// It ignores duplicate messages from the same sender in the same round.
func (s *byzantineServer) PropagateOrder(ctx context.Context, msg *pb.OrderMessage) (*pb.Ack, error) {
	log.Printf("Node %d: received message from Node %d for round %d: %s", s.node.id, msg.SenderId, msg.Round, msg.Order)
	round := int(msg.Round)
	s.node.mu.Lock()
	defer s.node.mu.Unlock()
	if s.node.receivedMsgs[round] == nil {
		s.node.receivedMsgs[round] = make(map[int32]*pb.OrderMessage)
	}
	// Only record the first message from a given sender in a round.
	if _, exists := s.node.receivedMsgs[round][msg.SenderId]; !exists {
		s.node.receivedMsgs[round][msg.SenderId] = msg
	}
	return &pb.Ack{Received: true}, nil
}

// startGRPCServer starts the gRPC server on the given port.
func startGRPCServer(port string, node *Node, wg *sync.WaitGroup) {
	defer wg.Done()
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("Node %d: failed to listen on port %s: %v", node.id, port, err)
	}
	s := grpc.NewServer()
	pb.RegisterByzantineServiceServer(s, &byzantineServer{node: node})
	log.Printf("Node %d: gRPC server listening on %s", node.id, port)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("Node %d: failed to serve: %v", node.id, err)
	}
}

func main() {
	// Seed the random number generator.
	rand.Seed(time.Now().UnixNano())

	// Command-line flags.
	var (
		id        = flag.Int("id", 1, "Unique ID for this node (general)")
		port      = flag.String("port", ":50051", "Port to listen on")
		peerStr   = flag.String("peers", "", "Comma-separated list of peer addresses (e.g., :50052,:50053)")
		commander = flag.Bool("commander", false, "Set to true if this node is the commander")
		traitor   = flag.Bool("traitor", false, "Set to true if this node is a traitor")
		initOrder = flag.String("order", "Attack", "Initial order (only used by the commander)")
		tVal      = flag.Int("t", 0, "Maximum number of traitors in the system")
		timeout   = flag.Int("timeout", 5, "Timeout in seconds for waiting for messages in each round")
	)
	flag.Parse()

	// Parse peer addresses.
	var peers []string
	if *peerStr != "" {
		peers = strings.Split(*peerStr, ",")
	}

	// Create the node.
	node := newNode(int32(*id), *commander, *traitor, *initOrder, *tVal, peers)

	// Check the condition n > 3t.
	if node.totalNodes <= 3*node.t {
		log.Fatalf("Condition n > 3t not met: total nodes (%d) must be greater than 3*t (%d)", node.totalNodes, 3*node.t)
	}

	// Start the gRPC server in a separate goroutine.
	var wg sync.WaitGroup
	wg.Add(1)
	go startGRPCServer(*port, node, &wg)

	// Wait a short while to ensure all nodes have started.
	time.Sleep(2 * time.Second)

	rounds := node.t + 1
	log.Printf("Node %d: starting simulation for %d rounds...", node.id, rounds)

	// Run simulation rounds.
	for round := 1; round <= rounds; round++ {
		// Print descriptive round name
		switch round {
		case 1:
			log.Printf("Node %d: Round %d - Commander Broadcast", node.id, round)
		case 2:
			log.Printf("Node %d: Round %d - Cross-Verification", node.id, round)
		case 3:
			log.Printf("Node %d: Round %d - Secondary Verification", node.id, round)
		default:
			log.Printf("Node %d: Round %d - Propagation", node.id, round)
		}

		if round == 1 {
			if node.isCommander {
				// Commander sends its (possibly falsified) order to all lieutenants.
				for _, peer := range node.peers {
					orderToSend := node.currentDecision
					if node.isTraitor {
						orderToSend = randomOrder()
					}
					go node.sendMessage(peer, round, orderToSend)
				}
			} else {
				// Lieutenants wait for the commander’s message (expect 1 message).
				node.waitForMessages(round, 1, time.Duration(*timeout)*time.Second)
				node.mu.Lock()
				if !node.isTraitor && len(node.receivedMsgs[round]) > 0 {
					// Honest lieutenants adopt the commander's order.
					// (If multiple messages are received, take the one from the lowest sender ID.)
					var chosen *pb.OrderMessage
					for _, msg := range node.receivedMsgs[round] {
						if chosen == nil || msg.SenderId < chosen.SenderId {
							chosen = msg
						}
					}
					if chosen != nil {
						node.currentDecision = chosen.Order
					}
				} else if node.isTraitor {
					// Traitor lieutenant chooses a random decision.
					node.currentDecision = randomOrder()
				}
				node.mu.Unlock()
			}
		} else {
			// For rounds 2 to t+1, every node sends its current decision to all peers.
			for _, peer := range node.peers {
				orderToSend := node.currentDecision
				if node.isTraitor {
					orderToSend = randomOrder()
				}
				go node.sendMessage(peer, round, orderToSend)
			}
			// Every node expects messages from all peers (n-1 messages).
			expected := node.totalNodes - 1
			node.waitForMessages(round, expected, time.Duration(*timeout)*time.Second)

			// Gather the orders received in this round along with our own decision.
			node.mu.Lock()
			var orders []string
			orders = append(orders, node.currentDecision)
			if node.receivedMsgs[round] != nil {
				for _, msg := range node.receivedMsgs[round] {
					orders = append(orders, msg.Order)
				}
			}
			node.mu.Unlock()

			// Honest nodes update their decision based on the majority vote.
			if !node.isTraitor {
				newDecision := majorityVote(orders)
				node.currentDecision = newDecision
			} else {
				// Traitors choose an arbitrary (random) decision.
				node.currentDecision = randomOrder()
			}
		}
		log.Printf("Node %d: decision after round %d: %s", node.id, round, node.currentDecision)
		time.Sleep(1 * time.Second)
	}

	log.Printf("Node %d: FINAL decision: %s", node.id, node.currentDecision)

	// Shutdown gracefully.
	// (In a real deployment, you might use signals or other methods.)
	log.Printf("Node %d: simulation complete. Shutting down in 2 seconds...", node.id)
	time.Sleep(2 * time.Second)
	os.Exit(0)
}

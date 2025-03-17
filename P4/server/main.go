package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"

	pb "github.com/kshitij-n24/DS-assignment2/P4/protofiles"
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
	receivedMsgs map[int][]*pb.OrderMessage // maps round -> list of messages received
}

// newNode initializes a new Node.
func newNode(id int32, isCommander, isTraitor bool, initOrder string, t int, peers []string) *Node {
	return &Node{
		id:              id,
		isCommander:     isCommander,
		isTraitor:       isTraitor,
		currentDecision: initOrder,
		t:               t,
		peers:           peers,
		totalNodes:      len(peers) + 1,
		receivedMsgs:    make(map[int][]*pb.OrderMessage),
	}
}

// waitForMessages polls until the expected number of messages have been received in the given round.
func (node *Node) waitForMessages(round int, expected int) {
	for {
		node.mu.Lock()
		count := len(node.receivedMsgs[round])
		node.mu.Unlock()
		if count >= expected {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// sendMessage dials a peer and sends the order message for the given round.
func (node *Node) sendMessage(peerAddr string, round int, order string) {
	// Establish connection (for demo purposes, we use insecure connection)
	conn, err := grpc.Dial(peerAddr, grpc.WithInsecure(), grpc.WithBlock(), grpc.WithTimeout(2*time.Second))
	if err != nil {
		log.Printf("Node %d: failed to connect to %s: %v", node.id, peerAddr, err)
		return
	}
	defer conn.Close()
	client := pb.NewByzantineServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	msg := &pb.OrderMessage{
		SenderId: node.id,
		Round:    int32(round),
		Order:    order,
	}
	_, err = client.PropagateOrder(ctx, msg)
	if err != nil {
		log.Printf("Node %d: error sending message to %s: %v", node.id, peerAddr, err)
	}
}

// majorityVote returns "Attack" if the count of "Attack" is greater than that of "Retreat"; otherwise "Retreat".
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
func (s *byzantineServer) PropagateOrder(ctx context.Context, msg *pb.OrderMessage) (*pb.Ack, error) {
	log.Printf("Node %d: received message from Node %d for round %d: %s", s.node.id, msg.SenderId, msg.Round, msg.Order)
	s.node.mu.Lock()
	defer s.node.mu.Unlock()
	round := int(msg.Round)
	s.node.receivedMsgs[round] = append(s.node.receivedMsgs[round], msg)
	return &pb.Ack{Received: true}, nil
}

// startGRPCServer starts the gRPC server on the given port.
func startGRPCServer(port string, node *Node) {
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
		id          = flag.Int("id", 1, "Unique ID for this node (general)")
		port        = flag.String("port", ":50051", "Port to listen on")
		peerStr     = flag.String("peers", "", "Comma-separated list of peer addresses (e.g., :50052,:50053)")
		commander   = flag.Bool("commander", false, "Set to true if this node is the commander")
		traitor     = flag.Bool("traitor", false, "Set to true if this node is a traitor")
		initOrder   = flag.String("order", "Attack", "Initial order (only used by the commander)")
		tVal        = flag.Int("t", 0, "Maximum number of traitors in the system")
	)
	flag.Parse()

	// Parse peer addresses.
	var peers []string
	if *peerStr != "" {
		peers = strings.Split(*peerStr, ",")
	}

	// Create the node.
	node := newNode(int32(*id), *commander, *traitor, *initOrder, *tVal, peers)

	// Start the gRPC server in a separate goroutine.
	go startGRPCServer(*port, node)

	// Wait a short while to ensure all nodes have started.
	time.Sleep(2 * time.Second)

	rounds := node.t + 1
	log.Printf("Node %d: starting simulation for %d rounds...", node.id, rounds)

	// Run simulation rounds.
	for round := 1; round <= rounds; round++ {
		log.Printf("Node %d: starting round %d", node.id, round)
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
				// Lieutenants wait for the commander's message (expect 1 message).
				node.waitForMessages(round, 1)
				node.mu.Lock()
				if !node.isTraitor && len(node.receivedMsgs[round]) > 0 {
					// Honest lieutenants adopt the commander’s order.
					node.currentDecision = node.receivedMsgs[round][0].Order
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
			node.waitForMessages(round, expected)

			// Gather the orders received in this round along with our own decision.
			node.mu.Lock()
			var orders []string
			orders = append(orders, node.currentDecision)
			for _, msg := range node.receivedMsgs[round] {
				orders = append(orders, msg.Order)
			}
			node.mu.Unlock()

			// Honest nodes update their decision based on the majority vote.
			if !node.isTraitor {
				newDecision := majorityVote(orders)
				node.currentDecision = newDecision
			} else {
				// Traitors may choose an arbitrary (or random) decision.
				node.currentDecision = randomOrder()
			}
		}
		log.Printf("Node %d: decision after round %d: %s", node.id, round, node.currentDecision)
		time.Sleep(1 * time.Second)
	}

	log.Printf("Node %d: FINAL decision: %s", node.id, node.currentDecision)

	// Block forever (or add logic to shut down gracefully).
	select {}
}

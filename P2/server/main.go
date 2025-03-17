// server/main.go:
package main

import (
	"container/heap"
	"context"
	"flag"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	pb "github.com/kshitij-n24/DS-assignment2/P2/protofiles"
)

// WorkerInfo represents a worker and its current load.
type WorkerInfo struct {
	addr  string // Worker address
	load  int    // Current number of assigned tasks
	index int    // Index in the heap
}

// WorkerHeap implements heap.Interface based on worker load (min-heap).
type WorkerHeap []*WorkerInfo

func (wh WorkerHeap) Len() int { return len(wh) }
func (wh WorkerHeap) Less(i, j int) bool {
	return wh[i].load < wh[j].load
}
func (wh WorkerHeap) Swap(i, j int) {
	wh[i], wh[j] = wh[j], wh[i]
	wh[i].index = i
	wh[j].index = j
}
func (wh *WorkerHeap) Push(x interface{}) {
	n := len(*wh)
	worker := x.(*WorkerInfo)
	worker.index = n
	*wh = append(*wh, worker)
}
func (wh *WorkerHeap) Pop() interface{} {
	old := *wh
	n := len(old)
	worker := old[n-1]
	worker.index = -1 // for safety
	*wh = old[0 : n-1]
	return worker
}

// WorkerManager manages a priority queue of workers.
type WorkerManager struct {
	workers WorkerHeap
	mu      sync.Mutex
}

// NewWorkerManager initializes the manager with a list of worker addresses.
func NewWorkerManager(addrs []string) *WorkerManager {
	wm := &WorkerManager{
		workers: make(WorkerHeap, len(addrs)),
	}
	for i, addr := range addrs {
		wm.workers[i] = &WorkerInfo{
			addr:  addr,
			load:  0,
			index: i,
		}
	}
	heap.Init(&wm.workers)
	return wm
}

// GetWorker returns the worker with the minimum load and increments its load.
func (wm *WorkerManager) GetWorker() *WorkerInfo {
	wm.mu.Lock()
	defer wm.mu.Unlock()
	worker := wm.workers[0]
	worker.load++
	heap.Fix(&wm.workers, worker.index)
	return worker
}

// ReleaseWorker decrements the load count of a worker.
func (wm *WorkerManager) ReleaseWorker(worker *WorkerInfo) {
	wm.mu.Lock()
	defer wm.mu.Unlock()
	if worker.load > 0 {
		worker.load--
	}
	heap.Fix(&wm.workers, worker.index)
}

// assignMapTask calls the ExecuteTask RPC on a worker for a map task.
func assignMapTask(taskID int, inputFile string, nReducer int, jobType string, wm *WorkerManager, wg *sync.WaitGroup) {
	defer wg.Done()
	worker := wm.GetWorker()
	workerAddr := worker.addr

	conn, err := grpc.Dial(workerAddr, grpc.WithInsecure(), grpc.WithBlock(), grpc.WithTimeout(time.Second*10))
	if err != nil {
		log.Printf("failed to connect to worker %s: %v", workerAddr, err)
		wm.ReleaseWorker(worker)
		return
	}
	defer conn.Close()
	client := pb.NewWorkerClient(conn)
	req := &pb.TaskRequest{
		TaskId:    int32(taskID),
		TaskType:  pb.TaskType_MAP,
		InputFile: inputFile,
		NReducer:  int32(nReducer),
		JobType:   jobType,
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	resp, err := client.ExecuteTask(ctx, req)
	if err != nil {
		log.Printf("Map task %d failed on worker %s: %v", taskID, workerAddr, err)
	} else {
		log.Printf("Map task %d response from worker %s: %s", taskID, workerAddr, resp.Message)
	}
	wm.ReleaseWorker(worker)
}

// assignReduceTask calls the ExecuteTask RPC on a worker for a reduce task.
func assignReduceTask(reducerID int, intermediateFiles []string, jobType string, wm *WorkerManager, wg *sync.WaitGroup) {
	defer wg.Done()
	worker := wm.GetWorker()
	workerAddr := worker.addr

	conn, err := grpc.Dial(workerAddr, grpc.WithInsecure(), grpc.WithBlock(), grpc.WithTimeout(time.Second*10))
	if err != nil {
		log.Printf("failed to connect to worker %s: %v", workerAddr, err)
		wm.ReleaseWorker(worker)
		return
	}
	defer conn.Close()
	client := pb.NewWorkerClient(conn)
	req := &pb.TaskRequest{
		TaskId:            int32(reducerID),
		TaskType:          pb.TaskType_REDUCE,
		IntermediateFiles: intermediateFiles,
		JobType:           jobType,
		ReduceTaskId:      int32(reducerID),
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	resp, err := client.ExecuteTask(ctx, req)
	if err != nil {
		log.Printf("Reduce task %d failed on worker %s: %v", reducerID, workerAddr, err)
	} else {
		log.Printf("Reduce task %d response from worker %s: %s", reducerID, workerAddr, resp.Message)
	}
	wm.ReleaseWorker(worker)
}

func main() {
	// Define CLI flags.
	jobType := flag.String("job", "wordcount", "Job type: wordcount or invertedindex")
	nReducer := flag.Int("nreducer", 2, "Number of reducer tasks")
	workersFlag := flag.String("workers", "", "Comma-separated list of worker addresses (e.g., localhost:5001,localhost:5002)")
	flag.Parse()

	inputFiles := flag.Args()
	if len(inputFiles) == 0 {
		log.Fatal("No input files provided")
	}
	if *workersFlag == "" {
		log.Fatal("No worker addresses provided")
	}
	workerAddrs := strings.Split(*workersFlag, ",")
	log.Printf("Job: %s, Reducers: %d", *jobType, *nReducer)
	log.Printf("Input files: %v", inputFiles)
	log.Printf("Worker addresses: %v", workerAddrs)

	// Initialize the worker manager with a priority queue.
	workerManager := NewWorkerManager(workerAddrs)

	var wg sync.WaitGroup
	mapTaskCount := len(inputFiles)

	// Assign map tasks.
	for i, inputFile := range inputFiles {
		wg.Add(1)
		go assignMapTask(i, inputFile, *nReducer, *jobType, workerManager, &wg)
	}
	wg.Wait()
	log.Printf("All map tasks completed.")

	// Assign reduce tasks.
	for r := 0; r < *nReducer; r++ {
		wg.Add(1)
		var intermediateFiles []string
		for m := 0; m < mapTaskCount; m++ {
			intermediateFiles = append(intermediateFiles, fmt.Sprintf("mr-%d-%d", m, r))
		}
		go assignReduceTask(r, intermediateFiles, *jobType, workerManager, &wg)
	}
	wg.Wait()
	log.Printf("All reduce tasks completed. Job done.")
}

package main

import (
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

func main() {
	// Define CLI flags.
	jobType := flag.String("job", "wordcount", "Job type: wordcount or invertedindex")
	nReducer := flag.Int("nreducer", 1, "Number of reducer tasks (set to 1 for aggregated word count output)")
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

	var wg sync.WaitGroup
	mapTaskCount := len(inputFiles)

	// Assign a map task for each input file (round-robin assignment).
	for i, inputFile := range inputFiles {
		wg.Add(1)
		workerAddr := workerAddrs[i%len(workerAddrs)]
		go func(taskID int, input string, worker string) {
			defer wg.Done()
			assignMapTask(taskID, input, *nReducer, *jobType, worker)
		}(i, inputFile, workerAddr)
	}
	wg.Wait()
	log.Printf("All map tasks completed.")

	// Assign reduce tasks: each reduce task collects intermediate files from all map tasks.
	for r := 0; r < *nReducer; r++ {
		wg.Add(1)
		var intermediateFiles []string
		for m := 0; m < mapTaskCount; m++ {
			intermediateFiles = append(intermediateFiles, fmt.Sprintf("mr-%d-%d", m, r))
		}
		workerAddr := workerAddrs[r%len(workerAddrs)]
		go func(reducerID int, files []string, worker string) {
			defer wg.Done()
			assignReduceTask(reducerID, files, *jobType, worker)
		}(r, intermediateFiles, workerAddr)
	}
	wg.Wait()
	log.Printf("All reduce tasks completed. Job done.")

	// Shutdown all workers.
	for _, workerAddr := range workerAddrs {
		shutdownWorker(workerAddr)
	}
}

// assignMapTask calls the ExecuteTask RPC on a worker for a map task.
func assignMapTask(taskID int, inputFile string, nReducer int, jobType string, workerAddr string) {
	conn, err := grpc.Dial(workerAddr, grpc.WithInsecure())
	if err != nil {
		log.Printf("failed to connect to worker %s: %v", workerAddr, err)
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
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := client.ExecuteTask(ctx, req)
	if err != nil {
		log.Printf("Map task %d failed on worker %s: %v", taskID, workerAddr, err)
		return
	}
	log.Printf("Map task %d response from worker %s: %s", taskID, workerAddr, resp.Message)
}

// assignReduceTask calls the ExecuteTask RPC on a worker for a reduce task.
func assignReduceTask(reducerID int, intermediateFiles []string, jobType string, workerAddr string) {
	conn, err := grpc.Dial(workerAddr, grpc.WithInsecure())
	if err != nil {
		log.Printf("failed to connect to worker %s: %v", workerAddr, err)
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
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := client.ExecuteTask(ctx, req)
	if err != nil {
		log.Printf("Reduce task %d failed on worker %s: %v", reducerID, workerAddr, err)
		return
	}
	log.Printf("Reduce task %d response from worker %s: %s", reducerID, workerAddr, resp.Message)
}

// shutdownWorker sends the Shutdown RPC to a worker.
func shutdownWorker(workerAddr string) {
	conn, err := grpc.Dial(workerAddr, grpc.WithInsecure())
	if err != nil {
		log.Printf("failed to connect to worker %s for shutdown: %v", workerAddr, err)
		return
	}
	defer conn.Close()
	client := pb.NewWorkerClient(conn)
	req := &pb.ShutdownRequest{}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := client.Shutdown(ctx, req)
	if err != nil {
		log.Printf("failed to shutdown worker %s: %v", workerAddr, err)
		return
	}
	log.Printf("Shutdown response from worker %s: %s", workerAddr, resp.Message)
}

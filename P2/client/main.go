package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"google.golang.org/grpc"
	pb "github.com/<user>/DS-assignment2/P2/protofiles"
)

// server implements the Worker gRPC service.
type server struct {
	pb.UnimplementedWorkerServer
	grpcServer *grpc.Server
}

// Regular expression to extract words (alphanumeric only)
var wordRegex = regexp.MustCompile(`[a-zA-Z0-9]+`)

// ExecuteTask processes the assigned map or reduce task.
func (s *server) ExecuteTask(ctx context.Context, req *pb.TaskRequest) (*pb.TaskResponse, error) {
	switch req.TaskType {
	case pb.TaskType_MAP:
		log.Printf("Received MAP task (ID=%d) for file: %s", req.TaskId, req.InputFile)
		err := handleMapTask(req.TaskId, req.InputFile, int(req.NReducer), req.JobType)
		if err != nil {
			return &pb.TaskResponse{Success: false, Message: err.Error()}, nil
		}
		return &pb.TaskResponse{Success: true, Message: "Map task completed"}, nil
	case pb.TaskType_REDUCE:
		log.Printf("Received REDUCE task (TaskID=%d, ReducerID=%d)", req.TaskId, req.ReduceTaskId)
		err := handleReduceTask(int(req.ReduceTaskId), req.IntermediateFiles, req.JobType)
		if err != nil {
			return &pb.TaskResponse{Success: false, Message: err.Error()}, nil
		}
		return &pb.TaskResponse{Success: true, Message: "Reduce task completed"}, nil
	default:
		return &pb.TaskResponse{Success: false, Message: "Unknown task type"}, nil
	}
}

// Shutdown handles the Shutdown RPC to gracefully stop the worker.
func (s *server) Shutdown(ctx context.Context, req *pb.ShutdownRequest) (*pb.ShutdownResponse, error) {
	log.Printf("Received shutdown request. Shutting down worker.")
	// Delay a little to allow the RPC response to be sent.
	go func() {
		time.Sleep(1 * time.Second)
		s.grpcServer.GracefulStop()
	}()
	return &pb.ShutdownResponse{Success: true, Message: "Shutting down"}, nil
}

// handleMapTask reads the input file, tokenizes it, and writes intermediate files.
func handleMapTask(taskID int32, inputFile string, nReducer int, jobType string) error {
	file, err := os.Open(inputFile)
	if err != nil {
		return fmt.Errorf("failed to open input file: %v", err)
	}
	defer file.Close()

	// Open nReducer files for partitioned output using buffered writers.
	writers := make([]*bufio.Writer, nReducer)
	files := make([]*os.File, nReducer)
	for i := 0; i < nReducer; i++ {
		outFileName := fmt.Sprintf("mr-%d-%d", taskID, i)
		f, err := os.Create(outFileName)
		if err != nil {
			return fmt.Errorf("failed to create intermediate file %s: %v", outFileName, err)
		}
		files[i] = f
		writers[i] = bufio.NewWriter(f)
	}
	
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		// Use regex to extract words (converted to lowercase).
		words := wordRegex.FindAllString(strings.ToLower(line), -1)
		for _, word := range words {
			if word == "" {
				continue
			}
			// Partition the word to one of the reducers.
			reducerIdx := ihash(word, nReducer)
			switch jobType {
			case "wordcount":
				// For word count, output (word, 1).
				_, err := writers[reducerIdx].WriteString(fmt.Sprintf("%s %d\n", word, 1))
				if err != nil {
					return err
				}
			case "invertedindex":
				// For inverted index, output (word, filename).
				_, err := writers[reducerIdx].WriteString(fmt.Sprintf("%s %s\n", word, filepath.Base(inputFile)))
				if err != nil {
					return err
				}
			default:
				return fmt.Errorf("unknown job type: %s", jobType)
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}

	// Flush and close all buffered writers and files.
	for i := 0; i < nReducer; i++ {
		if err := writers[i].Flush(); err != nil {
			return fmt.Errorf("failed to flush file mr-%d-%d: %v", taskID, i, err)
		}
		files[i].Close()
	}

	log.Printf("Map task %d completed for file %s", taskID, inputFile)
	return nil
}

// handleReduceTask reads the intermediate files, aggregates the results, and writes final output.
func handleReduceTask(reduceTaskID int, intermediateFiles []string, jobType string) error {
	switch jobType {
	case "wordcount":
		// Aggregate word counts.
		wc := make(map[string]int)
		for _, filename := range intermediateFiles {
			file, err := os.Open(filename)
			if err != nil {
				log.Printf("failed to open intermediate file %s: %v", filename, err)
				continue
			}
			scanner := bufio.NewScanner(file)
			for scanner.Scan() {
				line := scanner.Text()
				parts := strings.Fields(line)
				if len(parts) != 2 {
					log.Printf("warning: malformed line in file %s: %s", filename, line)
					continue
				}
				word := parts[0]
				count, err := strconv.Atoi(parts[1])
				if err != nil {
					log.Printf("warning: invalid count in file %s: %s", filename, parts[1])
					continue
				}
				wc[word] += count
			}
			file.Close()
		}
		// Write the aggregated counts to the final output file.
		outFileName := fmt.Sprintf("mr-out-%d", reduceTaskID)
		outFile, err := os.Create(outFileName)
		if err != nil {
			return fmt.Errorf("failed to create output file: %v", err)
		}
		defer outFile.Close()
		writer := bufio.NewWriter(outFile)
		for word, count := range wc {
			_, err := writer.WriteString(fmt.Sprintf("%s %d\n", word, count))
			if err != nil {
				return err
			}
		}
		writer.Flush()
	case "invertedindex":
		// Aggregate a list of filenames for each word.
		idx := make(map[string]map[string]bool)
		for _, filename := range intermediateFiles {
			file, err := os.Open(filename)
			if err != nil {
				log.Printf("failed to open intermediate file %s: %v", filename, err)
				continue
			}
			scanner := bufio.NewScanner(file)
			for scanner.Scan() {
				line := scanner.Text()
				parts := strings.Fields(line)
				if len(parts) != 2 {
					log.Printf("warning: malformed line in file %s: %s", filename, line)
					continue
				}
				word, fname := parts[0], parts[1]
				if idx[word] == nil {
					idx[word] = make(map[string]bool)
				}
				idx[word][fname] = true
			}
			file.Close()
		}
		// Write the inverted index to the output file.
		outFileName := fmt.Sprintf("mr-out-%d", reduceTaskID)
		outFile, err := os.Create(outFileName)
		if err != nil {
			return fmt.Errorf("failed to create output file: %v", err)
		}
		defer outFile.Close()
		writer := bufio.NewWriter(outFile)
		for word, filesMap := range idx {
			filesList := []string{}
			for f := range filesMap {
				filesList = append(filesList, f)
			}
			_, err := writer.WriteString(fmt.Sprintf("%s %v\n", word, filesList))
			if err != nil {
				return err
			}
		}
		writer.Flush()
	default:
		return fmt.Errorf("unknown job type: %s", jobType)
	}
	log.Printf("Reduce task %d completed", reduceTaskID)
	return nil
}

// ihash is a simple hash function to partition keys across reducers.
func ihash(key string, n int) int {
	hash := 0
	for _, ch := range key {
		hash = int(ch) + hash*31
	}
	return hash % n
}

func main() {
	if len(os.Args) < 2 {
		log.Fatalf("Usage: %s <port>", os.Args[0])
	}
	port := os.Args[1]
	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	workerServer := &server{grpcServer: grpcServer}
	pb.RegisterWorkerServer(grpcServer, workerServer)
	log.Printf("Worker server listening on port %s", port)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

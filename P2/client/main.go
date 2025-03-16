package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"google.golang.org/grpc"
	pb "github.com/kshitij-n24/DS-assignment2/P2/protofiles"
)

// server implements the Worker gRPC service.
type server struct {
	pb.UnimplementedWorkerServer
}

// A simple hash function to partition keys across reducers.
func ihash(key string, n int) int {
	hash := 0
	for _, ch := range key {
		hash = int(ch) + hash*31
	}
	return hash % n
}

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

// handleMapTask reads the input file, tokenizes it, and writes intermediate files.
func handleMapTask(taskID int32, inputFile string, nReducer int, jobType string) error {
	file, err := os.Open(inputFile)
	if err != nil {
		return fmt.Errorf("failed to open input file: %v", err)
	}
	defer file.Close()

	// Open nReducer files for partitioned output.
	writers := make([]*os.File, nReducer)
	for i := 0; i < nReducer; i++ {
		outFileName := fmt.Sprintf("mr-%d-%d", taskID, i)
		f, err := os.Create(outFileName)
		if err != nil {
			return fmt.Errorf("failed to create intermediate file %s: %v", outFileName, err)
		}
		writers[i] = f
		defer f.Close()
	}

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		words := strings.Fields(line)
		for _, word := range words {
			// Clean the word (e.g., remove punctuation, convert to lowercase).
			cleaned := strings.ToLower(strings.Trim(word, ".,!?:;\"'()"))
			if cleaned == "" {
				continue
			}
			// Partition the word to one of the reducers.
			reducerIdx := ihash(cleaned, nReducer)
			switch jobType {
			case "wordcount":
				// For word count, output (word, 1).
				_, err := writers[reducerIdx].WriteString(fmt.Sprintf("%s %d\n", cleaned, 1))
				if err != nil {
					return err
				}
			case "invertedindex":
				// For inverted index, output (word, filename).
				_, err := writers[reducerIdx].WriteString(fmt.Sprintf("%s %s\n", cleaned, filepath.Base(inputFile)))
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
					continue
				}
				word := parts[0]
				count, err := strconv.Atoi(parts[1])
				if err != nil {
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

func main() {
	if len(os.Args) < 2 {
		log.Fatalf("Usage: %s <port>", os.Args[0])
	}
	port := os.Args[1]
	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterWorkerServer(s, &server{})
	log.Printf("Worker server listening on port %s", port)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

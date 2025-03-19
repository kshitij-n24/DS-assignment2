
# Distributed MapReduce System

## Overview

This system implements a distributed MapReduce framework using gRPC for communication between clients (master) and workers. It supports two job types: **Word Count** and **Inverted Index**. The system is designed to efficiently process large datasets by distributing tasks across multiple worker nodes.

## Dependency Installation

Before running the system, you must install the required dependencies:

1. **gRPC and Protocol Buffers**:
   - Follow the installation guide for gRPC: [gRPC Installation](https://grpc.io/blog/installation/)
   - Install Protocol Buffers: [Protocol Buffers Installation](https://protobuf.dev/overview/)

2. **Go (or Python)**:
   - Install Go: [Go Installation](https://golang.org/doc/install) (for Go implementation).
   - Install Python (for Python implementation): [Python Installation](https://www.python.org/downloads/).

3. **gRPC for Go**:
   - Install the gRPC Go package: 
     ```bash
     go get google.golang.org/grpc
     ```

4. **gRPC for Python**:
   - Install the gRPC Python package:
     ```bash
     pip install grpcio grpcio-tools
     ```

### Steps to Run the System

1. **Start the Workers**:  
   Each worker runs on a different port and listens for task requests from the master.  
   Run the following command to start a worker:
   ```bash
   go run server/main.go <port>
   ```

2. **Start the Master**:  
   The master assigns map and reduce tasks to workers.  
   Run the following command to start the master:
   ```bash
   go run client/main.go --job <job_type> --nreducer <num_reducers> --workers <worker_addresses> <input_files>
   ```

   - `<job_type>`: `wordcount` or `invertedindex`.
   - `<num_reducers>`: Number of reducers (e.g., 2).
   - `<worker_addresses>`: Comma-separated list of worker addresses (e.g., `localhost:5001,localhost:5002`).
   - `<input_files>`: List of input files to process.

### Tests Performed

- **Word Count**: Tests were performed on a sample input file to check if the word count task was executed successfully, with correct aggregation of word counts across all workers.
- **Inverted Index**: The inverted index task was tested using multiple input files to ensure correct generation of word-to-file mappings.

### Input/Output Format

- **Word Count Output**:
  ```
  word1 5
  word2 10
  ```

- **Inverted Index Output**:
  ```
  word1 ["file1.txt", "file2.txt"]
  word2 ["file3.txt"]
  ```

### Example

#### Input:
```text
This is a sample text file.
This file contains some words.
```

#### Output (Word Count):
```text
this 2
is 1
a 1
sample 1
text 1
file 2
contains 1
some 1
words 1
```

#### Output (Inverted Index):
```text
this ["file1.txt"]
is ["file1.txt"]
a ["file1.txt"]
sample ["file1.txt"]
text ["file1.txt"]
file ["file1.txt"]
contains ["file2.txt"]
some ["file2.txt"]
words ["file2.txt"]
```

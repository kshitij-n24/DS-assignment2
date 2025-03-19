#!/usr/bin/env python3
import csv
import sys
import matplotlib.pyplot as plt

def read_csv(filename):
    # Initialize lists for each column.
    total_requests = []
    successful = []
    total_time = []
    avg_latency = []
    throughput = []
    
    with open(filename, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        # Adjust fieldnames based on the CSV header.
        for row in reader:
            try:
                total_requests.append(int(row["Total Requests"]))
                successful.append(int(row["Successful"]))
                total_time.append(float(row["Total Time"]))
                avg_latency.append(float(row["Average Latency"]))
                throughput.append(float(row["Throughput"]))
            except Exception as e:
                print(f"Error parsing row: {e}")
    return total_requests, successful, total_time, avg_latency, throughput

def plot_avg_latency(avg_latency):
    plt.figure()
    # x-axis will be the index (or test run number)
    runs = list(range(1, len(avg_latency) + 1))
    plt.plot(runs, avg_latency, marker='o', linestyle='-', label="Average Latency")
    plt.xlabel("Test Run")
    plt.ylabel("Average Latency")
    plt.title("Average Latency Across Test Runs")
    plt.legend()
    plt.grid(True)
    plt.savefig("avg_latency_plot.png")
    plt.show()

def main():
    if len(sys.argv) != 2:
        print("Usage: python3 run_plot.py <csv_file>")
        sys.exit(1)
    csv_file = sys.argv[1]
    _, _, _, avg_latency, _ = read_csv(csv_file)
    plot_avg_latency(avg_latency)

if __name__ == "__main__":
    main()

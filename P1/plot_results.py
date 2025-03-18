#!/usr/bin/env python3
import sys
import csv
import matplotlib.pyplot as plt

def read_results(csv_file):
    timestamps = []
    latencies = []
    with open(csv_file, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            timestamps.append(float(row["timestamp"]))
            latencies.append(float(row["latency_ms"]))
    return timestamps, latencies

def plot_latency(timestamps, latencies):
    plt.figure()
    plt.plot(timestamps, latencies, marker='o', linestyle='-')
    plt.title("Latency over Time")
    plt.xlabel("Timestamp (s)")
    plt.ylabel("Latency (ms)")
    plt.grid(True)
    plt.savefig("latency_plot.png")
    plt.show()

def main():
    if len(sys.argv) != 2:
        print("Usage: python3 plot_results.py results.csv")
        sys.exit(1)
    csv_file = sys.argv[1]
    timestamps, latencies = read_results(csv_file)
    plot_latency(timestamps, latencies)

if __name__ == "__main__":
    main()

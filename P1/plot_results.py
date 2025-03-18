#!/usr/bin/env python3
import pandas as pd
import matplotlib.pyplot as plt

# Read CSV
df = pd.read_csv("results.csv")

# Convert columns to numeric
df['response_time_ms'] = pd.to_numeric(df['response_time_ms'], errors='coerce')
df['backend_load'] = pd.to_numeric(df['backend_load'], errors='coerce')

# Convert timestamp to datetime
df['datetime'] = pd.to_datetime(df['timestamp'], unit='s')

# Plot 1: Response Time Over Time
plt.figure(figsize=(10, 6))
plt.plot(df['datetime'], df['response_time_ms'], 'o-', markersize=3)
plt.xlabel("Time")
plt.ylabel("Response Time (ms)")
plt.title("Response Time Over Time")
plt.grid(True)
plt.tight_layout()
plt.savefig("response_time_over_time.png")
plt.show()

# Plot 2: Throughput Over Time (Requests per Second)
df['second'] = df['datetime'].dt.floor('S')
throughput = df.groupby('second').size().reset_index(name='requests')
plt.figure(figsize=(10, 6))
plt.bar(throughput['second'], throughput['requests'], width=0.8)
plt.xlabel("Time (by second)")
plt.ylabel("Number of Requests")
plt.title("Throughput Over Time")
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig("throughput.png")
plt.show()

# Plot 3: Load Distribution per Backend
load_dist = df.groupby('backend_address')['backend_load'].mean().reset_index()
plt.figure(figsize=(10, 6))
plt.bar(load_dist['backend_address'], load_dist['backend_load'])
plt.xlabel("Backend Address")
plt.ylabel("Average Load")
plt.title("Load Distribution per Backend")
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig("load_distribution.png")
plt.show()

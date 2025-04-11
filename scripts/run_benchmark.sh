#!/bin/bash

# Script for running ChronDB SQL Protocol benchmark tests
# These tests are designed to benchmark performance with 1GB+ of data

echo "ChronDB SQL Protocol Benchmark"
echo "-----------------------------"
echo "WARNING: This will generate a large amount of data (1GB+)"
echo "Benchmark results will be stored in benchmark_results.txt"
echo ""

# Create a timestamp for this run
TIMESTAMP=$(date +"%Y-%m-%d_%H-%M-%S")
RESULTS_FILE="benchmark_results_${TIMESTAMP}.txt"

# Run the benchmark tests
echo "Starting benchmark at $(date)"
echo "Starting benchmark at $(date)" >> $RESULTS_FILE

# Execute the benchmark using Clojure CLI tools
JAVA_OPTS="-Xms1g -Xmx4g" clojure -M:benchmark 2>&1 | tee -a $RESULTS_FILE

echo "" >> $RESULTS_FILE
echo "Benchmark completed at $(date)" >> $RESULTS_FILE
echo "Benchmark completed at $(date)"
echo "Results saved to $RESULTS_FILE"
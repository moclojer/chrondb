#!/bin/bash

# Script to run all Redis-related tests for ChronDB

echo "Running Redis Protocol Implementation Tests for ChronDB"
echo "======================================================"
echo

# Function to run test with timeout
run_test_with_timeout() {
    local test_name=$1

    echo "Running $test_name..."
    clojure -M:test -n $test_name &
    local pid=$!

    # Wait for 60 seconds or until the process completes
    local count=0
    while kill -0 $pid 2>/dev/null && [ $count -lt 60 ]; do
        sleep 1
        ((count++))
    done

    # Check if the process is still running after timeout
    if kill -0 $pid 2>/dev/null; then
        echo "ERROR: $test_name timed out after 60 seconds!"
        kill -9 $pid 2>/dev/null
        wait $pid 2>/dev/null
        return 1
    fi

    # Wait for the process to complete and get its exit status
    wait $pid
    local status=$?

    if [ $status -ne 0 ]; then
        echo "ERROR: $test_name failed!"
        return 1
    else
        echo "$test_name completed successfully."
        return 0
    fi
}

# Run unit tests
echo "Running Redis Unit Tests..."
run_test_with_timeout chrondb.api.redis.redis-test
echo

# Run integration tests
echo "Running Redis Integration Tests..."
run_test_with_timeout chrondb.api.redis.redis-integration-test
echo

# Run Jedis compatibility tests
echo "Running Redis Jedis Compatibility Tests..."
run_test_with_timeout chrondb.api.redis.redis-jedis-test
echo

# Run performance tests
echo "Running Redis Performance Tests..."
run_test_with_timeout chrondb.api.redis.redis-performance-test
echo

# Run benchmark tests
echo "Running Redis Benchmark Tests..."
run_test_with_timeout chrondb.api.redis.redis-benchmark-test
echo

echo "All Redis tests completed!"

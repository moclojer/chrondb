#!/bin/bash

# Script to set up dependencies for Redis tests

echo "Setting up dependencies for Redis tests"
echo "======================================"
echo

# Check Java version
if command -v java &>/dev/null; then
    java_version=$(java -version 2>&1 | head -1 | cut -d'"' -f2 | sed 's/^1\.//' | cut -d'.' -f1)
    echo "Java version: $java_version"
    if [ "$java_version" -lt 8 ]; then
        echo "Java 8 or higher is required for Jedis tests"
        exit 1
    fi
else
    echo "Java is not installed. Please install Java 8 or higher to run the Jedis tests."
    exit 1
fi

# Install Clojure dependencies
echo "Installing Clojure dependencies..."
clojure -P -M:test || {
    echo "Failed to install Clojure dependencies"
    exit 1
}
echo "Clojure dependencies installed successfully"
echo

echo "All dependencies for Redis tests are set up successfully!"
echo "You can now run the tests using: ./scripts/run_redis_tests.sh"

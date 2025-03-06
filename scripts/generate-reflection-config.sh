#!/bin/bash
# Script to generate GraalVM reflection configuration using the tracing agent
# This helps identify classes that need reflection configuration for native-image

set -e

# Default options
OUTPUT_DIR="graalvm-config"
JAR_FILE="target/chrondb-standalone.jar"
MAIN_CLASS="chrondb.core"
ARGS=""

# Use o JAVA_HOME definido ou mantenha o valor atual
JAVA_HOME=${JAVA_HOME:-/nix/store/44rl2mxaqh7i0qbmlc1zjgfcw9dkr65a-graalvm-oracle-22}
export JAVA_HOME
export PATH=$JAVA_HOME/bin:$PATH

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
    --output-dir)
        OUTPUT_DIR="$2"
        shift 2
        ;;
    --jar)
        JAR_FILE="$2"
        shift 2
        ;;
    --main-class)
        MAIN_CLASS="$2"
        shift 2
        ;;
    --args)
        ARGS="$2"
        shift 2
        ;;
    *)
        echo "Unknown option: $1"
        echo "Usage: $0 [--output-dir DIR] [--jar JAR_FILE] [--main-class CLASS] [--args 'ARGS']"
        exit 1
        ;;
    esac
done

# Check if JAVA_HOME is set and points to GraalVM
if [ -z "$JAVA_HOME" ] || [ ! -f "$JAVA_HOME/bin/java" ]; then
    echo "Error: JAVA_HOME is not set or does not point to a valid Java installation."
    echo "Please set JAVA_HOME to point to your GraalVM installation."
    exit 1
fi

# Check if the jar file exists
if [ ! -f "$JAR_FILE" ]; then
    echo "Error: JAR file $JAR_FILE not found."
    echo "Please build the uberjar first with: clojure -T:build uber"
    exit 1
fi

# Create output directory if it doesn't exist
mkdir -p "$OUTPUT_DIR"

echo "Generating reflection configuration using GraalVM tracing agent..."
echo "Output directory: $OUTPUT_DIR"
echo "JAR file: $JAR_FILE"
echo "Main class: $MAIN_CLASS"

# Run the application with the tracing agent
echo "Running application with tracing agent. Please exercise all code paths and then terminate the application."
echo "Press Ctrl+C when you're done testing the application."

"$JAVA_HOME/bin/java" -agentlib:native-image-agent=config-output-dir="$OUTPUT_DIR" \
    -cp "$JAR_FILE" "$MAIN_CLASS" $ARGS

echo "Configuration files have been generated in $OUTPUT_DIR."
echo "You can now build the native image with: ./scripts/build-native-image.sh"

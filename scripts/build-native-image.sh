#!/bin/bash
# Comprehensive build script for GraalVM native-image
# This script includes extensive configuration to handle reflection issues

set -e

# Default options
VERBOSE=true
STATIC=false
OUTPUT_NAME=${OUTPUT_NAME:-"chrondb"}
EXTRA_FLAGS=()

# Use o JAVA_HOME definido ou mantenha o valor atual
JAVA_HOME=${JAVA_HOME:-/nix/store/44rl2mxaqh7i0qbmlc1zjgfcw9dkr65a-graalvm-oracle-22}
export JAVA_HOME
export PATH=$JAVA_HOME/bin:$PATH

while [[ $# -gt 0 ]]; do
    case $1 in
    --static)
        STATIC=true
        shift
        ;;
    --output)
        OUTPUT_NAME="$2"
        shift 2
        ;;
    --extra-flags)
        EXTRA_FLAGS+=("$2")
        shift 2
        ;;
    *)
        echo "Unknown option: $1"
        echo "Usage: $0 [--static] [--output NAME] [--extra-flags '[flag1,flag2,...]']"
        exit 1
        ;;
    esac
done

# Check if we're on macOS and using Nix
if [[ "$(uname)" == "Darwin" ]]; then
    echo "Detected macOS, searching for libz in Nix store..."
    LIBZ_PATH=$(find /nix/store -name "libz.dylib" | head -n 1)

    if [[ -n "$LIBZ_PATH" ]]; then
        echo "Found libz at: $LIBZ_PATH"
        LIBZ_DIR=$(dirname "$LIBZ_PATH")
        echo "Setting LIBRARY_PATH to include: $LIBZ_DIR"
        export LIBRARY_PATH="$LIBZ_DIR:$LIBRARY_PATH"
    else
        echo "Could not find libz.dylib in Nix store"
        exit 1
    fi
fi

# Create necessary directories
mkdir -p reports
mkdir -p graalvm-config

# Build the uberjar
echo "Building uberjar..."
clojure -T:build uber

# Build the native image directly
echo "Building native image with comprehensive configuration..."

# Construct the native-image command with careful ordering of initialization options
NATIVE_IMAGE_CMD="native-image \
    --no-fallback \
    --report-unsupported-elements-at-runtime \
    --initialize-at-build-time \
    --initialize-at-run-time=java.security.SecureRandom \
    --initialize-at-run-time=org.eclipse.jetty.server.Server \
    --initialize-at-run-time=org.eclipse.jetty.util.thread.QueuedThreadPool \
    --initialize-at-run-time=org.eclipse.jgit.lib.internal.WorkQueue \
    --initialize-at-run-time=org.eclipse.jgit.transport.HttpAuthMethod \
    --initialize-at-run-time=org.eclipse.jgit.internal.storage.file.WindowCache \
    --initialize-at-run-time=org.eclipse.jgit.util.FileUtils \
    -H:+ReportExceptionStackTraces \
    -H:-CheckToolchain \
    -H:ConfigurationFileDirectories=graalvm-config \
    -H:+PrintClassInitialization \
    -H:+AllowIncompleteClasspath \
    -H:+AddAllCharsets \
    -H:EnableURLProtocols=http,https \
    -H:ReflectionConfigurationFiles=graalvm-config/reflect-config.json \
    -H:ResourceConfigurationFiles=graalvm-config/resource-config.json \
    -H:+UnlockExperimentalVMOptions"

# Add the library path options for macOS with Nix
if [[ "$(uname)" == "Darwin" && -n "$LIBZ_DIR" ]]; then
    NATIVE_IMAGE_CMD="$NATIVE_IMAGE_CMD \
    -H:NativeLinkerOption=-L$LIBZ_DIR \
    -H:NativeLinkerOption=-Wl,-rpath,$LIBZ_DIR"
fi

# Add the output name and jar file
NATIVE_IMAGE_CMD="$NATIVE_IMAGE_CMD \
    -jar target/chrondb-*-standalone.jar \
    $OUTPUT_NAME"

# Execute the command
echo "Executing: $NATIVE_IMAGE_CMD"
eval $NATIVE_IMAGE_CMD

echo "Native image build completed. You can run it with: ./$OUTPUT_NAME"

#!/bin/bash
# Build script that uses the native-image function from build.clj
# This script leverages the Clojure build tooling directly

set -e

# Default options
VERBOSE=true
STATIC=false
OUTPUT_NAME=${OUTPUT_NAME:-"chrondb"}
EXTRA_FLAGS=()
CLJ_EASY=true

# Use o JAVA_HOME definido ou mantenha o valor atual
# No GitHub Actions, o JAVA_HOME já estará configurado corretamente
if [ -z "$JAVA_HOME" ]; then
    echo "JAVA_HOME is not set, using default GraalVM path"
    JAVA_HOME=/nix/store/44rl2mxaqh7i0qbmlc1zjgfcw9dkr65a-graalvm-oracle-22
fi
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
    --no-clj-easy)
        CLJ_EASY=false
        shift
        ;;
    *)
        echo "Unknown option: $1"
        echo "Usage: $0 [--static] [--output NAME] [--extra-flags '[flag1,flag2,...]'] [--no-clj-easy]"
        exit 1
        ;;
    esac
done

# Check if we're on macOS and handle libz
if [[ "$(uname)" == "Darwin" ]]; then
    echo "Detected macOS, searching for libz..."

    # First try Nix store
    LIBZ_PATH=$(find /nix/store -name "libz.dylib" 2>/dev/null | head -n 1)

    # If not found in Nix store, try common system locations
    if [[ -z "$LIBZ_PATH" ]]; then
        for path in /usr/lib/libz.dylib /usr/local/lib/libz.dylib /opt/homebrew/lib/libz.dylib; do
            if [[ -f "$path" ]]; then
                LIBZ_PATH="$path"
                break
            fi
        done
    fi

    if [[ -n "$LIBZ_PATH" ]]; then
        echo "Found libz at: $LIBZ_PATH"
        LIBZ_DIR=$(dirname "$LIBZ_PATH")
        echo "Setting LIBRARY_PATH to include: $LIBZ_DIR"
        export LIBRARY_PATH="$LIBZ_DIR:$LIBRARY_PATH"
        # Also set DYLD_LIBRARY_PATH for dynamic linking
        export DYLD_LIBRARY_PATH="$LIBZ_DIR:$DYLD_LIBRARY_PATH"
        # Set LD_LIBRARY_PATH as well for good measure
        export LD_LIBRARY_PATH="$LIBZ_DIR:$LD_LIBRARY_PATH"
        # Add extra flags to ensure the linker can find libz
        EXTRA_FLAGS+=("-H:CLibraryPath=$LIBZ_DIR")
    else
        echo "Could not find libz.dylib, will rely on system defaults"
    fi
elif [[ "$(uname)" == "Linux" ]]; then
    echo "Detected Linux, searching for libz..."

    # Try to find libz.so in common locations
    LIBZ_PATH=$(find /usr/lib* /lib* -name "libz.so*" 2>/dev/null | head -n 1)

    if [[ -n "$LIBZ_PATH" ]]; then
        echo "Found libz at: $LIBZ_PATH"
        LIBZ_DIR=$(dirname "$LIBZ_PATH")
        echo "Setting LD_LIBRARY_PATH to include: $LIBZ_DIR"
        export LD_LIBRARY_PATH="$LIBZ_DIR:$LD_LIBRARY_PATH"
        # Add extra flags to ensure the linker can find libz
        EXTRA_FLAGS+=("-H:CLibraryPath=$LIBZ_DIR")
    else
        echo "Could not find libz.so, will rely on system defaults"
    fi
fi

# Create necessary directories
mkdir -p reports

# Ensure graalvm-config directory has necessary files
if [ ! -f "graalvm-config/reflect-config.json" ]; then
    echo "not found graalvm-config/reflect-config.json"
    exit 1
fi

if [ ! -f "graalvm-config/resource-config.json" ]; then
    echo "not found graalvm-config/resource-config.json"
    exit 1
fi

# Build using the native-image function from build.clj
echo "Building native image using build.clj..."

# Construct the Clojure command
CLOJURE_PARAMS=""

# Add options
if [ "$VERBOSE" = true ]; then
    CLOJURE_PARAMS="$CLOJURE_PARAMS :verbose true"
fi

if [ "$STATIC" = true ]; then
    CLOJURE_PARAMS="$CLOJURE_PARAMS :static true"
fi

if [ -n "$OUTPUT_NAME" ]; then
    CLOJURE_PARAMS="$CLOJURE_PARAMS :output '\"$OUTPUT_NAME\"'"
fi

if [ "$CLJ_EASY" = true ]; then
    CLOJURE_PARAMS="$CLOJURE_PARAMS :clj_easy true"
else
    CLOJURE_PARAMS="$CLOJURE_PARAMS :clj_easy false"
fi

# Add extra flags if any
if [ ${#EXTRA_FLAGS[@]} -gt 0 ]; then
    EXTRA_FLAGS_STR=$(printf "%s," "${EXTRA_FLAGS[@]}")
    EXTRA_FLAGS_STR=${EXTRA_FLAGS_STR%,} # Remove trailing comma
    CLOJURE_PARAMS="$CLOJURE_PARAMS :extra_flags '[\"$EXTRA_FLAGS_STR\"]'"
fi

# Print environment variables for debugging
echo "Environment variables:"
echo "JAVA_HOME=$JAVA_HOME"
echo "LIBRARY_PATH=$LIBRARY_PATH"
echo "DYLD_LIBRARY_PATH=$DYLD_LIBRARY_PATH"
echo "LD_LIBRARY_PATH=$LD_LIBRARY_PATH"

# Execute the command
echo "Executing clojure params: $CLOJURE_PARAMS"
clojure -T:build native-image $CLOJURE_PARAMS

echo "Native image build completed. You can run it with: ./$OUTPUT_NAME"

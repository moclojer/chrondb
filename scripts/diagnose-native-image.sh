#!/bin/bash
# Diagnostic script for GraalVM native-image issues
# This script helps identify class initialization problems

set -e

echo "=== GraalVM Native Image Diagnostic Tool ==="
echo "This script will help diagnose issues with class initialization in GraalVM native-image"

# Check Java and GraalVM versions
echo -e "\n=== Java and GraalVM Versions ==="
java --version
native-image --version

# Create a simple test to trace class initialization
echo -e "\n=== Tracing Class Initialization ==="
echo "Tracing java.util.Random initialization..."
native-image --no-fallback \
    --initialize-at-build-time \
    --trace-class-initialization=java.util.Random \
    -H:+PrintClassInitialization \
    -H:+ReportExceptionStackTraces \
    --dry-run \
    -jar target/chrondb-*-standalone.jar \
    diagnostic-test

echo -e "\n=== Checking for Conflicting Initialization Settings ==="
echo "Looking for classes that might have conflicting initialization settings..."

# List all classes that are explicitly initialized at runtime
echo -e "\nClasses explicitly initialized at runtime in build-native-image.sh:"
grep -o "\-\-initialize-at-run-time=[^ ]*" scripts/build-native-image.sh | sort

# Check if any of these classes are also initialized at build time elsewhere
echo -e "\nChecking for potential conflicts..."
for class in $(grep -o "\-\-initialize-at-run-time=[^ ]*" scripts/build-native-image.sh | cut -d= -f2); do
    echo "Checking $class..."
    # This is a simplified check - in a real scenario, you'd need more sophisticated analysis
    if grep -q "$class.*build-time" scripts/* 2>/dev/null; then
        echo "POTENTIAL CONFLICT: $class might be initialized at both build time and run time"
    fi
done

echo -e "\n=== Recommendations ==="
echo "1. Make sure java.util.Random is consistently initialized (either at build time or run time, not both)"
echo "2. Check for any conflicting initialization directives in your build scripts"
echo "3. Consider using --trace-class-initialization=<problematic-class> to debug specific issues"
echo "4. Ensure your GraalVM version is compatible with your dependencies"

echo -e "\n=== Diagnostic Complete ==="

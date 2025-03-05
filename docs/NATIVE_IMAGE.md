# ChronDB Native Image

This document explains how to build and use the GraalVM Native Image for ChronDB.

## Prerequisites

1. Install GraalVM (version 22.3.0 or later recommended)
   - Download from: <https://www.graalvm.org/downloads/>
   - Set `JAVA_HOME` to point to your GraalVM installation

2. Install the `native-image` component:

   ```
   gu install native-image
   ```

3. Make sure you have the Clojure CLI tools installed:
   - <https://clojure.org/guides/install_clojure>

4. For macOS users, install Xcode Command Line Tools:

   ```
   xcode-select --install
   ```

## Building the Native Image

You can build the native image using the provided script:

```bash
./scripts/build-native-image.sh [options]
```

### Build Options

The build script supports the following options:

- `--verbose`: Enable verbose output during the build process
- `--static`: Build a statically linked executable (useful for containerized deployments)
- `--output NAME`: Specify a custom name for the output executable (default: "chrondb")
- `--java-home PATH`: Specify the path to your GraalVM installation
- `--extra-flags '[flags]'`: Specify additional flags to pass to the native-image tool

Examples:

```bash
# Build with verbose output
./scripts/build-native-image.sh --verbose

# Build a statically linked executable
./scripts/build-native-image.sh --static

# Build with a custom name
./scripts/build-native-image.sh --output chrondb-server

# Specify a custom GraalVM installation
./scripts/build-native-image.sh --java-home /path/to/graalvm

# Combine options
./scripts/build-native-image.sh --verbose --static --output chrondb-static
```

### macOS with Nix Environment

If you're using macOS with Nix, we provide specialized scripts that include optimizations for this environment:

```bash
# Basic macOS/Nix script (bypasses toolchain check)
./scripts/build-native-image-macos-nix.sh [options]

# Enhanced macOS/Nix script (handles libz library path issues)
./scripts/build-native-image-macos-nix-with-libz.sh [options]
```

These scripts automatically add the `-H:-CheckToolchain` flag to bypass toolchain detection issues common in Nix environments on macOS. The enhanced script also automatically finds and configures the path to the zlib library to avoid linker errors.

Options for these scripts:

- `--verbose`: Enable verbose output
- `--static`: Build a statically linked executable
- `--output NAME`: Specify a custom name for the output executable

Example:

```bash
# Basic script
./scripts/build-native-image-macos-nix.sh --verbose --output chrondb-macos

# Enhanced script with libz handling
./scripts/build-native-image-macos-nix-with-libz.sh --verbose --output chrondb-macos
```

The build process will:

1. Build an uberjar with all dependencies
2. Use GraalVM's native-image tool to compile the uberjar into a native executable

The build process may take several minutes depending on your hardware.

## Manual Build Process

If you prefer to build manually:

1. Build the uberjar:

   ```
   clojure -T:build uber
   ```

2. Build the native image with options:

   ```
   # Basic build
   clojure -T:build native-image

   # With options
   clojure -T:build native-image '{:verbose true, :static true, :output "chrondb-custom"}'
   ```

## Running the Native Image

After building, you'll have a `chrondb` executable (or your custom named executable) in the project root directory.

Run it with:

```bash
./chrondb [options]
```

The native image supports the same command-line options as the regular ChronDB:

- First non-flag argument: HTTP port (default: 3000)
- Second non-flag argument: Redis port (default: 6379)
- `--disable-redis`: Disable Redis server
- `--disable-rest`: Disable REST API server

Examples:

```bash
# Run with default settings
./chrondb

# Run with custom ports
./chrondb 8080 6380

# Run only REST API (no Redis)
./chrondb --disable-redis

# Run only Redis server (no REST API)
./chrondb --disable-rest
```

## Benefits of Native Image

- Faster startup time
- Lower memory footprint
- Standalone executable with no JVM required
- Improved performance for certain workloads

## Troubleshooting

If you encounter issues with the native image:

1. Check that you're using a compatible GraalVM version
2. Ensure all required reflection configurations are in place
3. Try running with the `--verbose` flag to see more detailed output:

   ```
   ./scripts/build-native-image.sh --verbose
   ```

4. If you encounter class initialization errors, you may need to add more classes to the reflection configuration

### Nix Environment Issues

If you're using Nix and encounter toolchain detection errors like:

```
Error: Unable to detect supported DARWIN native software development toolchain.
```

You have several options:

1. Use the specialized script for macOS with Nix:

   ```
   ./scripts/build-native-image-macos-nix.sh
   ```

2. Add the `-H:-CheckToolchain` flag manually:

   ```
   ./scripts/build-native-image.sh --extra-flags '[-H:-CheckToolchain]'
   ```

3. Make sure you have Xcode Command Line Tools installed:

   ```
   xcode-select --install
   ```

4. Specify the correct `JAVA_HOME` when building:

   ```
   ./scripts/build-native-image.sh --java-home /path/to/graalvm
   ```

5. If you're still having issues, you can use the Docker-based build which avoids local toolchain issues:

   ```
   ./scripts/docker-build.sh
   ```

### Common Errors and Solutions

#### Toolchain Detection Error

**Error:**

```
Error: Unable to detect supported DARWIN native software development toolchain.
```

**Solution:**
The build has been configured to skip toolchain checking by adding the `-H:-CheckToolchain` flag. If you're still seeing this error, try one of these approaches:

1. Use the macOS/Nix specific script: `./scripts/build-native-image-macos-nix.sh`
2. Add the flag manually: `./scripts/build-native-image.sh --extra-flags '[-H:-CheckToolchain]'`
3. Use Docker: `./scripts/docker-build.sh`

#### Class Not Found Errors

If you encounter errors about classes not being found, you may need to add them to the reflection configuration in `graalvm-config/reflect-config/reflect-config.json`.

#### Resource Not Found Errors

If resources are not being included in the native image, add their patterns to `graalvm-config/resource-config/resource-config.json`.

#### Missing Library Errors

**Error:**

```
ld: library not found for -lz
clang: error: linker command failed with exit code 1 (use -v to see invocation)
```

**Solution:**
This error indicates that the linker cannot find the zlib compression library (`libz`). This is common in Nix environments where libraries may not be in standard locations. Try one of these approaches:

1. Use the enhanced script for macOS with Nix that automatically handles libz path issues:

   ```bash
   ./scripts/build-native-image-macos-nix-with-libz.sh
   ```

   This script:
   - Automatically finds the libz library in the Nix store
   - Sets the `LIBRARY_PATH` environment variable to include the directory with libz
   - Passes the library path directly to the linker using the `-H:NativeLinkerOption=-L/path/to/libz` flag
   - Works around the common linking issues in Nix environments

2. Install zlib development libraries:

   ```bash
   # On macOS with Homebrew
   brew install zlib

   # On Nix
   nix-env -i zlib
   ```

3. Specify the library path explicitly:

   ```bash
   # Find where libz is installed
   find /nix/store -name "libz.dylib" | grep -v i686

   # Then add the directory to the library path and pass it to the linker
   LIBZ_DIR=$(dirname $(find /nix/store -name "libz.dylib" | grep -v i686 | head -n 1))
   export LIBRARY_PATH=$LIBZ_DIR:$LIBRARY_PATH
   ./scripts/build-native-image.sh --extra-flags "[-H:-CheckToolchain, -H:NativeLinkerOption=-L$LIBZ_DIR]"
   ```

4. Use Docker to build instead, which avoids local library path issues:

   ```bash
   ./scripts/docker-build.sh
   ```

#### Reflection Issues

**Error:**

```
Exception in thread "main" java.lang.IllegalArgumentException: No matching ctor found for class org.eclipse.jetty.server.Server
```

**Solution:**
This error occurs because GraalVM native-image requires explicit reflection configuration for classes that are instantiated dynamically at runtime. The Jetty server used by Ring falls into this category.

1. Make sure your `graalvm-config/reflect-config/reflect-config.json` file includes the necessary Jetty classes:

   ```json
   [
     // ... existing entries ...
     {
       "name": "org.eclipse.jetty.server.Server",
       "allDeclaredConstructors": true,
       "allPublicConstructors": true,
       "allDeclaredMethods": true,
       "allPublicMethods": true
     },
     {
       "name": "org.eclipse.jetty.server.ServerConnector",
       "allDeclaredConstructors": true,
       "allPublicConstructors": true,
       "allDeclaredMethods": true,
       "allPublicMethods": true
     },
     // ... other Jetty classes ...
   ]
   ```

2. Rebuild the native image after updating the reflection configuration:

   ```bash
   ./scripts/build-native-image.sh --verbose
   ```

3. If you're still encountering reflection issues, you can use the GraalVM tracing agent to automatically generate reflection configurations:

   ```bash
   # Run your application with the tracing agent
   java -agentlib:native-image-agent=config-output-dir=graalvm-config \
        -jar target/chrondb-standalone.jar

   # Use the application to exercise all code paths
   # Then stop the application and use the generated configs
   ```

4. For complex web applications, consider using the Docker build which includes a more comprehensive reflection configuration:

   ```bash
   ./scripts/docker-build.sh
   ```

## Docker Support

ChronDB provides Docker support for building and running the native image in a container. This approach avoids local toolchain and library issues by using a consistent build environment.

### Building the Docker Image

The `docker-build.sh` script automates the Docker build process:

```bash
# Build the Docker image with default tag (latest)
./scripts/docker-build.sh

# Build with a specific tag
./scripts/docker-build.sh --tag v0.1.0
```

The script:

1. Creates a multi-stage Dockerfile if it doesn't exist
2. Uses GraalVM in the build stage to compile the native image
3. Installs all necessary dependencies including zlib
4. Creates a minimal runtime image based on Debian
5. Configures appropriate volumes and ports

### Running the Docker Container

You can build and run the container in one step:

```bash
# Build and run the container with default settings
./scripts/docker-build.sh --run

# Customize the container
./scripts/docker-build.sh --run --name my-chrondb --http-port 8080 --redis-port 6380 --data-dir /path/to/data
```

Available options:

- `--tag TAG`: Specify the Docker image tag (default: "latest")
- `--run`: Run the container after building
- `--name NAME`: Specify the container name (default: "chrondb")
- `--http-port PORT`: Specify the HTTP port to expose (default: 3000)
- `--redis-port PORT`: Specify the Redis port to expose (default: 6379)
- `--data-dir DIR`: Specify the local directory to mount for data (default: "./data")

### Manual Docker Commands

If you prefer to use Docker commands directly:

```bash
# Build the image
docker build -t chrondb/chrondb:latest -f Dockerfile.native .

# Run the container
docker run -d \
  --name chrondb \
  -p 3000:3000 \
  -p 6379:6379 \
  -v ./data:/data \
  chrondb/chrondb:latest
```

### Docker Compose

You can also use Docker Compose to run ChronDB. Create a `docker-compose.yml` file:

```yaml
version: '3'
services:
  chrondb:
    build:
      context: .
      dockerfile: Dockerfile.native
    ports:
      - "3000:3000"
      - "6379:6379"
    volumes:
      - ./data:/data
    command: []  # Add any command line arguments here
```

Then run:

```bash
docker-compose up -d
```

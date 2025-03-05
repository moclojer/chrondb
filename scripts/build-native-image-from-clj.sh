#!/bin/bash
# Build script that uses the native-image function from build.clj
# This script leverages the Clojure build tooling directly

set -e

# Default options
VERBOSE=true
STATIC=false
OUTPUT_NAME=${OUTPUT_NAME:-"chrondb"}
EXTRA_FLAGS=()

# Use o JAVA_HOME definido ou mantenha o valor atual
# No GitHub Actions, o JAVA_HOME já estará configurado corretamente
if [ -z "$JAVA_HOME" ]; then
    echo "JAVA_HOME is not set, using default GraalVM path"
    JAVA_HOME=/nix/store/44rl2mxaqh7i0qbmlc1zjgfcw9dkr65a-graalvm-oracle-22
fi

export JAVA_HOME
export PATH=$JAVA_HOME/bin:$PATH

# Simplificando as flags - o graal-build-time vai cuidar da inicialização das classes
EXTRA_FLAGS+=("--no-fallback")
EXTRA_FLAGS+=("--allow-incomplete-classpath")
EXTRA_FLAGS+=("-Dlucene.tests.security.manager=false")
EXTRA_FLAGS+=("-Dlucene.tests.fail.on.unsupported.codec=false")
EXTRA_FLAGS+=("--initialize-at-run-time=org.apache.lucene.internal.tests.TestSecrets")

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
        # Tratar as flags extras como uma string única ou como múltiplos argumentos
        if [[ "$2" == "--"* ]]; then
            # Se começa com --, é uma única flag
            EXTRA_FLAGS+=("$2")
            shift 2
        else
            # Caso contrário, dividir a string em múltiplas flags
            IFS=',' read -ra FLAGS <<< "$2"
            for flag in "${FLAGS[@]}"; do
                EXTRA_FLAGS+=("$flag")
            done
            shift 2
        fi
        ;;
    *)
        echo "Unknown option: $1"
        echo "Usage: $0 [--static] [--output NAME] [--extra-flags '[flag1,flag2,...]']"
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

# Primeiro, construir o JAR usando a tarefa 'uber'
echo "Building uber JAR using build.clj..."
clojure -T:build uber

# Verificar se o JAR foi criado com sucesso
if [ ! -f "target/chrondb-chrondb-*.jar" ]; then
    # Tentar encontrar o JAR gerado
    JAR_FILE=$(find target -name "*.jar" | head -n 1)
    if [ -z "$JAR_FILE" ]; then
        echo "Failed to build JAR file. Exiting."
        exit 1
    else
        echo "Found JAR file: $JAR_FILE"
    fi
fi

# Em seguida, construir a imagem nativa
echo "Building native image using build.clj..."

# Construir os parâmetros como uma string EDN válida para Clojure
PARAMS="{:verbose $VERBOSE"
PARAMS="$PARAMS :static $STATIC"
PARAMS="$PARAMS :output \"$OUTPUT_NAME\""

# Adicionar extra_flags se houver
if [ ${#EXTRA_FLAGS[@]} -gt 0 ]; then
    PARAMS="$PARAMS :extra_flags ["
    for flag in "${EXTRA_FLAGS[@]}"; do
        PARAMS="$PARAMS \"$flag\" "
    done
    PARAMS="$PARAMS]"
fi

# Fechar o mapa de parâmetros
PARAMS="$PARAMS}"

# Print environment variables for debugging
echo "Environment variables:"
echo "JAVA_HOME=$JAVA_HOME"
echo "LIBRARY_PATH=$LIBRARY_PATH"
echo "DYLD_LIBRARY_PATH=$DYLD_LIBRARY_PATH"
echo "LD_LIBRARY_PATH=$LD_LIBRARY_PATH"

# Execute the command
echo "Executing clojure with params: $PARAMS"
clojure -A:build -T:build native-image "$PARAMS"

echo "Native image build completed. You can run it with: ./$OUTPUT_NAME"

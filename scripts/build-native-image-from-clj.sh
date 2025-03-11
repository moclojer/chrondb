#!/bin/bash
# Build script that uses the native-image function from build.clj
# This script leverages the Clojure build tooling directly

set -e
set -x  # Imprimir comandos para facilitar o debug

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

echo "======== ENVIRONMENT INFO ========"
echo "Running as user: $(whoami)"
echo "Java version: $(java -version 2>&1)"
echo "GraalVM version: $(native-image --version 2>&1 || echo 'GraalVM native-image not available')"
echo "Working directory: $(pwd)"
echo "=================================="

# Verificar se a biblioteca libz está instalada
if [[ "$(uname)" == "Linux" ]]; then
    echo "Verificando bibliotecas instaladas no Linux:"
    ldconfig -p | grep libz || echo "libz não encontrado via ldconfig"
    ls -la /usr/lib*/libz* /lib*/libz* 2>/dev/null || echo "libz não encontrado nos diretórios padrão"
elif [[ "$(uname)" == "Darwin" ]]; then
    echo "Verificando bibliotecas instaladas no macOS:"
    ls -la /usr/lib/libz* 2>/dev/null || echo "libz não encontrado em /usr/lib"
fi

# Adicionar diretórios de bibliotecas para o processo de linkagem
if [[ "$(uname)" == "Linux" ]]; then
    export LD_LIBRARY_PATH="/usr/lib/x86_64-linux-gnu:/lib/x86_64-linux-gnu:/usr/lib:/lib:$LD_LIBRARY_PATH"
    export LIBRARY_PATH="/usr/lib/x86_64-linux-gnu:/lib/x86_64-linux-gnu:/usr/lib:/lib:$LIBRARY_PATH"

    # Adicionar cada diretório de biblioteca como uma flag separada
    EXTRA_FLAGS+=("-H:CLibraryPath=/usr/lib/x86_64-linux-gnu")
    EXTRA_FLAGS+=("-H:CLibraryPath=/lib/x86_64-linux-gnu")
    EXTRA_FLAGS+=("-H:CLibraryPath=/usr/lib")
    EXTRA_FLAGS+=("-H:CLibraryPath=/lib")
fi

# Find libdl on Linux for dynamic linking support
if [[ "$(uname)" == "Linux" ]]; then
    echo "Detected Linux, searching for libdl..."
    LIBDL_PATH=$(find /usr/lib* /lib* -name "libdl.so*" 2>/dev/null | head -n 1)

    if [[ -n "$LIBDL_PATH" ]]; then
        echo "Found libdl at: $LIBDL_PATH"
        LIBDL_DIR=$(dirname "$LIBDL_PATH")
        echo "Setting LD_LIBRARY_PATH to include: $LIBDL_DIR"
        export LD_LIBRARY_PATH="$LIBDL_DIR:$LD_LIBRARY_PATH"
        # Add extra flag to ensure the linker can find libdl
        EXTRA_FLAGS+=("-H:CLibraryPath=$LIBDL_DIR")
    fi

    # Add specific flags to handle JDK internal class initialization issues
    EXTRA_FLAGS+=("--enable-monitoring")
    EXTRA_FLAGS+=("-H:+AddAllCharsets")
    EXTRA_FLAGS+=("-H:+JNI")
    EXTRA_FLAGS+=("--no-fallback")
fi

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
        # Add extra flag to ensure the linker can find libz
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
        # Add extra flag to ensure the linker can find libz
        EXTRA_FLAGS+=("-H:CLibraryPath=$LIBZ_DIR")
    else
        echo "Could not find libz.so, will rely on system defaults"

        # Em ambientes Docker/CI, podemos precisar instalar libz
        if [ -f "/etc/apt/sources.list" ]; then
            echo "Tentando instalar zlib1g-dev via apt-get..."
            apt-get update || sudo apt-get update
            apt-get install -y zlib1g-dev || sudo apt-get install -y zlib1g-dev
        fi
    fi
fi

# Create necessary directories
mkdir -p reports
mkdir -p target

# Compilar biblioteca JNI para resolver o problema do ScopedMemoryAccess
echo "Compilando biblioteca JNI..."
./scripts/compile-jni.sh || {
    echo "AVISO: Falha ao compilar biblioteca JNI. Tentando continuar..."
}

# Capturar o caminho JNI
JNI_DIR="$(pwd)/target/jni"
if [ -d "$JNI_DIR" ]; then
    echo "Diretório JNI encontrado: $JNI_DIR"
    # Adicionar biblioteca JNI às flags
    if [ "$(uname)" == "Linux" ]; then
        # Configuração para Linux
        LIBRARY_PATH="$JNI_DIR:$LIBRARY_PATH"
        LD_LIBRARY_PATH="$JNI_DIR:$LD_LIBRARY_PATH"
        export LIBRARY_PATH LD_LIBRARY_PATH

        # Adicionar a flag para o linker
        if [ -f "$JNI_DIR/libscopedmemory.so" ]; then
            echo "Biblioteca JNI encontrada: $JNI_DIR/libscopedmemory.so"
            EXTRA_FLAGS+=("-H:CLibraryPath=$JNI_DIR")
        fi
    elif [ "$(uname)" == "Darwin" ]; then
        # Configuração para macOS
        DYLD_LIBRARY_PATH="$JNI_DIR:$DYLD_LIBRARY_PATH"
        LIBRARY_PATH="$JNI_DIR:$LIBRARY_PATH"
        export DYLD_LIBRARY_PATH LIBRARY_PATH

        # Adicionar a flag para o linker
        if [ -f "$JNI_DIR/libscopedmemory.dylib" ]; then
            echo "Biblioteca JNI encontrada: $JNI_DIR/libscopedmemory.dylib"
            EXTRA_FLAGS+=("-H:CLibraryPath=$JNI_DIR")
        fi
    fi

    # Adicionar mais flags específicas para lidar com o problema do ScopedMemoryAccess
    EXTRA_FLAGS+=("--initialize-at-run-time=jdk.internal.misc.ScopedMemoryAccess")
    EXTRA_FLAGS+=("--report-unsupported-elements-at-runtime")
else
    echo "AVISO: Diretório JNI não encontrado: $JNI_DIR"
fi

# Primeiro, construir o JAR usando a tarefa 'uber'
echo "Building uber JAR using build.clj..."
clojure -T:build uber || {
    echo "Falha ao construir o JAR. Verificando ambiente..."
    clojure -Spath
    ls -la target/
    exit 1
}

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

# Detectar se estamos em um ambiente CI e usar configurações simplificadas
if [ -n "$CI" ] || [ -n "$GITHUB_ACTIONS" ]; then
    echo "Ambiente CI detectado, usando configuração simplificada"
    USE_SIMPLIFIED=true
else
    USE_SIMPLIFIED=false
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
echo "LIBZ_PATH=$LIBZ_PATH"

# Set Memory Configuration for GraalVM
export JAVA_OPTS="-Xmx8g -Xms2g"
# Configurações simplificadas
export JAVA_TOOL_OPTIONS="-Djava.awt.headless=true"

# Add configs
EXTRA_FLAGS+=("-H:JNIConfigurationFiles=graalvm-config/jni-config.json")
EXTRA_FLAGS+=("-H:+AllowIncompleteClasspath")

# Execute the command
echo "Executing clojure with params: $PARAMS"
if [ "$USE_SIMPLIFIED" = true ]; then
    echo "Usando diretamente configuração simplificada devido ao ambiente CI"
    SIMPLE_PARAMS="{:verbose true :output \"$OUTPUT_NAME\" :simplified true}"
    echo "Parâmetros simplificados: $SIMPLE_PARAMS"
    clojure -A:build -T:build native-image "$SIMPLE_PARAMS"
else
    clojure -A:build -T:build native-image "$PARAMS" || {
        echo "Falha ao construir a imagem nativa. Tentando com configuração simplificada..."
        # Usar modo simplificado
        SIMPLE_PARAMS="{:verbose true :output \"$OUTPUT_NAME\" :simplified true}"
        echo "Tentando com configuração simplificada: $SIMPLE_PARAMS"
        clojure -A:build -T:build native-image "$SIMPLE_PARAMS"
    }
fi

# Verificar se a imagem nativa foi gerada
if [ -f "./$OUTPUT_NAME" ]; then
    echo "Native image built successfully. You can run it with: ./$OUTPUT_NAME"
    exit 0
else
    echo "Failed to build native image. Image file './$OUTPUT_NAME' not found."
    exit 1
fi

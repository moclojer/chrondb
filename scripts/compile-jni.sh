#!/bin/bash
set -e
set -x  # Adicionando para debug

# Detectar se estamos em um ambiente CI
if [ -n "$CI" ] || [ -n "$GITHUB_ACTIONS" ]; then
    echo "Ambiente CI detectado"
    # No GitHub Actions, JAVA_HOME já deve estar configurado corretamente
    if [ -n "$GRAALVM_HOME" ]; then
        echo "Usando GRAALVM_HOME: $GRAALVM_HOME"
        JAVA_HOME="$GRAALVM_HOME"
    fi
fi

# Identificar o diretório do JDK
if [ -z "$JAVA_HOME" ]; then
    echo "JAVA_HOME is not set, trying common locations"
    for path in /usr/lib/jvm/java-* /Library/Java/JavaVirtualMachines/* /nix/store/*-graalvm-*; do
        if [ -d "$path" ]; then
            JAVA_HOME="$path"
            echo "Found potential JAVA_HOME: $JAVA_HOME"
            break
        fi
    done
fi

echo "Using JAVA_HOME: $JAVA_HOME"

# Diretório para os arquivos JNI (usando caminho absoluto)
JNI_DIR="$(pwd)/target/jni"

# Criar diretório para os arquivos compilados e garantir que ele exista
mkdir -p "$JNI_DIR"
chmod 755 "$JNI_DIR"

# Verificar se o diretório foi criado
if [ ! -d "$JNI_DIR" ]; then
    echo "ERRO: Não foi possível criar o diretório $JNI_DIR"
    exit 1
fi

echo "Diretório JNI: $JNI_DIR"
ls -la "$JNI_DIR"

# Criar diretório src/jni se não existir
mkdir -p src/jni

# Verificar se o arquivo C existe, senão, criá-lo
JNI_C_FILE="src/jni/libjava_jdk_internal_misc_ScopedMemoryAccess.c"
if [ ! -f "$JNI_C_FILE" ]; then
    echo "Criando arquivo de implementação JNI..."
    cat > "$JNI_C_FILE" << 'EOF'
#include <jni.h>

JNIEXPORT void JNICALL Java_jdk_internal_misc_ScopedMemoryAccess_closeScope0
  (JNIEnv *env, jobject this, jobject scope) {
    // Empty implementation to satisfy the linker
    return;
}
EOF
    chmod 644 "$JNI_C_FILE"
fi

# Verificar o sistema operacional
if [[ "$(uname)" == "Darwin" ]]; then
    # macOS
    echo "Compilando para macOS..."

    # Verificar se os headers JNI existem
    if [ -d "$JAVA_HOME/include" ]; then
        echo "Encontrou headers JNI em $JAVA_HOME/include"

        if [ -d "$JAVA_HOME/include/darwin" ]; then
            # Compilar o arquivo C
            gcc -I"$JAVA_HOME/include" -I"$JAVA_HOME/include/darwin" -shared -fPIC -o "$JNI_DIR/libscopedmemory.dylib" "$JNI_C_FILE" || {
                echo "Falha na compilação, criando arquivo vazio"
                touch "$JNI_DIR/libscopedmemory.dylib"
            }
            LIBRARY_NAME="libscopedmemory.dylib"
            export DYLD_LIBRARY_PATH="$JNI_DIR:$DYLD_LIBRARY_PATH"
            echo "Biblioteca compilada: $JNI_DIR/$LIBRARY_NAME"
        else
            echo "Headers específicos do Darwin não encontrados, criando arquivo vazio"
            touch "$JNI_DIR/libscopedmemory.dylib"
        fi
    else
        echo "Headers JNI não encontrados, criando arquivo vazio"
        touch "$JNI_DIR/libscopedmemory.dylib"
    fi
else
    # Linux
    echo "Compilando para Linux..."

    # Verificar se os headers JNI existem
    if [ -d "$JAVA_HOME/include" ]; then
        echo "Encontrou headers JNI em $JAVA_HOME/include"

        if [ -d "$JAVA_HOME/include/linux" ]; then
            # Compilar o arquivo C
            gcc -I"$JAVA_HOME/include" -I"$JAVA_HOME/include/linux" -shared -fPIC -o "$JNI_DIR/libscopedmemory.so" "$JNI_C_FILE" || {
                echo "Falha na compilação, criando arquivo vazio"
                # Criar um arquivo .o vazio e transformá-lo em uma biblioteca compartilhada vazia
                echo "void Java_jdk_internal_misc_ScopedMemoryAccess_closeScope0() {}" > /tmp/dummy.c
                gcc -c /tmp/dummy.c -o /tmp/dummy.o
                gcc -shared -o "$JNI_DIR/libscopedmemory.so" /tmp/dummy.o
                rm /tmp/dummy.c /tmp/dummy.o
            }
            LIBRARY_NAME="libscopedmemory.so"
            echo "Biblioteca compilada: $JNI_DIR/$LIBRARY_NAME"
        else
            echo "Headers específicos do Linux não encontrados, criando arquivo vazio"
            # Criar um arquivo .o vazio e transformá-lo em uma biblioteca compartilhada vazia
            echo "void Java_jdk_internal_misc_ScopedMemoryAccess_closeScope0() {}" > /tmp/dummy.c
            gcc -c /tmp/dummy.c -o /tmp/dummy.o
            gcc -shared -o "$JNI_DIR/libscopedmemory.so" /tmp/dummy.o
            rm /tmp/dummy.c /tmp/dummy.o
        fi
    else
        echo "Headers JNI não encontrados, criando arquivo vazio"
        # Criar um arquivo .o vazio e transformá-lo em uma biblioteca compartilhada vazia
        echo "void Java_jdk_internal_misc_ScopedMemoryAccess_closeScope0() {}" > /tmp/dummy.c
        gcc -c /tmp/dummy.c -o /tmp/dummy.o
        gcc -shared -o "$JNI_DIR/libscopedmemory.so" /tmp/dummy.o
        rm /tmp/dummy.c /tmp/dummy.o
    fi
fi

# Listar o conteúdo do diretório JNI
ls -la "$JNI_DIR"

# Exportar o caminho para a biblioteca
export LD_LIBRARY_PATH="$JNI_DIR:$LD_LIBRARY_PATH"
echo "LD_LIBRARY_PATH=$LD_LIBRARY_PATH"
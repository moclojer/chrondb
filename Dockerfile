FROM ghcr.io/graalvm/native-image:ol9-java17-22.3.2 AS builder

ENV CLOJURE_VERSION=1.12.3.1577 \
    PATH="/usr/local/bin:/usr/bin:/bin:/usr/local/graalvm-ce-java17-22.3.2/bin:$PATH"

RUN microdnf install -y curl unzip tar gzip git && microdnf clean all && \
    curl -L -o /tmp/clojure-install.sh "https://download.clojure.org/install/linux-install-${CLOJURE_VERSION}.sh" && \
    chmod +x /tmp/clojure-install.sh && /tmp/clojure-install.sh && \
    rm /tmp/clojure-install.sh

WORKDIR /app

COPY deps.edn .
COPY dev dev
COPY . .

RUN clojure -M:build -- --uberjar && \
    native-image @target/native-image-args -H:Name=chrondb

FROM debian:12-slim AS runtime

RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates libstdc++6 && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app/chrondb /usr/local/bin/chrondb
COPY --from=builder /app/resources ./resources

RUN mkdir -p /app/data && \
    chown -R nobody:nogroup /app

EXPOSE 3000 6379 5432

# rodamos como usuário restrito padrão do Debian (UID 65532)
USER nobody

ENTRYPOINT ["/usr/local/bin/chrondb"]

# ChronDB Operations Guide

This guide covers operational aspects of ChronDB, including installation, execution, monitoring, and maintenance.

## Installation

### Requirements

- Java 11+
- Git 2.25.0+
- 1GB+ RAM (recommended)
- Disk space proportional to data volume and history

### Installation via JAR

1. Download the latest JAR file from the releases page:

   ```
   curl -LO https://github.com/moclojer/chrondb/releases/latest/download/chrondb.jar
   ```

2. Run the JAR:

   ```
   java -jar chrondb.jar
   ```

### Manual Compilation

1. Clone the repository:

   ```
   git clone https://github.com/moclojer/chrondb.git
   cd chrondb
   ```

2. Compile the project:

   ```
   clojure -T:build uber
   ```

3. Run the generated JAR:

   ```
   java -jar target/chrondb.jar
   ```

## Running ChronDB

### Command Line Options

```
java -jar chrondb.jar [options]

Options:
  --config FILE         Path to configuration file
  --data-dir DIR        Directory for data storage
  --http-port PORT      Port for HTTP REST server
  --redis-port PORT     Port for Redis protocol server
  --pg-port PORT        Port for PostgreSQL protocol server
  --log-level LEVEL     Log level (debug, info, warn, error)
  --memory-limit MB     Memory limit in MB
```

### As a Systemd Service

1. Create a service file `/etc/systemd/system/chrondb.service`:

   ```
   [Unit]
   Description=ChronDB Chronological Database
   After=network.target

   [Service]
   User=chrondb
   Group=chrondb
   ExecStart=/usr/bin/java -jar /opt/chrondb/chrondb.jar --config /etc/chrondb/config.edn
   Restart=on-failure

   [Install]
   WantedBy=multi-user.target
   ```

2. Reload systemd:

   ```
   sudo systemctl daemon-reload
   ```

3. Start the service:

   ```
   sudo systemctl start chrondb
   ```

## Monitoring

ChronDB exposes metrics and health information for monitoring.

### Health Check Endpoint

```
GET /api/v1/health
```

Response:

```json
{
  "status": "healthy",
  "version": "0.1.0",
  "uptime": "10h 30m",
  "memory": {
    "used": "256MB",
    "total": "1024MB"
  },
  "storage": {
    "size": "500MB",
    "documents": 1250
  }
}
```

### Logs

Logs contain detailed information about operations and errors. By default, they are sent to stdout or a file as configured.

### Prometheus Integration

ChronDB exposes metrics in Prometheus format at:

```
GET /metrics
```

## Backup and Restoration

By using Git internally, ChronDB has native backup capabilities.

### GraalVM Native Image

ChronDB pode ser distribuído como binário compilado via GraalVM:

1. Instale GraalVM com o componente `native-image`
2. Rode `clojure -M:build -- --uberjar` para gerar `target/chrondb.jar` e arquivos auxiliares
3. Execute `native-image @target/native-image-args -jar target/chrondb.jar -o target/chrondb_local`
4. Teste o binário local: `./target/chrondb_local`

O workflow `build-native-image.yml` (GitHub Actions) executa o mesmo processo e publica os artefatos por sistema operacional.

### Backup

1. Traditional backup:

   ```
   java -jar chrondb.jar --command backup --output /path/to/backup.tar.gz
   ```

2. Using Git directly:

   ```
   cd /path/to/data-dir
   git bundle create /path/to/backup.bundle --all
   ```

### Restoration

1. Traditional restoration:

   ```
   java -jar chrondb.jar --command restore --input /path/to/backup.tar.gz
   ```

2. Using Git directly:

   ```
   mkdir -p /path/to/new-data-dir
   cd /path/to/new-data-dir
   git clone /path/to/backup.bundle .
   ```

## Maintenance

### Compaction

To optimize performance and space usage:

```
java -jar chrondb.jar --command compact
```

This operation:

- Runs git gc to optimize the repository
- Rebuilds indices to improve search performance
- Optimizes the internal structure

### Updating

1. Stop the ChronDB service
2. Backup your data
3. Replace the JAR file with the new version
4. Start the service again

### Branch Management

Unused branches can be removed:

```
java -jar chrondb.jar --command remove-branch --name test-branch
```

### Disk Space Monitoring

ChronDB will grow over time due to the historical storage nature. Monitor disk usage and configure retention policies if necessary.

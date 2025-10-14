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

   ```sh
   curl -LO https://github.com/moclojer/chrondb/releases/latest/download/chrondb.jar
   ```

2. Run the JAR:

   ```sh
   java -jar chrondb.jar
   ```

### Manual Compilation

1. Clone the repository:

   ```sh
   git clone https://github.com/moclojer/chrondb.git
   cd chrondb
   ```

2. Compile the project:

   ```sh
   clojure -T:build uber
   ```

3. Run the generated JAR:

   ```sh
   java -jar target/chrondb.jar
   ```

## Running ChronDB

### Command Line Options

```sh
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

   ```toml
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

   ```sh
   sudo systemctl daemon-reload
   ```

3. Start the service:

   ```sh
   sudo systemctl start chrondb
   ```

## Monitoring

ChronDB exposes metrics and health information for monitoring.

### Health Check Endpoint

```http
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

```http
GET /metrics
```

## Backup and Restoration

ChronDB usa Git internamente, então cada gravação gera commits imutáveis. A partir desta versão há suporte oficial a backup/restore completos e incrementais, tanto via CLI quanto via REST.

### Backup (CLI)

- Full tar.gz (default):

  ```bash
  clojure -M:run backup --output backups/full-2025-10-12.tar.gz
  ```

- Full bundle:

  ```bash
  clojure -M:run backup --output backups/full-main.bundle --format bundle
  ```

- Incremental bundle (desde commit base):

  ```bash
  clojure -M:run backup --format bundle --output backups/incr.bundle --base-commit <full-commit>
  ```

### Restore (CLI)

- Tar.gz:

  ```bash
  clojure -M:run restore --input backups/full-2025-10-12.tar.gz
  ```

- Bundle:

  ```bash
  clojure -M:run restore --input backups/full-main.bundle --format bundle
  ```

### Export/Import bundle (CLI)

```bash
clojure -M:run export-snapshot --output backups/main.bundle --refs refs/heads/main
clojure -M:run import-snapshot --input backups/main.bundle
```

### Scheduler (CLI)

```bash
clojure -M:run schedule --mode full --interval 60 --output-dir backups/
clojure -M:run cancel-schedule --id <job-id>
clojure -M:run list-schedules
```

### Backup via REST

- POST `/api/v1/backup` — body JSON `{"output":"/path/to/full.tar.gz","format":"tar.gz"}`
- POST `/api/v1/export` — JSON `{"output":"/path/to/main.bundle","refs":["refs/heads/main"]}`

### Restore via REST

- POST `/api/v1/restore` com upload multipart (campo `file`) e campo opcional `format`
- POST `/api/v1/import` com upload multipart (`file`) e flag `verify`

As respostas retornam status HTTP e o manifesto com checksum, refs inclusas e tipo (:full, :incremental).

> **Dica:** Incrementais suportam apenas formato bundle; para reconstruir completamente, aplique primeiro o full e depois os incrementais na ordem.

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

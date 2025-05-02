# chrondb

_Chronological **key/value** Database storing based on database-shaped `git` (core) architecture and Lucene for indexing._

## Features

We are:

- Immutable and atomic data
- ACID transactions
- Schemaless
- Chronological
- **RESTfull server:** communication via HTTP protocol
- **Redis protocol:** use the Redis driver to communicate with chrondb
- **SQL compliance:** in query within document

Understand how and when changes were made. **chrondb** stores all history, and lets you query against any point in time.

[Git structure](https://git-scm.com/book/en/v2/Git-Internals-Git-Objects) is a powerful solution for storing **"data"** (files) in chronological order, _chrondb_ uses git core as a data structure to structure the data timeline, making it possible to return to any necessary point and bringing all git functions for a database:

- diff
- notes
- restore
- branch
- checkout
- revert
- merge
- log
- blame
- archive
- [hooks](https://git-scm.com/docs/githooks#_hooks)
- ... [git high-level commands (porcelain)](https://git-scm.com/docs/git#_high_level_commands_porcelain)

## Term alignment, from database to git

> The goal is to speak the same language as the database world

- **database:** _git_ repository (local or remotely)
- **scheme:** _git_ branch
- **table:** directory added on _git_ repository
- **field struct:** json (document) - will be persisted in a file and indexed in _lucene_

### Storage Structure

Documents are stored in a directory structure that follows these rules:

- Each table is a directory in the Git repository
- Documents belonging to a table are stored as JSON files inside that table's directory
- Document IDs are used as filenames (with proper encoding for special characters)
- Each document contains a `_table` field that identifies which table it belongs to

For example, a document with ID `123` in the `user` table would be stored at:

```
user/123.json
```

If a data directory is configured, the path would be:

```
{data-dir}/user/123.json
```

## Running

ChronDB can be run using the Clojure CLI with various options to customize its behavior.

### Basic Usage

```bash
# Run with default settings (HTTP on port 3000, Redis on port 6379)
clojure -M:run

# Run with custom ports
clojure -M:run 8080 6380
```

### Command Line Options

ChronDB supports the following command line options:

- First non-flag argument: HTTP port (default: 3000)
- Second non-flag argument: Redis port (default: 6379)
- `--disable-redis`: Disable the Redis protocol server
- `--disable-rest`: Disable the REST API server
- `--disable-sql`: Disable the SQL server

### Examples

```bash
# Run only the REST API server (no Redis)
clojure -M:run --disable-redis
# or use the dedicated alias
clojure -M:run-rest

# Run only the Redis protocol server (no REST API)
clojure -M:run --disable-rest
# or use the dedicated alias
clojure -M:run-redis

# Run with custom HTTP port and disabled Redis
clojure -M:run 8080 --disable-redis

# Run with both custom ports
clojure -M:run 8080 6380 5432
```

#### PostgreSQL

```bash
PGSSLMODE=disable psql \
  -h localhost \
  -p 5432 \
  -d chrondb \
  -U chrondb \
  -c "SELECT * FROM key-chrondb"
```

#### SQL History Functions

ChronDB provides SQL functions to access the version history of documents, allowing you to query and compare different versions directly through SQL:

```sql
-- Function to query the complete history of a document
-- Returns: commit_id, timestamp, committer, data
SELECT * FROM chrondb_history('table_name', 'document_id');

-- Function to query a document at a specific point (commit hash)
-- Returns the document as it was at that commit
SELECT * FROM chrondb_at('table_name', 'document_id', 'commit_hash');

-- Function to compare different versions of a document
-- Returns: id, commit1, commit2, added, removed, changed
SELECT * FROM chrondb_diff('table_name', 'document_id', 'commit_hash1', 'commit_hash2');
```

Example usage:

```sql
-- View the complete history of a user document
SELECT * FROM chrondb_history('user', '1');

-- Get user document as it was at a specific commit
SELECT * FROM chrondb_at('user', '1', 'abc123def456');

-- Compare user document between two versions
SELECT * FROM chrondb_diff('user', '1', 'abc123def456', 'def456abc123');
```

These functions make it easy to leverage ChronDB's versioning capabilities directly through standard SQL queries.

### Using the Functional Interface

You can also run ChronDB using the `-X` option:

```bash
# Run with default settings
clojure -X:run

# Run with custom arguments
clojure -X:run :args '["8080", "--disable-redis"]'
```

## Configuration

ChronDB uses an EDN configuration file to define its settings. To get started, copy the `config.example.edn` file to `config.edn`:

```bash
cp config.example.edn config.edn
```

### Configuration File Structure

The configuration file is divided into three main sections:

#### 1. Git Configuration (:git)

```clojure
:git {
  :committer-name "ChronDB"      ; Name used in commits
  :committer-email "chrondb@example.com"  ; Email used in commits
  :default-branch "main"         ; Default repository branch
  :sign-commits false            ; Whether to sign commits with GPG
}
```

#### 2. Storage Configuration (:storage)

```clojure
:storage {
  :data-dir "data"              ; Directory where data will be stored
}
```

#### 3. Logging Configuration (:logging)

```clojure
:logging {
  :level :info                  ; Log level (:debug, :info, :warn, :error)
  :output :stdout               ; Log output (:stdout or :file)
  :file "chrondb.log"          ; Log file (when output is :file)
}
```

### Log Levels

- `:debug` - Detailed information for development/debugging
- `:info` - General operation information
- `:warn` - Warnings that don't affect functionality
- `:error` - Errors that may affect functionality

### Usage Example

```clojure
(require '[chrondb.config :as config])

;; Load configuration from config.edn
(def chrondb-config (config/load-config))

;; Or specify a different file
(def custom-config (config/load-config "custom-config.edn"))
```

## Development

To run the example with default configuration:

```bash
clj -X:example
```

To run tests:

```bash
clj -X:test
```

## Installation

Add the following dependency to your `deps.edn`:

```clojure
{:deps {com.github.chrondb/chrondb {:git/tag "v0.1.0"
                                   :git/sha "..."}}}
```

## Quick Start

```clojure
(require '[chrondb.core :as chrondb])

;; Create a new ChronDB instance
(def db (chrondb/create-chrondb))

;; Save a document
(chrondb/save db "user:1" {:name "John" :age 30})

;; Get a document
(chrondb/get db "user:1")

;; Search documents
(chrondb/search db "name:John")

;; Get document history
(chrondb/history db "user:1")

;; Get document at specific point in time
(chrondb/get-at db "user:1" "2024-01-01T00:00:00Z")
```

## Requirements

- Java 11 or later
- Git 2.25.0 or later

## Documentation

For more detailed documentation, see:

- [Configuration Guide](docs/configuration.md)
- [API Reference](docs/api.md)
- [Example Usage](src/chrondb/example.clj)

For generated API documentation, run:

```bash
clj -X:codox
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.

## License

ChronDB is licensed under the terms of the GNU General Public License v3.0 (GPLv3).

This means:

- You are free to use, study, modify, and redistribute the software.
- Any distributed modified version must also be licensed under GPLv3.
- See the [LICENSE](./LICENSE) file for full details or visit [gnu.org/licenses/gpl-3.0](https://www.gnu.org/licenses/gpl-3.0.html).

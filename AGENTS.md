# ChronDB Agent Guide

> Instructions for LLM-assisted development of ChronDB.

## Project Overview

ChronDB is a **chronological key/value database** implemented in Clojure, backed by a Git-like storage engine. Every write produces an immutable commit, enabling full history, time-travel queries, and branching semantics.

### Key Characteristics

- **Immutable by design**: Documents are never updated in-place; writes create new Git commits
- **Full audit trail**: Git notes capture transaction metadata (origin, user, flags, correlation IDs)
- **Multi-protocol**: REST API, Redis RESP protocol, PostgreSQL wire protocol
- **Time-travel**: Query any point in history via commit selection
- **Branching**: Isolated environments via Git branches
- **FFI bindings**: Python and Rust clients via GraalVM native-image shared library

### Conceptual Mapping

| Git | Database |
|-----|----------|
| repository | database instance |
| branch | schema/environment |
| directory | table/collection |
| file (JSON) | document |
| commit | transaction |
| commit hash | transaction ID |
| tag | named snapshot |
| notes/chrondb | transaction metadata |

## Project Structure

```
chrondb/
├── src/chrondb/              # Main application (~15k LOC)
│   ├── core.clj              # Entry point (CLI/server dispatcher)
│   ├── config.clj            # Configuration loader
│   ├── api/                  # Protocol implementations
│   │   ├── v1.clj            # REST API handlers
│   │   ├── v1/routes.clj     # REST routing (Compojure)
│   │   ├── server.clj        # HTTP server (Ring/Jetty)
│   │   ├── redis/            # Redis RESP protocol
│   │   │   ├── core.clj      # Command handlers (GET, SET, DEL, KEYS...)
│   │   │   └── server.clj    # Socket server
│   │   └── sql/              # PostgreSQL wire protocol
│   │       ├── core.clj      # Server orchestration
│   │       ├── parser/       # SQL tokenizer, statement, clause parsers
│   │       ├── execution/    # Query planner, joins, aggregates
│   │       ├── protocol/     # Wire protocol encoding
│   │       └── schema/       # DDL (CREATE/ALTER/DROP TABLE)
│   ├── storage/              # Storage layer (protocol-based)
│   │   ├── protocol.clj      # Storage protocol definition
│   │   ├── git/              # Git-based implementation (JGit)
│   │   │   ├── core.clj      # Main storage record
│   │   │   ├── document.clj  # Save/get/delete operations
│   │   │   ├── commit.clj    # Git commit creation
│   │   │   ├── history.clj   # Document history (commit walk)
│   │   │   ├── notes.clj     # Transaction metadata in refs/notes/chrondb
│   │   │   ├── remote.clj    # Push/pull with SSH/HTTP
│   │   │   └── path.clj      # Path utilities
│   │   ├── memory.clj        # In-memory (testing)
│   │   └── durable.clj       # Durable wrapper
│   ├── index/                # Indexing layer (Lucene 9.8)
│   │   ├── protocol.clj      # Index protocol definition
│   │   ├── lucene.clj        # Apache Lucene implementation
│   │   ├── lucene_nrt.clj    # Near-Real-Time reader pool
│   │   ├── cache.clj         # Query result caching
│   │   └── memory.clj        # In-memory index
│   ├── query/                # Query AST builders
│   │   └── ast.clj           # term, wildcard, range, fts, and/or/not
│   ├── transaction/          # Transaction context
│   │   └── core.clj          # Dynamic binding for tx metadata
│   ├── wal/                  # Write-Ahead Logging
│   │   ├── core.clj          # WAL protocol & implementation
│   │   └── recovery.clj      # Startup recovery
│   ├── backup/               # Backup & restore
│   │   ├── core.clj          # High-level API
│   │   ├── git.clj           # Git bundle creation
│   │   ├── archive.clj       # TAR/GZIP handling
│   │   └── scheduler.clj     # Scheduled backups
│   ├── validation/           # JSON Schema validation
│   │   ├── core.clj          # Validation logic
│   │   ├── storage.clj       # Schema persistence
│   │   └── schema.clj        # Schema utilities
│   ├── cli/                  # Command-line interface
│   │   ├── core.clj          # CLI dispatcher
│   │   ├── server.clj        # Server commands
│   │   └── http.clj          # HTTP helpers
│   ├── observability/        # Monitoring
│   │   ├── health.clj        # K8s liveness/readiness probes
│   │   └── metrics.clj       # Counters, histograms
│   ├── concurrency/          # Concurrency control
│   │   └── occ.clj           # Optimistic concurrency
│   ├── temporal/             # Time-travel support
│   │   └── core.clj
│   ├── util/                 # Utilities
│   │   ├── logging.clj       # SLF4J logging
│   │   └── locks.clj         # Lock cleanup
│   ├── tools/                # Developer tools
│   │   ├── diagnose.clj      # Repository diagnostics
│   │   ├── dump.clj          # Data export
│   │   └── migrator.clj      # Data migration
│   └── lib/                  # FFI entry points
│       └── core.clj          # Native library exports
├── test/                     # Test suite (61 files)
│   └── chrondb/
│       ├── storage/git/      # Storage integration tests
│       ├── api/              # Protocol tests
│       ├── transaction/      # Transaction tests
│       ├── wal/              # WAL recovery tests
│       ├── query/            # AST tests
│       └── benchmark/        # Performance benchmarks
├── bindings/                 # Language bindings (FFI)
│   ├── python/               # Python client (ctypes)
│   │   ├── chrondb/
│   │   │   ├── client.py     # High-level API
│   │   │   └── _ffi.py       # FFI loader
│   │   └── tests/
│   └── rust/                 # Rust client
│       ├── src/
│       │   ├── lib.rs        # Safe wrapper
│       │   ├── ffi.rs        # FFI definitions
│       │   └── setup.rs      # Library setup
│       └── Cargo.toml
├── dev/                      # Build tooling
│   └── chrondb/
│       ├── build.clj         # Uberjar builder
│       ├── shared_library.clj # Native-image prep
│       └── native_image.clj  # GraalVM config
├── java/                     # Java/GraalVM integration
├── .github/workflows/        # CI/CD pipelines
├── docs/                     # Documentation
├── deps.edn                  # Clojure dependencies
├── config.example.edn        # Configuration template
└── Dockerfile                # Multi-stage Docker build
```

## Architecture

### Storage Layer

The storage layer is protocol-based (`storage/protocol.clj`):

```clojure
(defprotocol Storage
  (save-document [this id data] [this id data branch])
  (get-document [this id] [this id branch])
  (delete-document [this id] [this id branch])
  (get-documents-by-prefix [this prefix] [this prefix branch])
  (get-documents-by-table [this table] [this table branch])
  (get-document-history [this id] [this id branch])
  (close [this]))
```

**Git Implementation** (`storage/git/`):
- Documents stored as `table-name/document-id.json`
- Each save creates an atomic Git commit
- History via commit walk with diff extraction
- Transaction metadata in `refs/notes/chrondb`

### Index Layer

The index layer is protocol-based (`index/protocol.clj`):

```clojure
(defprotocol Index
  (index-document [this id data])
  (delete-document [this id])
  (search [this field query-string])
  (search-query [this query-map])
  (close [this]))
```

**Lucene Implementation** (`index/lucene.clj`):
- Lucene 9.8 for full-text and structured search
- Near-Real-Time reader pool for consistency
- Query AST: term, wildcard, range, prefix, fts, exists, geo
- Boolean combinations: and, or, not

### Transaction Context

Dynamic binding for metadata tracking (`transaction/core.clj`):

```clojure
(def ^:dynamic *transaction-context* (atom nil))

;; Context contains:
;; :tx_id, :origin, :user, :timestamp, :flags, :metadata, :status
```

Every write path must call transaction helpers to ensure Git notes capture metadata.

### Protocol Servers

All protocols share the same storage/index backend:

| Protocol | Port | Implementation |
|----------|------|----------------|
| HTTP REST | 3000 | Ring/Jetty + Compojure |
| Redis RESP | 6379 | Custom socket server |
| PostgreSQL | 5432 | Wire protocol parser |

### FFI Bindings

Both Python and Rust use a **shared isolate pattern** to avoid GraalVM file lock conflicts:

- Global registry keyed by `(data_path, index_path)`
- Multiple instances → single isolate, thread-safe
- Reference counting for cleanup
- **64MB stack** for Rust FFI worker (Lucene/JGit deep call chains)

## Code Style & Conventions

### Clojure

- **Naming**: `kebab-case` for vars, namespaces as `chrondb.*`
- **Pure functions**: Prefer data transformations over side effects
- **Docstrings**: Required for all public vars and functions
- **Indentation**: Enforced by `cljfmt`
- **Logging**: Descriptive, reflects chronological operations

### Error Handling

- **Fail fast**: Surface errors early with actionable messages
- **Structured logging**: Include context (what, where, data involved)
- **No silent failures**: Always propagate or log errors

### Immutability

- **Never rewrite commits**: Create new commits instead
- **Preserve history**: Every change is traceable
- **Transaction notes**: Always capture origin, user, flags

## Development

### Running the Server

```bash
# All protocols (REST + Redis + SQL)
clojure -M:run

# Individual protocols
clojure -M:run-rest   # HTTP only (port 3000)
clojure -M:run-redis  # Redis only (port 6379)
clojure -M:run-sql    # PostgreSQL only (port 5432)
```

### Configuration

Copy `config.example.edn` to `config.edn`:

```clojure
{:git {:committer-name "ChronDB"
       :committer-email "chrondb@example.com"
       :default-branch "main"
       :remote-url nil              ; e.g., "git@github.com:org/repo.git"
       :push-enabled true
       :push-notes true
       :pull-on-start true}
 :storage {:data-dir "data"}
 :logging {:level :info             ; :debug, :info, :warn, :error
           :output :stdout}}        ; :stdout, :file
```

### Testing

```bash
# Full test suite (sequential for protocol tests)
clojure -M:test

# Core logic only (faster, parallel)
clojure -M:test-non-external-protocol

# Individual protocol tests
clojure -M:test-redis-sequential
clojure -M:test-sql-only

# Benchmarks
clojure -M:benchmark
```

### Linting & Formatting

```bash
clojure -M:lint      # clj-kondo
clojure -M:fmt       # Check formatting
clojure -M:fmt-fix   # Fix formatting
```

### Building

```bash
# Uberjar
clojure -M:build -- --uberjar
# Output: target/chrondb.jar

# Native image (requires GraalVM)
clojure -M:shared-lib
native-image @target/shared-image-args -H:Name=libchrondb

# Docker
docker build -t avelino/chrondb .
docker run --rm -p 3000:3000 -p 6379:6379 -p 5432:5432 avelino/chrondb
```

## Key Dependencies

| Library | Version | Purpose |
|---------|---------|---------|
| org.clojure/clojure | 1.11.1 | Language |
| org.eclipse.jgit/org.eclipse.jgit | 6.7.0 | Git operations |
| org.apache.lucene/* | 9.8.0 | Full-text search |
| ring/ring-core | 1.10.0 | HTTP abstraction |
| compojure | 1.7.0 | Routing |
| com.networknt/json-schema-validator | 1.0.87 | JSON Schema |

## CI/CD Workflows

### Tests (`tests.yml`)
1. Setup GraalVM 17, Clojure
2. Run `test-non-external-protocol` (parallel)
3. Run `test-sql-only` (sequential)
4. Run `test-redis-sequential` (sequential)

### Shared Library (`build-shared-library.yml`)
1. Build on macOS-14 and Ubuntu-22.04
2. Generate `libchrondb.so` / `libchrondb.dylib`
3. Test Rust bindings with artifacts
4. Test Python bindings with artifacts

### Release (`release-shared-library.yml`)
1. Create GitHub release
2. Upload platform-specific artifacts
3. Publish Docker image to `ghcr.io`

## Instructions for Code Generation

### When Writing Code

1. **Respect immutability**: Never rewrite Git commits; create new ones
2. **Transaction context**: Always use `transaction/core` helpers for writes
3. **Git notes**: Ensure metadata (tx_id, origin, user, flags) is recorded
4. **Fail fast**: Return errors early with structured context
5. **Use protocols**: Extend `Storage` or `Index` for new backends
6. **Efficient data flows**: Prefer `transduce`, `reduce`, pipelines
7. **Concurrency**: Use atoms with `swap!`; guard shared state
8. **Lucene queries**: Route through `query/ast.clj` builders
9. **Profile Git-heavy operations**: Watch for performance regressions

### When Modifying Storage

- Preserve chronological ordering and causal relationships
- Handle JGit lock contention gracefully
- Surface meaningful errors (lock conflicts, missing refs)
- Test with concurrent access scenarios

### When Adding API Endpoints

- Maintain consistency across REST, Redis, SQL
- Validate inputs early; return protocol-appropriate errors
- Support `?branch=name` parameter
- Capture headers: `x-chrondb-origin`, `x-chrondb-user`, `x-request-id`
- Document in `docs/`

### When Modifying Index/Search

- Route through centralized Lucene planner
- Update `query/ast.clj` for new query types
- Maintain NRT reader consistency
- Test with various query patterns

## Instructions for Code Review

### Checklist

1. **Chronological correctness**: Time-based queries return correct snapshots?
2. **API parity**: Behavior consistent across REST, Redis, SQL?
3. **Test coverage**: Unit + integration tests for new behaviors?
4. **Git storage**: No performance regressions in storage operations?
5. **Transaction metadata**: Git notes capture all required fields?
6. **Clojure idioms**: kebab-case, pure functions, docstrings?
7. **Error handling**: Actionable messages with context?

### Red Flags

- Modifying commits in-place (violates immutability)
- Missing transaction context in write paths
- Bypassing Lucene planner with custom query engines
- Silent error swallowing
- Blocking operations without timeouts
- Missing tests for branching/time-travel scenarios

## FFI Bindings Development

### Architecture: Smart Wrapper, Dumb Core

> **Critical design principle**: `libchrondb.so/.dylib` is a **minimal, stateless core** that only knows how to execute ChronDB operations (Git + Lucene). It has **zero awareness** of OS-level concerns.

**What `libchrondb` does:**
- Git operations (commit, read, history, branches)
- Lucene indexing and queries
- JSON serialization/deserialization
- GraalVM isolate management (create/attach/detach)

**What `libchrondb` does NOT do:**
- Signal handling (SIGTERM, SIGINT, SIGHUP)
- Graceful shutdown orchestration
- Thread lifecycle management
- Resource cleanup on process exit
- Connection pooling or keepalive
- Retry logic or circuit breaking
- Timeout enforcement
- Memory pressure monitoring
- File descriptor management
- Lock recovery after crashes

### Binding Layer Responsibilities

Each language binding (Python, Rust, etc.) **MUST** implement a robust wrapper layer:

```
┌─────────────────────────────────────────────────────────────┐
│                    Application Code                         │
├─────────────────────────────────────────────────────────────┤
│                 Language Binding (Smart)                    │
│  ┌─────────────┬─────────────┬─────────────┬─────────────┐ │
│  │   Signal    │   Thread    │  Resource   │    Error    │ │
│  │  Handling   │   Mgmt      │   Cleanup   │   Recovery  │ │
│  ├─────────────┼─────────────┼─────────────┼─────────────┤ │
│  │  Lifecycle  │   Pooling   │  Timeouts   │   Retries   │ │
│  └─────────────┴─────────────┴─────────────┴─────────────┘ │
├─────────────────────────────────────────────────────────────┤
│                    FFI Bridge (Thin)                        │
├─────────────────────────────────────────────────────────────┤
│              libchrondb.so/.dylib (Dumb Core)               │
│                   Git + Lucene only                         │
└─────────────────────────────────────────────────────────────┘
```

**Required implementations in each binding:**

| Concern | What to implement |
|---------|-------------------|
| **Signal handling** | Trap SIGTERM/SIGINT, trigger graceful shutdown |
| **Graceful shutdown** | Flush pending writes, close isolates, release locks |
| **Thread management** | Worker threads with proper join/cleanup on exit |
| **Resource cleanup** | Ensure `close()` is called even on crashes (atexit, Drop, __del__) |
| **Shared isolate registry** | Prevent multiple isolates for same path (file lock conflicts) |
| **Reference counting** | Track open handles, cleanup when count reaches zero |
| **Timeout enforcement** | Wrap FFI calls with timeouts, kill hung operations |
| **Error recovery** | Detect corrupted state, attempt recovery or fail cleanly |
| **Lock file cleanup** | Remove stale `.lock` files from Git/Lucene on startup |

### Why This Design?

1. **GraalVM limitations**: Native-image has restricted signal handling capabilities
2. **Cross-language portability**: Each language has idiomatic ways to handle OS events
3. **Testability**: Core logic is pure, side effects are in the wrapper
4. **Failure isolation**: Binding crashes don't corrupt the shared library state
5. **Flexibility**: Different bindings can implement different strategies (sync vs async)

### Implementing a New Binding

When creating a binding for a new language:

1. **Start with lifecycle management** - ensure resources are always cleaned up
2. **Implement shared isolate registry** - keyed by `(data_path, index_path)`
3. **Add signal handlers** - graceful shutdown on SIGTERM/SIGINT
4. **Wrap all FFI calls** - add timeouts, error translation, logging
5. **Test crash scenarios** - kill -9, out of memory, disk full
6. **Test concurrent access** - multiple processes, same database path

### Shared Library Build

```bash
# Generate GraalVM args
clojure -M:shared-lib

# Build native library
native-image @target/shared-image-args -H:Name=libchrondb

# Output: libchrondb.so (Linux) / libchrondb.dylib (macOS)
```

### Python Bindings

```python
from chrondb import ChronDB

with ChronDB(data_path="./data", index_path="./index") as db:
    db.put("users/1", {"name": "Alice"})
    user = db.get("users/1")
    history = db.history("users/1")
```

Key implementation details:
- Shared isolate registry: `_isolate_registry` keyed by paths
- Reference counting: `add_ref()`, `release()`
- Auto library discovery: `~/.chrondb/lib/` or `CHRONDB_LIB_PATH`

### Rust Bindings

```rust
use chrondb::ChronDB;

let db = ChronDB::open("./data", "./index")?;
db.put("users/1", serde_json::json!({"name": "Alice"}))?;
let user = db.get("users/1")?;
let history = db.history("users/1")?;
```

Key implementation details:
- Shared worker registry: `OnceLock<Mutex<HashMap<...>>>`
- 64MB stack for FFI worker thread
- Command channel: Put, Get, Delete, Query, Shutdown
- Auto library download via `ensure_library_installed()`

### Testing Bindings

```bash
# Rust
cd bindings/rust && cargo test

# Python
cd bindings/python && pytest
```

## Native Image Considerations

### GraalVM Configuration

- Use `--features=clj_easy.graal_build_time.InitClojureClasses`
- Jackson factories must be runtime-initialized (see `native_image.clj`)
- CLI namespaces lazily loaded via `requiring-resolve`

### Performance

- Profile-guided optimization (PGO) for production builds
- Tune heap: `--initial-heap-size`, `--max-heap-size`
- Audit reflection/dynamic class loading

### Determinism

- Capture timestamps/random seeds explicitly during build
- Avoid environment-sensitive values in compiled code

## Docker

```bash
# Build
docker build -t avelino/chrondb .

# Run with data persistence
docker run --rm \
  -p 3000:3000 -p 6379:6379 -p 5432:5432 \
  -v $(pwd)/data:/app/data \
  avelino/chrondb
```

Image details:
- Builder: GraalVM 17
- Runtime: Debian 12-slim
- User: Non-root `chrondb`
- Ports: 3000 (HTTP), 6379 (Redis), 5432 (PostgreSQL)

## Documentation Updates

When modifying:
- **Backup/restore**: Update `docs/operations.md`
- **Lucene/search**: Update `docs/architecture.md`, `docs/performance.md`
- **API endpoints**: Update `docs/` and add examples
- **Configuration**: Update `config.example.edn` and `docs/quickstart.md`
- **Bindings**: Update `docs/bindings/python.md` or `docs/bindings/rust.md`

## Resources

- **Documentation**: [chrondb.avelino.run](https://chrondb.avelino.run/)
- **Source**: [github.com/avelino/chrondb](https://github.com/avelino/chrondb)
- **Issues**: [github.com/avelino/chrondb/issues](https://github.com/avelino/chrondb/issues)
- **Discussions**: [github.com/avelino/chrondb/discussions](https://github.com/avelino/chrondb/discussions)

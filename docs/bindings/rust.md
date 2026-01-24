# ChronDB Rust Binding

Rust client for ChronDB, a time-traveling key/value database built on Git architecture.

## Requirements

- Rust 1.56+ (2021 edition)
- `libclang` (for bindgen to generate FFI bindings)

## Installation

Add `chrondb` from [crates.io](https://crates.io/crates/chrondb):

```bash
cargo add chrondb
```

Or add it directly to your `Cargo.toml`:

```toml
[dependencies]
chrondb = "*"
serde_json = "1"
```

### Native shared library

The crate requires the `libchrondb` native library at runtime. Download it from the [latest GitHub release](https://github.com/moclojer/chrondb/releases/tag/latest):

**macOS (Apple Silicon):**

```bash
curl -L https://github.com/moclojer/chrondb/releases/download/latest/libchrondb-latest-macos-aarch64.tar.gz | tar xz
```

**Linux (x86_64):**

```bash
curl -L https://github.com/moclojer/chrondb/releases/download/latest/libchrondb-latest-linux-x86_64.tar.gz | tar xz
```

### Configure the runtime library path

The shared library must be discoverable at runtime:

**Linux:**

```bash
export LD_LIBRARY_PATH=/path/to/chrondb-rust/lib:$LD_LIBRARY_PATH
```

**macOS:**

```bash
export DYLD_LIBRARY_PATH=/path/to/chrondb-rust/lib:$DYLD_LIBRARY_PATH
```

### Library path (advanced)

To override the library location at build time, set `CHRONDB_LIB_DIR`:

```bash
export CHRONDB_LIB_DIR=/path/to/dir/with/libchrondb
```

## Quick Start

```rust
use chrondb::ChronDB;
use serde_json::json;

fn main() -> chrondb::Result<()> {
    let db = ChronDB::open("/tmp/chrondb-data", "/tmp/chrondb-index")?;

    // Save a document
    db.put("user:1", &json!({"name": "Alice", "age": 30}), None)?;

    // Retrieve it
    let doc = db.get("user:1", None)?;
    println!("{}", doc); // {"name":"Alice","age":30}

    Ok(())
    // db is automatically closed via Drop
}
```

## API Reference

### `ChronDB::open(data_path, index_path) -> Result<ChronDB>`

Opens a database connection.

| Parameter | Type | Description |
|-----------|------|-------------|
| `data_path` | `&str` | Path for the Git repository (data storage) |
| `index_path` | `&str` | Path for the Lucene index |

**Returns:** `Result<ChronDB>`

**Errors:** `IsolateCreationFailed`, `OpenFailed(reason)`

---

### `put(&self, id, doc, branch) -> Result<serde_json::Value>`

Saves a document.

| Parameter | Type | Description |
|-----------|------|-------------|
| `id` | `&str` | Document ID (e.g., `"user:1"`) |
| `doc` | `&serde_json::Value` | Document data as JSON value |
| `branch` | `Option<&str>` | Branch name (`None` for default) |

**Returns:** The saved document as `serde_json::Value`.

**Errors:** `OperationFailed(reason)`

---

### `get(&self, id, branch) -> Result<serde_json::Value>`

Retrieves a document by ID.

| Parameter | Type | Description |
|-----------|------|-------------|
| `id` | `&str` | Document ID |
| `branch` | `Option<&str>` | Branch name |

**Returns:** The document as `serde_json::Value`.

**Errors:** `NotFound`, `OperationFailed(reason)`

---

### `delete(&self, id, branch) -> Result<()>`

Deletes a document by ID.

| Parameter | Type | Description |
|-----------|------|-------------|
| `id` | `&str` | Document ID |
| `branch` | `Option<&str>` | Branch name |

**Returns:** `Ok(())` on success.

**Errors:** `NotFound`, `OperationFailed(reason)`

---

### `list_by_prefix(&self, prefix, branch) -> Result<serde_json::Value>`

Lists documents whose IDs start with the given prefix.

| Parameter | Type | Description |
|-----------|------|-------------|
| `prefix` | `&str` | ID prefix to match |
| `branch` | `Option<&str>` | Branch name |

**Returns:** JSON array of matching documents (empty array if none).

---

### `list_by_table(&self, table, branch) -> Result<serde_json::Value>`

Lists all documents in a table.

| Parameter | Type | Description |
|-----------|------|-------------|
| `table` | `&str` | Table name |
| `branch` | `Option<&str>` | Branch name |

**Returns:** JSON array of matching documents (empty array if none).

---

### `history(&self, id, branch) -> Result<serde_json::Value>`

Returns the change history of a document.

| Parameter | Type | Description |
|-----------|------|-------------|
| `id` | `&str` | Document ID |
| `branch` | `Option<&str>` | Branch name |

**Returns:** JSON array of history entries (empty array if none).

---

### `query(&self, query, branch) -> Result<serde_json::Value>`

Executes a query against the Lucene index.

| Parameter | Type | Description |
|-----------|------|-------------|
| `query` | `&serde_json::Value` | Query in Lucene AST format |
| `branch` | `Option<&str>` | Branch name |

**Returns:** Results object with `results`, `total`, `limit`, `offset`.

**Errors:** `OperationFailed(reason)`

---

### `last_error(&self) -> Option<String>`

Returns the last error message from the native library, if any.

---

### `Drop`

The `ChronDB` struct implements `Drop`. When it goes out of scope, the database connection is closed and the GraalVM isolate is torn down automatically.

## Document ID Convention

Documents use the format `table:id`:

```rust
db.put("user:123", &json!({"name": "Alice"}), None)?;
db.put("order:456", &json!({"total": 99.90}), None)?;
```

Internally, `user:123` is stored as `user/user_COLON_123.json` in the Git repository.

Use `list_by_table("user")` to retrieve all documents in the `user` table.

## Error Handling

### `ChronDBError` Enum

```rust
pub enum ChronDBError {
    IsolateCreationFailed,   // GraalVM isolate could not be created
    OpenFailed(String),      // Database failed to open (with reason)
    CloseFailed,             // Database failed to close
    NotFound,                // Document does not exist
    OperationFailed(String), // Operation failed (with reason)
    JsonError(String),       // JSON serialization/deserialization error
}
```

All variants implement `Display` and `std::error::Error`.

The crate also provides a type alias:

```rust
pub type Result<T> = std::result::Result<T, ChronDBError>;
```

### Conversion

`serde_json::Error` is automatically converted to `ChronDBError::JsonError` via `From`.

### Example

```rust
use chrondb::{ChronDB, ChronDBError};

fn main() {
    let db = ChronDB::open("/tmp/data", "/tmp/index").unwrap();

    match db.get("user:999", None) {
        Ok(doc) => println!("Found: {}", doc),
        Err(ChronDBError::NotFound) => println!("Document does not exist"),
        Err(e) => eprintln!("Error: {}", e),
    }
}
```

## Examples

### Full CRUD

```rust
use chrondb::ChronDB;
use serde_json::json;

fn main() -> chrondb::Result<()> {
    let db = ChronDB::open("/tmp/data", "/tmp/index")?;

    // Create
    db.put("user:1", &json!({"name": "Alice", "email": "alice@example.com"}), None)?;
    db.put("user:2", &json!({"name": "Bob", "email": "bob@example.com"}), None)?;

    // Read
    let alice = db.get("user:1", None)?;

    // Update
    let mut updated = alice.clone();
    updated["age"] = json!(30);
    db.put("user:1", &updated, None)?;

    // Delete
    db.delete("user:2", None)?;

    // List by table
    let users = db.list_by_table("user", None)?;
    println!("Users: {}", users);

    // List by prefix
    let matched = db.list_by_prefix("user:1", None)?;
    println!("Matched: {}", matched);

    Ok(())
}
```

### Query

```rust
use chrondb::ChronDB;
use serde_json::json;

fn main() -> chrondb::Result<()> {
    let db = ChronDB::open("/tmp/data", "/tmp/index")?;

    db.put("product:1", &json!({"name": "Laptop", "price": 999}), None)?;
    db.put("product:2", &json!({"name": "Mouse", "price": 29}), None)?;

    let results = db.query(&json!({
        "type": "term",
        "field": "name",
        "value": "Laptop"
    }), None)?;

    println!("Total: {}", results["total"]); // 1

    Ok(())
}
```

### History (Time Travel)

```rust
use chrondb::ChronDB;
use serde_json::json;

fn main() -> chrondb::Result<()> {
    let db = ChronDB::open("/tmp/data", "/tmp/index")?;

    db.put("config:app", &json!({"version": "1.0"}), None)?;
    db.put("config:app", &json!({"version": "2.0"}), None)?;

    let entries = db.history("config:app", None)?;
    println!("History: {}", entries);

    Ok(())
}
```

### Using with `Drop` (automatic cleanup)

```rust
use chrondb::ChronDB;
use serde_json::json;

fn do_work() -> chrondb::Result<()> {
    let db = ChronDB::open("/tmp/data", "/tmp/index")?;
    db.put("temp:1", &json!({"data": "value"}), None)?;
    Ok(())
    // db is dropped here, closing the connection
}
```

## Building from Source

```bash
# 1. Build the shared library (requires Java 11+ and GraalVM)
cd chrondb/
clojure -M:shared

# 2. Build the Rust binding
cd bindings/rust/
CHRONDB_LIB_DIR=../../target cargo build

# 3. Run tests
CHRONDB_LIB_DIR=../../target \
  LD_LIBRARY_PATH=../../target \
  cargo test
```

### `build.rs` Behavior

The build script (`build.rs`):

1. Reads `CHRONDB_LIB_DIR` (defaults to `../../target`)
2. Configures `rustc-link-search` and `rustc-link-lib=dylib=chrondb`
3. If `libchrondb.h` and `graal_isolate.h` exist in that directory, generates FFI bindings via `bindgen`
4. Otherwise, uses stub bindings (`src/ffi_stub.rs`) to allow compilation without the native library

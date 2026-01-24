# ChronDB Python Binding

Python client for ChronDB, a time-traveling key/value database built on Git architecture.

## Requirements

- Python 3.8+
- ChronDB shared library (`libchrondb.so` / `libchrondb.dylib` / `chrondb.dll`)
- Java 11+ and GraalVM (for building the shared library from source)

## Installation

### 1. Build the shared library

```bash
cd chrondb/
clojure -M:shared
```

This produces the native library in `target/` (e.g., `target/libchrondb.so`).

### 2. Install the Python package

```bash
cd bindings/python/
pip install .
```

### 3. Configure the library path

The binding searches for the shared library in this order:

1. `CHRONDB_LIB_PATH` environment variable (full path to the library file)
2. `CHRONDB_LIB_DIR` environment variable (directory containing the library)
3. Relative to the package: `../../target/`
4. Package `lib/` subdirectory
5. System paths: `/usr/local/lib`, `/usr/lib`

```bash
export CHRONDB_LIB_PATH=/path/to/libchrondb.so
# or
export CHRONDB_LIB_DIR=/path/to/dir/
```

## Quick Start

```python
from chrondb import ChronDB

with ChronDB("/tmp/chrondb-data", "/tmp/chrondb-index") as db:
    # Save a document
    db.put("user:1", {"name": "Alice", "age": 30})

    # Retrieve it
    doc = db.get("user:1")
    print(doc)  # {"name": "Alice", "age": 30}
```

## API Reference

### `ChronDB(data_path: str, index_path: str)`

Opens a database connection.

| Parameter | Type | Description |
|-----------|------|-------------|
| `data_path` | `str` | Path for the Git repository (data storage) |
| `index_path` | `str` | Path for the Lucene index |

**Raises:** `ChronDBError` if the database cannot be opened.

Implements the context manager protocol (`__enter__` / `__exit__`), calling `close()` automatically on exit.

---

### `put(id, doc, branch=None) -> Dict[str, Any]`

Saves a document.

| Parameter | Type | Description |
|-----------|------|-------------|
| `id` | `str` | Document ID (e.g., `"user:1"`) |
| `doc` | `Dict[str, Any]` | Document data |
| `branch` | `Optional[str]` | Branch name (`None` for default) |

**Returns:** The saved document as a dictionary.

**Raises:** `ChronDBError` on failure.

---

### `get(id, branch=None) -> Dict[str, Any]`

Retrieves a document by ID.

| Parameter | Type | Description |
|-----------|------|-------------|
| `id` | `str` | Document ID |
| `branch` | `Optional[str]` | Branch name |

**Returns:** The document as a dictionary.

**Raises:** `DocumentNotFoundError` if not found, `ChronDBError` on failure.

---

### `delete(id, branch=None) -> bool`

Deletes a document by ID.

| Parameter | Type | Description |
|-----------|------|-------------|
| `id` | `str` | Document ID |
| `branch` | `Optional[str]` | Branch name |

**Returns:** `True` if deleted.

**Raises:** `DocumentNotFoundError` if not found, `ChronDBError` on failure.

---

### `list_by_prefix(prefix, branch=None) -> List[Dict[str, Any]]`

Lists documents whose IDs start with the given prefix.

| Parameter | Type | Description |
|-----------|------|-------------|
| `prefix` | `str` | ID prefix to match |
| `branch` | `Optional[str]` | Branch name |

**Returns:** List of matching documents (empty list if none).

---

### `list_by_table(table, branch=None) -> List[Dict[str, Any]]`

Lists all documents in a table.

| Parameter | Type | Description |
|-----------|------|-------------|
| `table` | `str` | Table name |
| `branch` | `Optional[str]` | Branch name |

**Returns:** List of matching documents (empty list if none).

---

### `history(id, branch=None) -> List[Dict[str, Any]]`

Returns the change history of a document.

| Parameter | Type | Description |
|-----------|------|-------------|
| `id` | `str` | Document ID |
| `branch` | `Optional[str]` | Branch name |

**Returns:** List of history entries (empty list if none).

---

### `query(query, branch=None) -> Dict[str, Any]`

Executes a query against the Lucene index.

| Parameter | Type | Description |
|-----------|------|-------------|
| `query` | `Dict[str, Any]` | Query in Lucene AST format |
| `branch` | `Optional[str]` | Branch name |

**Returns:** Results dict with keys `results`, `total`, `limit`, `offset`.

**Raises:** `ChronDBError` on failure.

---

### `close()`

Closes the database connection and releases native resources. Called automatically when using the context manager.

## Document ID Convention

Documents use the format `table:id`:

```python
db.put("user:123", {"name": "Alice"})
db.put("order:456", {"total": 99.90})
```

Internally, `user:123` is stored as `user/user_COLON_123.json` in the Git repository.

Use `list_by_table("user")` to retrieve all documents in the `user` table.

## Error Handling

### Exception Hierarchy

```
ChronDBError (base)
  └── DocumentNotFoundError
```

### `ChronDBError`

Base exception for all ChronDB errors (connection failures, operation errors, native library issues).

### `DocumentNotFoundError`

Raised by `get()` and `delete()` when the target document does not exist.

### Example

```python
from chrondb import ChronDB, ChronDBError, DocumentNotFoundError

try:
    with ChronDB("/tmp/data", "/tmp/index") as db:
        doc = db.get("user:999")
except DocumentNotFoundError:
    print("Document does not exist")
except ChronDBError as e:
    print(f"Database error: {e}")
```

## Examples

### Full CRUD

```python
from chrondb import ChronDB

with ChronDB("/tmp/data", "/tmp/index") as db:
    # Create
    db.put("user:1", {"name": "Alice", "email": "alice@example.com"})
    db.put("user:2", {"name": "Bob", "email": "bob@example.com"})

    # Read
    alice = db.get("user:1")

    # Update
    alice["age"] = 30
    db.put("user:1", alice)

    # Delete
    db.delete("user:2")

    # List by table
    users = db.list_by_table("user")

    # List by prefix
    matched = db.list_by_prefix("user:1")
```

### Query

```python
with ChronDB("/tmp/data", "/tmp/index") as db:
    db.put("product:1", {"name": "Laptop", "price": 999})
    db.put("product:2", {"name": "Mouse", "price": 29})

    results = db.query({
        "type": "term",
        "field": "name",
        "value": "Laptop"
    })
    print(results["total"])  # 1
```

### History (Time Travel)

```python
with ChronDB("/tmp/data", "/tmp/index") as db:
    db.put("config:app", {"version": "1.0"})
    db.put("config:app", {"version": "2.0"})

    entries = db.history("config:app")
    for entry in entries:
        print(entry)
```

### Pytest Fixture

```python
import pytest
from chrondb import ChronDB

@pytest.fixture
def db(tmp_path):
    data_path = str(tmp_path / "data")
    index_path = str(tmp_path / "index")
    with ChronDB(data_path, index_path) as conn:
        yield conn

def test_put_and_get(db):
    db.put("item:1", {"value": 42})
    doc = db.get("item:1")
    assert doc["value"] == 42
```

## Building from Source

```bash
# 1. Build the shared library (requires Java 11+ and GraalVM)
cd chrondb/
clojure -M:shared

# 2. Install the Python package in development mode
cd bindings/python/
pip install -e .

# 3. Run tests
CHRONDB_LIB_DIR=../../target pytest
```

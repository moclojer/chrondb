# ChronDB Bindings — Shared Library + Language Bindings

ChronDB exposes its core functionality as a **C shared library** via GraalVM `native-image --shared`. This enables bindings for any language that can consume C APIs — currently Rust and Python.

## Architecture

```
┌─────────────────────────────────────────────────────┐
│  Language Bindings (Rust, Python, ...)              │
│  Safe wrappers, JSON parsing, resource management   │
├─────────────────────────────────────────────────────┤
│  C API (libchrondb.dylib / libchrondb.so)           │
│  @CEntryPoint — GraalVM native-image --shared      │
├─────────────────────────────────────────────────────┤
│  Java Bridge (ChronDBLib.java)                      │
│  Lazy-loads Clojure, converts C ↔ Java types        │
├─────────────────────────────────────────────────────┤
│  Clojure Bridge (chrondb.lib.core)                  │
│  Handle registry, orchestrates Storage + Index      │
├─────────────────────────────────────────────────────┤
│  ChronDB Core                                       │
│  GitStorage + LuceneIndex                           │
└─────────────────────────────────────────────────────┘
```

### Call flow (e.g. `put`)

1. **Binding** (Rust/Python) converts native arguments → C strings
2. **C API** receives `IsolateThread*`, `handle`, `id`, `json`, `branch`
3. **Java Bridge** converts `CCharPointer` → `String`, calls Clojure via `IFn.invoke()`
4. **Clojure Bridge** looks up `{:storage, :index}` in the registry by handle, executes the operation
5. Result returns as a JSON string (C `char*`), which the binding parses into native types

## Building the Shared Library

### Prerequisites

- Java 17+ (GraalVM CE or Oracle GraalVM)
- `native-image` installed (`gu install native-image` or bundled with GraalVM 22.3+)
- Clojure CLI (`clojure`)

### Steps

```bash
# 1. Prepare (uberjar + compile Java + generate args)
clojure -M:shared-lib

# 2. Build the shared library
native-image @target/shared-image-args

# 3. Verify output
ls target/libchrondb.* target/graal_isolate.h target/libchrondb.h
```

Step 1 generates `target/shared-image-args` — an argument file for `native-image`. Step 2 produces:

| File | Description |
|------|-------------|
| `libchrondb.dylib` / `.so` | The shared library |
| `libchrondb.h` | C header with exported functions |
| `libchrondb_dynamic.h` | Header for dynamic loading |
| `graal_isolate.h` | GraalVM isolate types |
| `graal_isolate_dynamic.h` | Dynamic version of the isolate header |

## C API

All interaction with the library requires a **GraalVM Isolate** (embedded VM):

```c
#include "graal_isolate.h"
#include "libchrondb.h"

graal_isolate_t *isolate = NULL;
graal_isolatethread_t *thread = NULL;
graal_create_isolate(NULL, &isolate, &thread);

// ... use the API ...

graal_tear_down_isolate(thread);
```

### Available Functions

```c
// Lifecycle
int chrondb_open(thread, data_path, index_path)     // → handle (>= 0) or -1
int chrondb_close(thread, handle)                   // → 0 ok, -1 error

// Storage
char* chrondb_put(thread, handle, id, json, branch)          // → JSON saved doc
char* chrondb_get(thread, handle, id, branch)                // → JSON or NULL
int   chrondb_delete(thread, handle, id, branch)             // → 0 ok, 1 not found, -1 error
char* chrondb_list_by_prefix(thread, handle, prefix, branch) // → JSON array
char* chrondb_list_by_table(thread, handle, table, branch)   // → JSON array
char* chrondb_history(thread, handle, id, branch)            // → JSON array

// Query
char* chrondb_query(thread, handle, query_json, branch)      // → JSON result

// Utilities
void  chrondb_free_string(thread, ptr)   // free returned strings
char* chrondb_last_error(thread)         // last error for this thread
```

**Conventions:**
- `branch = NULL` → uses default branch ("main")
- Returned strings must be freed with `chrondb_free_string`
- On error, `chrondb_last_error` returns the error message

## Existing Bindings

### Rust (`bindings/rust/`)

```rust
use chrondb::ChronDB;
use serde_json::json;

let db = ChronDB::open("/tmp/data", "/tmp/index")?;
db.put("user:1", &json!({"name": "Alice"}), None)?;
let doc = db.get("user:1", None)?;
db.delete("user:1", None)?;
// Drop automatically closes the handle and tears down the isolate
```

**Running tests:**
```bash
cd bindings/rust
CHRONDB_LIB_DIR=../../target cargo test
```

### Python (`bindings/python/`)

```python
from chrondb import ChronDB

with ChronDB("/tmp/data", "/tmp/index") as db:
    db.put("user:1", {"name": "Alice"})
    doc = db.get("user:1")
    db.delete("user:1")
```

**Running tests:**
```bash
cd bindings/python
CHRONDB_LIB_PATH=../../target/libchrondb.dylib pytest tests/ -v
```

## Installing from GitHub Releases

Pre-built binaries are available as GitHub Releases. Each push to `main` updates a rolling `latest` prerelease; tagged versions (e.g. `v0.1.0`) produce stable releases.

### Python

```bash
# Latest stable release
pip install https://github.com/moclojer/chrondb/releases/download/v0.1.0/chrondb-0.1.0-py3-none-manylinux_2_35_x86_64.whl

# Rolling release from main
pip install https://github.com/moclojer/chrondb/releases/download/latest/chrondb-latest-py3-none-manylinux_2_35_x86_64.whl
```

### Rust

```bash
# Download and extract (latest or specific version)
curl -L https://github.com/moclojer/chrondb/releases/download/latest/chrondb-rust-latest-linux-x86_64.tar.gz | tar xz
cd chrondb-rust-latest-linux-x86_64
CHRONDB_LIB_DIR=./lib cargo build
```

### C / Other Languages

```bash
curl -L https://github.com/moclojer/chrondb/releases/download/latest/libchrondb-latest-linux-x86_64.tar.gz | tar xz
# Link against lib/libchrondb.so, include headers from include/
```

## Maintenance

### Adding a New Operation to the API

When a new operation needs to be exposed (e.g. `compact`, `branch-create`):

**1. Clojure Bridge** — `src/chrondb/lib/core.clj`

Add the function:

```clojure
(defn lib-new-operation
  "Description of the operation."
  [handle arg1 arg2]
  (try
    (when-let [{:keys [storage index]} (get @handle-registry handle)]
      ;; implementation using storage/index
      (json/write-str result))
    (catch Exception _e
      nil)))
```

**2. Java Bridge** — `java/chrondb/lib/ChronDBLib.java`

Add the `IFn` field and the `@CEntryPoint`:

```java
private static IFn libNewOperation;

// In ensureInitialized():
libNewOperation = Clojure.var("chrondb.lib.core", "lib-new-operation");

@CEntryPoint(name = "chrondb_new_operation")
public static CCharPointer newOperation(IsolateThread thread, int handle,
                                        CCharPointer arg1, CCharPointer arg2) {
    try {
        ensureInitialized();
        String a1 = toJavaString(arg1);
        String a2 = toJavaString(arg2);
        Object result = libNewOperation.invoke(handle, a1, a2);
        if (result instanceof String) {
            return toCString((String) result);
        }
        return WordFactory.nullPointer();
    } catch (Exception e) {
        lastError.set(e.getMessage());
        return WordFactory.nullPointer();
    }
}
```

**3. Rebuild** the shared library to generate the updated header.

**4. Update each binding:**

- **Rust:** Add a method in `src/lib.rs` under `impl ChronDB`
- **Python:** Add in `chrondb/_ffi.py` (argtypes/restype) and `chrondb/client.py` (method)

**5. Tests:** Add tests in each binding.

### Updating Dependencies

If the ChronDB core changes (e.g. new field in the Storage protocol):

1. Update `src/chrondb/lib/core.clj` to use the new API
2. The C API **does not need to change** if the interface is compatible
3. Rebuild the shared library

### Changing the GraalVM Version

Edit `.github/workflows/build-shared-library.yml`:

```yaml
- uses: graalvm/setup-graalvm@v1
  with:
    version: "22.3.2"  # ← change here
    java-version: "17"  # ← and/or here
```

Test locally before merging.

## Creating a New Binding

To create bindings for another language (Go, C#, Ruby, etc.):

### 1. Directory Structure

```
bindings/
└── <language>/
    ├── README.md          # Language-specific instructions
    ├── <build-file>       # go.mod, .csproj, Gemfile, etc.
    ├── src/
    │   ├── ffi.*          # FFI declarations (extern, P/Invoke, ffi gem, etc.)
    │   └── client.*       # Safe wrapper with native types
    └── tests/
        └── test_*.*       # Integration tests
```

### 2. What the Binding Must Implement

1. **Load the library** (`dlopen`, `LoadLibrary`, etc.)
2. **Create isolate:** `graal_create_isolate(NULL, &isolate, &thread)`
3. **Open database:** `chrondb_open(thread, data_path, index_path)` → handle
4. **Operations:** Convert native strings → C strings, call function, parse JSON result
5. **Cleanup:** `chrondb_close(thread, handle)` + `graal_tear_down_isolate(thread)`

### 3. Safe Wrapper Pattern

The wrapper should:

- Encapsulate `isolate`, `thread`, and `handle` in a struct/class
- Implement `Drop`/`Dispose`/`close()` for automatic cleanup
- Convert returned JSON strings to native language types
- Handle errors: check for NULL/negative returns, call `chrondb_last_error`
- Be thread-safe if the language supports it (each OS thread needs its own isolate thread)

### 4. Test Pattern

```
1. Create a temporary directory for data + index
2. Open database
3. Put → Get → verify equality
4. Delete → Get → verify not found
5. List by prefix / table
6. History (put 2x, verify entries)
7. Close database (or test automatic cleanup)
```

### 5. CI

Add a job to `.github/workflows/build-shared-library.yml`:

```yaml
test-<language>-bindings:
  needs: build-shared-lib
  runs-on: ${{ matrix.os }}
  steps:
    - uses: actions/checkout@v4
    - uses: actions/download-artifact@v4
      with:
        name: libchrondb-${{ runner.os }}
        path: target/
    # Language setup
    # Run tests with CHRONDB_LIB_PATH/DIR pointing to target/
```

## Troubleshooting

### `native-image` fails with classes not found

The shared library includes the entire uberjar classpath. If new dependencies were added to the core, rebuild:

```bash
clojure -M:shared-lib  # regenerates uberjar + args
native-image @target/shared-image-args
```

### Reflection errors at runtime

Add the class to `reflect-config.json` via `dev/chrondb/native_image.clj` in `base-reflect-config`.

### Linker errors in Rust (`library 'chrondb' not found`)

The library must be in the indicated path:

```bash
CHRONDB_LIB_DIR=/path/to/target cargo test
# or
export LD_LIBRARY_PATH=/path/to/target  # Linux
export DYLD_LIBRARY_PATH=/path/to/target  # macOS
```

### Python cannot find the library

```bash
CHRONDB_LIB_PATH=/full/path/to/libchrondb.dylib pytest tests/
```

### Segfault when calling functions

Check:
1. The isolate was created before calling any function
2. The handle returned by `chrondb_open` is >= 0
3. Passed strings are null-terminated (valid C strings)
4. Returned strings are not used after `chrondb_free_string`

## Design Decisions

### Why GraalVM `--shared` instead of JNI/JNA?

- **Zero JVM dependency** at the consumer's runtime
- The library is a native binary — loads in ~50ms, no JDK required
- Any language that can call C can use it

### Why JSON as the interchange format?

- Simple, ubiquitous, no rigid schema
- ChronDB already stores JSON documents internally
- Avoids complex type mapping between Clojure ↔ C ↔ N languages
- Acceptable overhead for database operations (I/O dominates)

### Why a handle registry instead of opaque pointers?

- Safer: handles are integers, not raw pointers
- Allows multiple simultaneous databases
- Deterministic cleanup on shutdown
- Thread-safety via Clojure atom

# ChronDB

Rust bindings for [ChronDB](https://github.com/moclojer/chrondb) — a time-traveling key/value database built on Git.

## Install

```bash
cargo add chrondb
```

## Quick Start

```rust
use chrondb::ChronDB;
use serde_json::json;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = ChronDB::open("/tmp/data", "/tmp/index")?;

    db.put("user:1", &json!({"name": "Alice", "email": "alice@example.com"}), None)?;

    let user = db.get("user:1", None)?;
    println!("{:?}", user);

    Ok(())
}
```

## Documentation

- [Rust Bindings](https://github.com/moclojer/chrondb/tree/main/bindings/rust)
- [Full Documentation](https://github.com/moclojer/chrondb/tree/main/docs/bindings)
- [All Language Bindings](https://github.com/moclojer/chrondb/tree/main/bindings)

## License

AGPL-3.0

# ChronDB

Python bindings for [ChronDB](https://github.com/moclojer/chrondb) — a time-traveling key/value database built on Git.

## Install

```bash
pip install chrondb
```

## Quick Start

```python
from chrondb import ChronDB

with ChronDB("/tmp/data", "/tmp/index") as db:
    db.put("user:1", {"name": "Alice", "email": "alice@example.com"})

    user = db.get("user:1")
    print(user)
```

## Documentation

- [Python Bindings](https://github.com/moclojer/chrondb/tree/main/bindings/python)
- [Full Documentation](https://github.com/moclojer/chrondb/tree/main/docs/bindings)
- [All Language Bindings](https://github.com/moclojer/chrondb/tree/main/bindings)

## License

AGPL-3.0

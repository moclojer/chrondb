# ChronDB Examples

This document provides links to detailed examples for using the various ChronDB interfaces.

ChronDB supports multiple access protocols, allowing you to choose the interface that best suits your needs:

## [Clojure API](examples-clojure.md)

The native Clojure API offers the most direct way to interact with ChronDB, providing access to all features including CRUD operations, searching, version control, and branching.

```clojure
;; Basic example
(require '[chrondb.core :as chrondb])
(def db (chrondb/create-chrondb))
(chrondb/save db "user:1" {:name "Alice" :email "alice@example.com"})
```

[View complete Clojure API examples →](examples-clojure.md)

## [REST API](examples-rest.md)

The REST API allows you to interact with ChronDB using HTTP requests, making it easy to integrate with any programming language or environment that can make HTTP requests.

```bash
# Create a document using curl
curl -X POST http://localhost:3000/api/v1/documents/user:1 \
  -H "Content-Type: application/json" \
  -d '{"name": "John Doe", "email": "john@example.com"}'
```

[View complete REST API examples →](examples-rest.md)

## [Redis Protocol](examples-redis.md)

ChronDB implements a subset of the Redis protocol, allowing you to connect using standard Redis clients.

```bash
# Using redis-cli
redis-cli -h localhost -p 6379
> SET user:1 '{"name":"John Doe","email":"john@example.com"}'
```

[View complete Redis protocol examples →](examples-redis.md)

## [PostgreSQL Protocol](examples-postgresql.md)

ChronDB implements a subset of the PostgreSQL protocol, allowing you to connect using standard PostgreSQL clients and leverage SQL query capabilities.

```sql
-- Create a document using psql
INSERT INTO user (id, name, email)
VALUES ('1', 'John Doe', 'john@example.com');
```

[View complete PostgreSQL protocol examples →](examples-postgresql.md)

## Examples by Protocol

For more detailed examples, see:

- [Clojure API Examples](examples-clojure.md)
- [REST API Examples](examples-rest.md)
- [Redis Protocol Examples](examples-redis.md)
- [PostgreSQL Protocol Examples](examples-postgresql.md)

## SQL History Functions

Here are examples of using the SQL history functions:

### Document History

To retrieve the complete history of a document:

```sql
-- View all changes to a user document
SELECT * FROM chrondb_history('user', '1');

-- Extract only specific history information
SELECT commit_id, timestamp FROM chrondb_history('user', '1');

-- Find the most recent 5 changes
SELECT * FROM chrondb_history('user', '1') LIMIT 5;
```

### Point-in-Time Document Access

To access documents at specific points in time:

```sql
-- View document as it was at a specific commit
SELECT * FROM chrondb_at('user', '1', 'abc123def456');

-- Extract specific fields from a historical version
SELECT name, email FROM chrondb_at('user', '1', 'abc123def456');

-- Compare with current version
SELECT
  current.name AS current_name,
  history.name AS previous_name
FROM
  user AS current,
  chrondb_at('user', '1', 'abc123def456') AS history
WHERE
  current.id = '1';
```

### Document Version Comparison

To compare different versions of a document:

```sql
-- Compare two versions of a document
SELECT * FROM chrondb_diff('user', '1', 'abc123def456', 'def456abc123');

-- Check only what fields changed
SELECT changed FROM chrondb_diff('user', '1', 'abc123def456', 'def456abc123');

-- Get compact view of changes
SELECT
  id,
  commit1,
  commit2,
  CASE
    WHEN added IS NULL THEN 'No additions'
    ELSE added
  END AS added_fields,
  CASE
    WHEN removed IS NULL THEN 'No removals'
    ELSE removed
  END AS removed_fields,
  CASE
    WHEN changed IS NULL THEN 'No changes'
    ELSE changed
  END AS modified_fields
FROM
  chrondb_diff('user', '1', 'abc123def456', 'def456abc123');
```

These examples demonstrate how to leverage ChronDB's history tracking capabilities using the SQL interface.

---

Choose the interface that best suits your needs and refer to the specific documentation for detailed examples and use cases.

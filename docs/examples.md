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

---

Choose the interface that best suits your needs and refer to the specific documentation for detailed examples and use cases.

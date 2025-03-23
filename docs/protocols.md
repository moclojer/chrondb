# ChronDB Protocols

ChronDB supports multiple access protocols, allowing integration with different clients and frameworks.

## REST API

ChronDB provides a complete REST API for database operations.

### Configuration

In the `config.edn` file:

```clojure
:servers {
  :rest {
    :enabled true
    :host "0.0.0.0"
    :port 3000
  }
}
```

### Endpoints

#### Documents

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/v1/documents/:key` | Get document by key |
| POST | `/api/v1/documents/:key` | Create/update document |
| DELETE | `/api/v1/documents/:key` | Delete document |
| GET | `/api/v1/documents/:key/history` | Get document history |
| GET | `/api/v1/documents/:key/at/:timestamp` | Get document at a point in time |

#### Search

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/v1/search?q=query` | Search documents |

#### Branches

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/v1/branches` | List branches |
| POST | `/api/v1/branches/:name` | Create branch |
| PUT | `/api/v1/branches/:name/checkout` | Switch to branch |
| POST | `/api/v1/branches/:source/merge/:target` | Merge branches |

### Example with curl

```bash
# Create document
curl -X POST http://localhost:3000/api/v1/documents/user:1 \
  -H "Content-Type: application/json" \
  -d '{"name":"John Doe","email":"john@example.com"}'

# Get document
curl http://localhost:3000/api/v1/documents/user:1

# Search documents
curl http://localhost:3000/api/v1/search?q=name:John

# Get history
curl http://localhost:3000/api/v1/documents/user:1/history
```

## Redis Protocol

ChronDB implements a subset of the Redis protocol, allowing Redis clients to connect directly.

### Configuration

In the `config.edn` file:

```clojure
:servers {
  :redis {
    :enabled true
    :host "0.0.0.0"
    :port 6379
  }
}
```

### Supported Commands

| Command | Description |
|---------|-------------|
| `GET key` | Get document by key |
| `SET key value` | Create/update document |
| `DEL key` | Delete document |
| `KEYS pattern` | List keys matching the pattern |
| `HGET key field` | Get specific field from document |
| `HSET key field value` | Set specific field in document |
| `CHRONDB.HISTORY key` | Get document history |
| `CHRONDB.GETAT key timestamp` | Get document at a point in time |
| `CHRONDB.DIFF key t1 t2` | Compare document versions |

### Example with redis-cli

```bash
# Connect to ChronDB
redis-cli -h localhost -p 6379

# Set document
SET user:1 '{"name":"John Doe","email":"john@example.com"}'

# Get document
GET user:1

# Get history
CHRONDB.HISTORY user:1
```

## PostgreSQL Protocol

ChronDB implements a subset of the PostgreSQL protocol, allowing connection with SQL clients.

### Configuration

In the `config.edn` file:

```clojure
:servers {
  :postgresql {
    :enabled true
    :host "0.0.0.0"
    :port 5432
    :username "chrondb"
    :password "chrondb"
  }
}
```

### Data Model

Documents are mapped to virtual tables based on their keys:

- Prefix before `:` becomes the table name
- Document fields become columns

For example, the key `user:1` with document `{"name":"John","email":"john@example.com"}` is accessible as:

```sql
SELECT * FROM user WHERE id = '1';
```

### SQL Features

| Feature | Description |
|---------|-------------|
| `SELECT` | Query documents |
| `INSERT` | Create documents |
| `UPDATE` | Update documents |
| `DELETE` | Delete documents |
| `CREATE TABLE` | Create collection (optional, schemas are inferred) |

### Special Functions

| Function | Description |
|----------|-------------|
| `chrondb_history(table, id)` | Get document history |
| `chrondb_at(table, id, timestamp)` | Get document at a point in time |
| `chrondb_diff(table, id, t1, t2)` | Compare document versions |

### Example with psql

```bash
# Connect to ChronDB
psql -h localhost -p 5432 -U chrondb

# Create document
INSERT INTO user (id, name, email) VALUES ('1', 'John Doe', 'john@example.com');

# Query document
SELECT * FROM user WHERE id = '1';

# Update document
UPDATE user SET email = 'john.doe@example.com' WHERE id = '1';

# Get history
SELECT * FROM chrondb_history('user', '1');
```

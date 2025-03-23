# Quick Start Guide

This guide will help you get started with ChronDB quickly. We'll cover installation, basic operations, and examples for each supported protocol.

## Installation

### Using Docker (Recommended)

The fastest way to get started is with Docker:

```bash
# Pull the ChronDB image
docker pull moclojer/chrondb:latest

# Run ChronDB with all protocols enabled
docker run -d --name chrondb \
  -p 3000:3000 \  # REST API
  -p 6379:6379 \  # Redis protocol
  -p 5432:5432 \  # PostgreSQL protocol
  -v chrondb-data:/data \
  moclojer/chrondb:latest
```

### Using JAR File

You can also download and run the JAR file directly:

```bash
# Download the latest release
curl -L -o chrondb.jar https://github.com/moclojer/chrondb/releases/latest/download/chrondb.jar

# Run ChronDB (with default configuration)
java -jar chrondb.jar
```

### Using Clojure Tools

For Clojure developers:

```bash
# Clone the repository
git clone https://github.com/moclojer/chrondb.git
cd chrondb

# Start the server using Clojure tools
clojure -M:run
```

## Basic Configuration

ChronDB uses a `config.edn` file for configuration. Create one in your working directory:

```clojure
{:storage {:data-dir "/path/to/data"}
 :servers {:rest {:enabled true
                  :host "0.0.0.0"
                  :port 3000}
           :redis {:enabled true
                   :host "0.0.0.0"
                   :port 6379}
           :postgresql {:enabled true
                        :host "0.0.0.0"
                        :port 5432
                        :username "chrondb"
                        :password "chrondb"}}}
```

## Getting Started with ChronDB

Let's look at basic operations using different protocols.

### Using the Clojure API

Add ChronDB as a dependency in your project:

```clojure
;; deps.edn
{:deps {com.github.moclojer/chrondb {:git/tag "v0.1.0"
                                     :git/sha "..."}}}
```

Basic usage:

```clojure
(require '[chrondb.core :as chrondb])

;; Create a database
(def db (chrondb/create-chrondb))

;; Create documents
(chrondb/save db "user:1" {:name "Alice" :email "alice@example.com"})
(chrondb/save db "user:2" {:name "Bob" :email "bob@example.com"})

;; Retrieve a document
(def user (chrondb/get db "user:1"))
(println user)  ;; {:name "Alice" :email "alice@example.com"}

;; Update a document
(chrondb/save db "user:1" (assoc user :age 30))

;; Search for documents
(def results (chrondb/search db "name:Alice"))
(println results)

;; Get document history
(def history (chrondb/history db "user:1"))
(println history)

;; Try branching
(def test-db (chrondb/create-branch db "test"))
(chrondb/save test-db "user:1" {:name "Alice (test)" :email "alice@test.com"})

;; Compare main and test branch
(println (chrondb/get db "user:1"))  ;; Original version
(println (chrondb/get test-db "user:1"))  ;; Test version
```

For more comprehensive Clojure examples, see [Clojure API Documentation](examples-clojure.md).

### Using the REST API

If you have ChronDB running with the REST API enabled, you can interact with it using HTTP:

```bash
# Create a document
curl -X POST http://localhost:3000/api/v1/documents/user:3 \
  -H "Content-Type: application/json" \
  -d '{"name": "Charlie", "email": "charlie@example.com"}'

# Retrieve a document
curl http://localhost:3000/api/v1/documents/user:3

# Update a document
curl -X POST http://localhost:3000/api/v1/documents/user:3 \
  -H "Content-Type: application/json" \
  -d '{"name": "Charlie", "email": "charlie@example.com", "age": 25}'

# Search for documents
curl http://localhost:3000/api/v1/search?q=name:Charlie

# Get document history
curl http://localhost:3000/api/v1/documents/user:3/history
```

For more comprehensive REST API examples, including JavaScript clients, see [REST API Documentation](examples-rest.md).

### Using the Redis Protocol

If you have Redis CLI installed, you can interact with ChronDB using Redis commands:

```bash
# Connect to ChronDB via Redis protocol
redis-cli -h localhost -p 6379

# Create a document
SET user:4 '{"name":"Dave","email":"dave@example.com"}'

# Retrieve a document
GET user:4

# Update a document
SET user:4 '{"name":"Dave","email":"dave@example.com","age":35}'

# Get document history (ChronDB-specific command)
CHRONDB.HISTORY user:4
```

For more comprehensive Redis protocol examples, including JavaScript clients, see [Redis Protocol Documentation](examples-redis.md).

### Using the PostgreSQL Protocol

If you have `psql` installed, you can interact with ChronDB using SQL:

```bash
# Connect to ChronDB via PostgreSQL protocol
psql -h localhost -p 5432 -U chrondb -d chrondb

# Create a document (as a row in a table)
INSERT INTO user (id, name, email) VALUES ('5', 'Eve', 'eve@example.com');

# Retrieve a document
SELECT * FROM user WHERE id = '5';

# Update a document
UPDATE user SET email = 'eve.smith@example.com' WHERE id = '5';

# Get document history (ChronDB-specific function)
SELECT * FROM chrondb_history('user', '5');
```

For more comprehensive PostgreSQL protocol examples, including JavaScript clients, see [PostgreSQL Protocol Documentation](examples-postgresql.md).

## Key Features to Try

Once you're familiar with basic operations, try these key features:

### Version Control

```clojure
;; Get a document as it existed at a point in time
(def old-version (chrondb/get-at db "user:1" "2023-01-01T00:00:00Z"))

;; Compare versions
(def diff (chrondb/diff db "user:1"
                        "2023-01-01T00:00:00Z"
                        "2023-06-01T00:00:00Z"))
```

For more details on version control features, see [Version Control Documentation](version-control.md).

### Branching and Merging

```clojure
;; Create a development branch
(def dev-db (chrondb/create-branch db "dev"))

;; Make changes in the dev branch
(chrondb/save dev-db "user:1" {:name "Alice (Updated)" :email "alice@example.com"})

;; Merge changes back to main
(def merged-db (chrondb/merge-branch db "dev" "main"))
```

### Transactions

```clojure
;; Execute multiple operations atomically
(chrondb/with-transaction [db]
  (chrondb/save db "order:1" {:items [{:id "item1" :qty 2}] :total 50.0})
  (chrondb/save db "inventory:item1" {:stock 18}) ;; Reducing stock
  (chrondb/save db "user:1" (update (chrondb/get db "user:1") :orders conj "order:1")))
```

## Next Steps

Now that you've got the basics, you can:

1. Explore the [complete Clojure API examples](examples-clojure.md)
2. Learn more about [REST API integration](examples-rest.md)
3. Check out [Redis protocol examples](examples-redis.md)
4. Dive into [PostgreSQL protocol examples](examples-postgresql.md)
5. Understand the [Data Model](data-model.md) in depth
6. Leverage [Version Control Features](version-control.md)
7. Consider [Performance and Scalability](performance.md) for production use

## Troubleshooting

### Common Issues

1. **Connection refused**: Make sure ChronDB is running and the ports are correctly exposed
2. **Authentication failure** (PostgreSQL): Use the default username/password (chrondb/chrondb)
3. **Data persistence**: Ensure you've mounted a volume for Docker or set the data directory

### Getting Help

If you encounter issues:

- Check the [Frequently Asked Questions](faq.md)
- Open an issue on [GitHub](https://github.com/moclojer/chrondb/issues)
- Join our community chat

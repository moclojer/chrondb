# Getting Started with ChronDB

This quick start guide will have you up and running with ChronDB in minutes, with clear examples for each supported interface.

## Step 1: Installation

Choose the installation method that works best for you:

### 🐳 Using Docker (Simplest)

```bash
# Start ChronDB with all protocols enabled
docker run -d --name chrondb \
  -p 3000:3000 \  # REST API
  -p 6379:6379 \  # Redis protocol
  -p 5432:5432 \  # PostgreSQL protocol
  -v chrondb-data:/data \
  moclojer/chrondb:latest
```

### 🧪 Using JAR File

```bash
# Download the latest release
curl -L -o chrondb.jar https://github.com/moclojer/chrondb/releases/latest/download/chrondb.jar

# Run ChronDB
java -jar chrondb.jar
```

### 🔧 For Clojure Developers

```bash
# Clone the repository
git clone https://github.com/moclojer/chrondb.git
cd chrondb

# Start the server
clojure -M:run
```

## Step 2: Verify Installation

Let's make sure everything is working correctly:

```bash
# Check the REST API
curl http://localhost:3000/api/v1/health

# Expected response:
# {"status":"ok","version":"0.1.0"}
```

## Step 3: Choose Your Interface

ChronDB offers multiple ways to connect. Let's try each one with a simple example:

### 📡 Using the REST API

The REST API is the most universal interface, accessible from any language with HTTP capabilities.

```bash
# Create your first document
curl -X POST http://localhost:3000/api/v1/documents/greeting:hello \
  -H "Content-Type: application/json" \
  -d '{"message": "Hello, ChronDB!", "created": "2023-06-01T12:00:00Z"}'

# Retrieve the document
curl http://localhost:3000/api/v1/documents/greeting:hello

# Update the document
curl -X POST http://localhost:3000/api/v1/documents/greeting:hello \
  -H "Content-Type: application/json" \
  -d '{"message": "Hello, ChronDB!", "created": "2023-06-01T12:00:00Z", "updated": "2023-06-02T15:30:00Z"}'

# View the document history
curl http://localhost:3000/api/v1/documents/greeting:hello/history
```

### 🔄 Using the Redis Protocol

If you're familiar with Redis, you'll feel right at home:

```bash
# Connect with redis-cli
redis-cli -p 6379

# Create a document
SET user:101 '{"name":"Alex","email":"alex@example.com"}'

# Retrieve the document
GET user:101

# Update the document
SET user:101 '{"name":"Alex","email":"alex@example.com","role":"editor"}'

# View document history (ChronDB-specific command)
CHRONDB.HISTORY user:101
```

### 🗄️ Using the PostgreSQL Protocol

Connect with your favorite SQL tools:

```bash
# Connect with psql
psql -h localhost -p 5432 -U chrondb -d chrondb -W
# Password is 'chrondb' by default

# Create a document as a row
INSERT INTO product (id, name, price, stock)
VALUES ('123', 'Ergonomic Chair', 299.99, 10);

# Query the document
SELECT * FROM product WHERE id = '123';

# Update the document
UPDATE product SET stock = 9, last_sold = '2023-06-15' WHERE id = '123';

# View document history
SELECT * FROM chrondb_history('product', '123');
```

### 🧩 Using the Clojure API

For Clojure applications, add ChronDB as a dependency:

```clojure
;; In deps.edn
{:deps {com.github.moclojer/chrondb {:git/tag "v0.1.0"
                                     :git/sha "..."}}}
```

Then use it in your code:

```clojure
(require '[chrondb.core :as chrondb])

;; Create a database connection
(def db (chrondb/create-chrondb))

;; Create a document
(chrondb/save db "order:1001"
  {:customer "Pat Smith"
   :items [{:product "123" :quantity 1}]
   :total 299.99
   :status "pending"})

;; Retrieve the document
(def order (chrondb/get db "order:1001"))

;; Update the document
(chrondb/save db "order:1001"
  (assoc order :status "shipped"))

;; View document history
(def history (chrondb/history db "order:1001"))
```

## Step 4: Explore Time Travel Features

Now let's try ChronDB's most powerful feature - time travel:

### REST API Time Travel

```bash
# Get document at a specific point in time
curl http://localhost:3000/api/v1/documents/greeting:hello/at/2023-06-01T13:00:00Z

# Compare versions
curl http://localhost:3000/api/v1/documents/greeting:hello/diff \
  -d '{"from":"2023-06-01T12:00:00Z","to":"2023-06-02T16:00:00Z"}'
```

### Redis Protocol Time Travel

```bash
# Get document at a specific commit/timestamp
CHRONDB.GETAT user:101 "2023-06-10T15:00:00Z"

# Compare versions
CHRONDB.DIFF user:101 "2023-06-10T15:00:00Z" "2023-06-15T18:00:00Z"
```

### PostgreSQL Protocol Time Travel

```sql
-- Get document at a specific point in time
SELECT * FROM chrondb_at('product', '123', '2023-06-15T10:00:00Z');

-- Compare versions
SELECT * FROM chrondb_diff('product', '123',
  '2023-06-14T00:00:00Z', '2023-06-16T00:00:00Z');
```

### Clojure API Time Travel

```clojure
;; Get document at a specific point in time
(def old-order (chrondb/get-at db "order:1001" "2023-06-10T12:00:00Z"))

;; Compare versions
(def changes (chrondb/diff db "order:1001"
              "2023-06-10T12:00:00Z" "2023-06-11T12:00:00Z"))
```

## Step 5: Try Branching (Optional)

ChronDB's Git foundation enables powerful branching:

### REST API Branching

```bash
# Create a test branch
curl -X POST http://localhost:3000/api/v1/branches/test

# Switch to the test branch
curl -X PUT http://localhost:3000/api/v1/branches/test/checkout

# Make a change in the test branch
curl -X POST http://localhost:3000/api/v1/documents/greeting:hello \
  -H "Content-Type: application/json" \
  -d '{"message": "Hello from test branch!", "created": "2023-06-01T12:00:00Z"}'

# Compare with main branch
curl http://localhost:3000/api/v1/branches/main/documents/greeting:hello
curl http://localhost:3000/api/v1/branches/test/documents/greeting:hello

# Merge test branch to main
curl -X POST http://localhost:3000/api/v1/branches/test/merge/main
```

### Clojure API Branching

```clojure
;; Create a test branch
(def test-db (chrondb/create-branch db "test"))

;; Make changes in the test branch
(chrondb/save test-db "order:1001"
  (assoc (chrondb/get test-db "order:1001")
         :status "canceled"))

;; Compare changes
(println (chrondb/get db "order:1001"))  ;; Main branch
(println (chrondb/get test-db "order:1001"))  ;; Test branch

;; Merge test changes to main
(chrondb/merge-branch db "test" "main")
```

## Next Steps

Congratulations! You've taken your first steps with ChronDB. Here's where to go next:

- [Data Model](data-model) - Learn more about ChronDB's document structure
- [Protocol References](protocols) - Complete protocol documentation
- [Configuration](configuration) - Customize your ChronDB instance
- [Examples](examples) - More detailed usage examples

Need help? Check our [FAQ](faq) or join our community:

- [Discord Community](https://discord.com/channels/1099017682487087116/1353399752636497992)
- [GitHub Discussions](https://github.com/moclojer/chrondb/discussions)
- [Official Documentation](https://chrondb.moclojer.com/)

**Happy time traveling with ChronDB!**

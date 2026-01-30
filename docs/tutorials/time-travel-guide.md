# Time Travel with ChronDB: A Step-by-Step Tutorial

One of ChronDB's most powerful features is **time travel** - the ability to view, query, and work with your data as it existed at any point in time. This tutorial will guide you through practical examples of how to use time travel in your applications.

## What is Time Travel?

In ChronDB, every change to your data is preserved. Nothing is ever truly deleted or overwritten. This means you can:

- View any document as it existed at a specific point in time
- Compare different versions of a document
- Restore previous versions
- Query your entire database as it existed in the past
- Analyze how your data has evolved

## Time Travel Use Cases

Time travel is invaluable for many scenarios:

- **Audit trails** - See who changed what and when
- **Debugging** - Understand when a data issue was introduced
- **Compliance** - Fulfill regulatory requirements for data history
- **Analytics** - Compare trends over time
- **Undo** - Recover from mistakes or data corruption

## Tutorial Setup

Before starting, make sure you have ChronDB running:

```bash
docker run -d --name chrondb \
  -p 3000:3000 -p 6379:6379 -p 5432:5432 \
  -v chrondb-data:/data \
  avelino/chrondb:latest
```

## Exercise 1: Creating Documents with History

Let's create a document and track its evolution:

### Using the REST API

```bash
# Create initial customer document
curl -X POST http://localhost:3000/api/v1/documents/customer:1001 \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Acme Corp",
    "contact": "John Doe",
    "email": "john@acme.com",
    "plan": "starter",
    "created": "2023-01-01T10:00:00Z"
  }'

# Note the timestamp for reference
TIMESTAMP1=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
echo "First version created at: $TIMESTAMP1"

# Wait a moment, then update the document
sleep 5

# Update the customer plan
curl -X POST http://localhost:3000/api/v1/documents/customer:1001 \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Acme Corp",
    "contact": "John Doe",
    "email": "john@acme.com",
    "plan": "professional",
    "created": "2023-01-01T10:00:00Z",
    "upgraded": "2023-03-15T14:30:00Z"
  }'

# Note the second timestamp
TIMESTAMP2=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
echo "Second version created at: $TIMESTAMP2"

# Wait again, then update once more
sleep 5

# Update contact information
curl -X POST http://localhost:3000/api/v1/documents/customer:1001 \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Acme Corp",
    "contact": "Jane Smith",
    "email": "jane@acme.com",
    "plan": "professional",
    "created": "2023-01-01T10:00:00Z",
    "upgraded": "2023-03-15T14:30:00Z",
    "contact_changed": "2023-05-20T09:15:00Z"
  }'

TIMESTAMP3=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
echo "Third version created at: $TIMESTAMP3"
```

## Exercise 2: Viewing Document History

Now that we have a document with multiple versions, let's explore its history:

### REST API

```bash
# Get the complete document history
curl http://localhost:3000/api/v1/documents/customer:1001/history
```

### Redis Protocol

```bash
# Connect to Redis interface
redis-cli -p 6379

# Get document history
CHRONDB.HISTORY customer:1001
```

### PostgreSQL Protocol

```bash
# Connect to PostgreSQL interface
psql -h localhost -p 5432 -U chrondb -d chrondb -W
# Password: chrondb

# View document history
SELECT * FROM chrondb_history('customer', '1001');
```

### Clojure API

```clojure
(require '[chrondb.core :as chrondb])

(def db (chrondb/create-chrondb))

;; Get document history
(def history (chrondb/history db "customer:1001"))

;; Print each version
(doseq [version history]
  (println "Timestamp:" (:timestamp version))
  (println "Data:" (:data version))
  (println "---"))
```

## Exercise 3: Time-Travel Queries

Now let's retrieve specific versions of our document:

### REST API

```bash
# Get the document as it was at the first timestamp
curl http://localhost:3000/api/v1/documents/customer:1001/at/$TIMESTAMP1

# Get the document as it was after the plan upgrade
curl http://localhost:3000/api/v1/documents/customer:1001/at/$TIMESTAMP2

# Get the current version
curl http://localhost:3000/api/v1/documents/customer:1001
```

### Redis Protocol

```bash
# In redis-cli
CHRONDB.GETAT customer:1001 "$TIMESTAMP1"
CHRONDB.GETAT customer:1001 "$TIMESTAMP2"
GET customer:1001  # Current version
```

### PostgreSQL Protocol

```sql
-- In psql
SELECT * FROM chrondb_at('customer', '1001', '$TIMESTAMP1');
SELECT * FROM chrondb_at('customer', '1001', '$TIMESTAMP2');
SELECT * FROM customer WHERE id = '1001';  -- Current version
```

### Clojure API

```clojure
;; Get specific versions
(def version1 (chrondb/get-at db "customer:1001" "$TIMESTAMP1"))
(def version2 (chrondb/get-at db "customer:1001" "$TIMESTAMP2"))
(def current (chrondb/get db "customer:1001"))

(println "Version 1:" version1)
(println "Version 2:" version2)
(println "Current:" current)
```

## Exercise 4: Comparing Versions

Let's examine what changed between versions:

### REST API

```bash
# Compare the first and second versions
curl -X POST http://localhost:3000/api/v1/documents/customer:1001/diff \
  -H "Content-Type: application/json" \
  -d '{
    "from": "'$TIMESTAMP1'",
    "to": "'$TIMESTAMP2'"
  }'
```

### Redis Protocol

```bash
# In redis-cli
CHRONDB.DIFF customer:1001 "$TIMESTAMP1" "$TIMESTAMP2"
```

### PostgreSQL Protocol

```sql
-- In psql
SELECT * FROM chrondb_diff('customer', '1001', '$TIMESTAMP1', '$TIMESTAMP2');
```

### Clojure API

```clojure
;; Compare versions
(def diff-1-2 (chrondb/diff db "customer:1001" "$TIMESTAMP1" "$TIMESTAMP2"))
(def diff-2-3 (chrondb/diff db "customer:1001" "$TIMESTAMP2" "$TIMESTAMP3"))

(println "Changes from v1 to v2:" diff-1-2)
(println "Changes from v2 to v3:" diff-2-3)
```

## Exercise 5: Reverting to a Previous Version

Sometimes you need to restore data to a previous state:

### Clojure API (most direct way)

```clojure
;; Get the version you want to restore
(def version-to-restore (chrondb/get-at db "customer:1001" "$TIMESTAMP1"))

;; Save it as the current version
(chrondb/save db "customer:1001" version-to-restore)

;; Verify
(println "Restored version:" (chrondb/get db "customer:1001"))
```

### REST API

```bash
# First, get the version you want to restore
old_version=$(curl -s http://localhost:3000/api/v1/documents/customer:1001/at/$TIMESTAMP1)

# Then save it as the current version
curl -X POST http://localhost:3000/api/v1/documents/customer:1001 \
  -H "Content-Type: application/json" \
  -d "$old_version"
```

## Exercise 6: Checking Who Made Changes (Audit Trail)

ChronDB tracks metadata about each change. Let's see who changed what:

### PostgreSQL Protocol (includes committer info)

```sql
-- In psql
SELECT commit_id, timestamp, committer, committer_email
FROM chrondb_history('customer', '1001');
```

### Clojure API with Metadata

```clojure
;; Get history with metadata
(def history-with-meta (chrondb/history db "customer:1001" {:include-metadata true}))

;; Show committer information for each version
(doseq [version history-with-meta]
  (println "Timestamp:" (:timestamp version))
  (println "Committer:" (:committer version))
  (println "Email:" (:committer-email version))
  (println "---"))
```

## Exercise 7: Branching and Time Travel

ChronDB's branching feature works seamlessly with time travel. Let's see how:

### Creating a Test Branch

```clojure
;; Create a test branch
(def test-db (chrondb/create-branch db "test"))

;; Make changes only in the test branch
(chrondb/save test-db "customer:1001"
  (assoc (chrondb/get test-db "customer:1001")
         :plan "enterprise"
         :notes "Considering enterprise upgrade"))

;; Compare the versions in different branches
(println "Main branch:" (chrondb/get db "customer:1001"))
(println "Test branch:" (chrondb/get test-db "customer:1001"))

;; Time travel in the test branch
(def test-history (chrondb/history test-db "customer:1001"))
(println "Test branch history:" test-history)
```

## Real-World Application: Feature Flagging with Time Travel

Let's implement a practical example: using ChronDB's time travel for feature flagging and rollbacks:

```clojure
;; Create a feature flags document
(chrondb/save db "system:feature-flags"
  {:dark-mode-enabled false
   :beta-features-enabled false
   :new-dashboard-enabled false
   :updated "2023-06-01T00:00:00Z"})

;; Record the initial timestamp
(def initial-timestamp "2023-06-01T00:00:00Z")

;; Later, enable some features
(chrondb/save db "system:feature-flags"
  {:dark-mode-enabled true
   :beta-features-enabled true
   :new-dashboard-enabled false
   :updated "2023-06-15T00:00:00Z"})

;; Record this timestamp
(def beta-timestamp "2023-06-15T00:00:00Z")

;; Later, enable all features
(chrondb/save db "system:feature-flags"
  {:dark-mode-enabled true
   :beta-features-enabled true
   :new-dashboard-enabled true
   :updated "2023-07-01T00:00:00Z"})

;; Oh no! The new dashboard has bugs, roll back to beta features only
(def beta-flags (chrondb/get-at db "system:feature-flags" beta-timestamp))
(chrondb/save db "system:feature-flags" beta-flags)

;; Verify we're back to the beta stage
(println "Current flags:" (chrondb/get db "system:feature-flags"))
```

## Conclusion

Time travel is one of ChronDB's most powerful features. By preserving the complete history of your data, ChronDB enables:

- Complete audit trails
- Point-in-time recovery
- Historical analysis
- Safe experimentation with branching
- Compliance with data retention requirements

In this tutorial, you've learned how to:

1. Create and update documents to build history
2. View the complete history of a document
3. Retrieve specific historical versions
4. Compare different versions
5. Revert to previous versions
6. Track who made changes
7. Use branching with time travel
8. Implement practical time travel patterns

For more advanced time travel techniques, check out the [Version Control](../version-control) documentation.

## Next Steps

- [Data Model](../data-model) - Understand ChronDB's document structure
- [Branching Guide](./branching-guide) - Learn more about branching
- [Performance Considerations](../performance) - Optimize for time travel queries

## Community Resources

- **Documentation**: [chrondb.avelino.run](https://chrondb.avelino.run/)
- **Questions & Help**: [Discord Community](https://discord.com/channels/1099017682487087116/1353399752636497992)
- **Share Your Experience**: [GitHub Discussions](https://github.com/avelino/chrondb/discussions)

# Data Model

ChronDB uses a document-oriented data model built on top of Git's version control system. This document explains the core concepts of this model and how data is organized, stored, and versioned.

## Core Concepts

ChronDB organizes data using the following hierarchy:

```
Repository
  └── Collections
       └── Documents
            └── Fields
```

### Documents

The primary unit of storage in ChronDB is a **document**. Documents are:

- JSON-compatible objects (maps, arrays, strings, numbers, booleans, and null)
- Identified by a unique key
- Schema-less (fields can vary between documents)
- Automatically versioned

Example document:

```json
{
  "name": "John Doe",
  "email": "john@example.com",
  "age": 30,
  "roles": ["admin", "user"],
  "address": {
    "street": "123 Main St",
    "city": "Anytown",
    "zip": "12345"
  }
}
```

### Document Keys

Each document is identified by a unique key that follows the convention:

```
collection:identifier
```

For example:

- `user:1234`
- `product:macbook-pro`
- `order:ORD-2023-0001`

The part before the colon (`user`, `product`, `order`) implicitly defines the collection, while the part after the colon is the identifier within that collection.

### Collections

Collections are logical groupings of related documents. Collections in ChronDB:

- Are implicitly created when documents are saved with a collection prefix
- Do not require explicit schema definition
- Can contain heterogeneous documents (different field structures)
- Are represented as directories in the underlying Git storage

## Storage Model

### Git-Based Storage

ChronDB maps its document model to Git's storage system:

1. Each document is stored as a separate file in the Git repository
2. The document's path is derived from its key
3. The document's content is stored as JSON
4. Each change to a document creates a Git commit
5. Document history is tracked through Git's commit history

For example, a document with key `user:1234` might be stored as:

```
repository/
  └── user/
       └── 1234.json
```

### Versioning Model

Every change to a document in ChronDB is automatically versioned:

1. When a document is created or updated, a new commit is created
2. Each commit includes:
   - The document's new state
   - A timestamp
   - Optional commit message or metadata
   - Reference to previous versions

This versioning system enables:

- Point-in-time snapshots of any document
- Complete audit trail of changes
- The ability to revert to previous versions
- Branch-based development and testing

## Data Types

ChronDB supports the following data types:

| Type | JSON Representation | Example |
|------|---------------------|---------|
| String | String | `"Hello World"` |
| Number | Number | `42`, `3.14159` |
| Boolean | Boolean | `true`, `false` |
| Array | JSON Array | `[1, 2, 3]`, `["red", "green", "blue"]` |
| Object | JSON Object | `{"name": "John", "age": 30}` |
| Null | null | `null` |
| Date* | String (ISO 8601) | `"2023-05-15T14:30:00Z"` |

\* Dates are typically stored as ISO 8601 strings and parsed by client libraries.

## Schema Flexibility

ChronDB is schema-less by default, meaning:

- Documents in the same collection can have different fields
- Fields can be added or removed without affecting other documents
- No up-front schema definition is required

However, applications can implement optional schema validation:

```clojure
;; Example: Register a validation hook
(chrondb/register-hook db :pre-save
  (fn [doc]
    (when (and (= (namespace (:_id doc)) "user")
               (not (:email doc)))
      (throw (ex-info "User documents must have an email" {:doc doc})))
    doc))
```

## Branches and Isolation

ChronDB's Git foundation enables powerful branching capabilities:

- Multiple branches can exist simultaneously
- Each branch can have its own version of the documents
- Changes in one branch do not affect others
- Branches can be merged to combine changes

This enables several workflows:

1. **Development/Testing**: Make changes in a development branch before merging to production
2. **Multi-Tenancy**: Use branches to isolate data between tenants
3. **Scenarios/Simulations**: Create branches to model different scenarios
4. **Feature Flags**: Implement features in separate branches before enabling them

## Querying and Indexes

ChronDB provides two main approaches for retrieving documents:

### Direct Access

- Retrieve documents by their key
- Extremely fast, constant-time operation

```
GET user:1234  # Direct key lookup
```

### Search Queries

- Search across document contents using query expressions
- Based on Lucene's query syntax
- Supports boolean operations, wildcards, ranges, and more

```
SEARCH name:John AND age:[25 TO 35]  # Search with multiple criteria
```

Indexes are maintained for fields commonly used in search queries to improve performance.

## Mapping to Other Protocols

ChronDB's data model is designed to be accessible through multiple protocols with consistent semantics:

### PostgreSQL Protocol

- Collections map to tables
- Document keys map to primary keys
- Document fields map to columns

### Redis Protocol

- Document keys map directly to Redis keys
- Document values are stored as JSON strings
- Special commands provide history and versioning functionality

## Transactions

ChronDB supports multi-document transactions to ensure atomic operations across multiple documents:

```clojure
(with-transaction [db]
  (save db "account:123" {:balance 500})
  (save db "account:456" {:balance 1500})
  (save db "transfer:789" {:from "123", :to "456", :amount 500}))
```

All changes within a transaction are committed together or rolled back if any operation fails.

## Time-Travel Queries

The versioned nature of ChronDB enables powerful time-travel capabilities:

- Retrieve any document as it existed at a specific point in time
- Compare document versions across time
- Query the database as it existed at a specific time

```clojure
;; Get document as it was on January 1, 2023
(get-at db "user:1234" "2023-01-01T00:00:00Z")

;; Compare versions
(diff db "user:1234" "2023-01-01T00:00:00Z" "2023-06-01T00:00:00Z")
```

## Conclusion

ChronDB's document-oriented model built on Git offers a flexible, versioned approach to data storage. By combining the simplicity of document databases with the power of version control, ChronDB provides a unique solution for applications that require data versioning, history tracking, and branching capabilities.

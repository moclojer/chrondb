# ChronDB API Reference

This document provides detailed information about the ChronDB API.

## Core API (Clojure/Java)

### Creating a Database

```clojure
(require '[chrondb.core :as chrondb])

;; Create with default configuration
(def db (chrondb/create-chrondb))

;; Create with custom configuration
(def db (chrondb/create-chrondb config))
```

### Document Operations

#### Save Document

```clojure
(chrondb/save db "key" value)
```

Parameters:

- `db`: ChronDB instance
- `key`: String identifier for the document
- `value`: Document data (will be stored as JSON)

Returns: The saved document

#### Get Document

```clojure
(chrondb/get db "key")
```

Parameters:

- `db`: ChronDB instance
- `key`: String identifier for the document

Returns: The document if found, nil otherwise

#### Delete Document

```clojure
(chrondb/delete db "key")
```

Parameters:

- `db`: ChronDB instance
- `key`: String identifier for the document

Returns: true if document was deleted, false otherwise

### Search Operations

#### Search Documents

```clojure
(chrondb/search db query)
(chrondb/search db query {:limit 10 :offset 0})
```

Parameters:

- `db`: ChronDB instance
- `query`: Lucene query string
- `options`: Optional map with:
  - `:limit`: Maximum number of results (default: 10)
  - `:offset`: Number of results to skip (default: 0)

Returns: Sequence of matching documents

### Version Control Operations

#### Get Document History

```clojure
(chrondb/history db "key")
```

Parameters:

- `db`: ChronDB instance
- `key`: String identifier for the document

Returns: Sequence of document versions with timestamps

#### Get Document at Point in Time

```clojure
(chrondb/get-at db "key" timestamp)
```

Parameters:

- `db`: ChronDB instance
- `key`: String identifier for the document
- `timestamp`: ISO-8601 timestamp string or java.time.Instant

Returns: The document as it existed at the specified time

#### Compare Document Versions

```clojure
(chrondb/diff db "key" timestamp1 timestamp2)
```

Parameters:

- `db`: ChronDB instance
- `key`: String identifier for the document
- `timestamp1`: First timestamp
- `timestamp2`: Second timestamp

Returns: Differences between versions

### Branch Operations

#### Create Branch

```clojure
(chrondb/create-branch db "branch-name")
```

Parameters:

- `db`: ChronDB instance
- `branch-name`: Name of the new branch

Returns: Updated ChronDB instance

#### Switch Branch

```clojure
(chrondb/switch-branch db "branch-name")
```

Parameters:

- `db`: ChronDB instance
- `branch-name`: Name of the branch to switch to

Returns: Updated ChronDB instance

#### Merge Branches

```clojure
(chrondb/merge-branch db "source-branch" "target-branch")
```

Parameters:

- `db`: ChronDB instance
- `source-branch`: Branch to merge from
- `target-branch`: Branch to merge into

Returns: Updated ChronDB instance

### Transaction Operations

```clojure
(chrondb/with-transaction [db]
  (chrondb/save db "key1" value1)
  (chrondb/save db "key2" value2))
```

All operations within the transaction block are atomic:

- Either all succeed or all fail
- Changes are only visible after successful commit
- Automatic rollback on failure

### Event Hooks

#### Register Hook

```clojure
(chrondb/register-hook db :pre-save
  (fn [doc]
    ;; Hook logic here
    doc))
```

Available hook points:

- `:pre-save`: Before saving a document
- `:post-save`: After saving a document
- `:pre-delete`: Before deleting a document
- `:post-delete`: After deleting a document

### Utility Functions

#### Health Check

```clojure
(chrondb/health-check db)
```

Returns: Map with health status information

#### Backup / Restore

```clojure
(chrondb.backup.core/create-full-backup storage {:output-path "backups/full.tar.gz"})
(chrondb.backup.core/create-incremental-backup storage {:output-path "backups/incr.bundle"
                                                       :base-commit "<commit>"
                                                       :format :bundle})
```

```clojure
(chrondb.backup.core/restore-backup storage {:input-path "backups/full.tar.gz"})
```

```clojure
(chrondb.backup.core/export-snapshot storage {:output "backups/main.bundle"
                                              :refs ["refs/heads/main"]})
```

```clojure
(chrondb.backup.core/import-snapshot storage {:input "backups/main.bundle"})
```

Scheduled backup example:

```clojure
(chrondb.backup.core/schedule-backup storage {:mode :bundle
                                              :format :bundle
                                              :interval-minutes 60
                                              :output-dir "backups/"})
```

#### Statistics

```clojure
(chrondb/stats db)
```

Returns: Map with database statistics

## Error Handling

All API functions may throw the following exceptions:

- `ChronDBException`: Base exception class
- `StorageException`: Storage-related errors
- `IndexException`: Index-related errors
- `ConfigurationException`: Configuration errors
- `ValidationException`: Data validation errors

Example error handling:

```clojure
(try
  (chrondb/save db "key" value)
  (catch chrondb.exceptions.StorageException e
    (log/error "Storage error:" (.getMessage e)))
  (catch chrondb.exceptions.ValidationException e
    (log/error "Validation error:" (.getMessage e))))

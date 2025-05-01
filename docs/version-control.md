# Version Control Features

ChronDB uniquely combines a document database with Git's powerful version control system, providing capabilities that traditional databases lack. This document explores the version control features of ChronDB and how they can be leveraged in your applications.

## Version Control Overview

Unlike traditional databases that only store the current state of data, ChronDB automatically tracks the complete history of every document:

- **All changes are versioned**: Every document update creates a new revision
- **Full history preservation**: Historical states are never lost
- **Git foundation**: Built on Git's proven version control infrastructure
- **First-class version management**: Version control is a core feature, not an add-on

## Document History and Revisions

### Automatic Change Tracking

Every time a document is created, modified, or deleted, ChronDB:

1. Creates a new version of the document
2. Records metadata about the change (timestamp, author)
3. Preserves the document's previous state
4. Links the revisions in a chronological history

```clojure
;; Create initial document
(chrondb/save db "user:1" {:name "John" :email "john@example.com"})

;; Update the document
(chrondb/save db "user:1" {:name "John" :email "john.doe@example.com"})

;; Each operation creates a new version in the history
```

### Accessing Document History

ChronDB provides API methods to access the complete history of any document:

```clojure
;; Get the complete history of a document
(def history (chrondb/history db "user:1"))

;; Example history result
;; [
;;   {:timestamp "2023-05-10T14:30:00Z",
;;    :data {:name "John", :email "john@example.com"}}
;;   {:timestamp "2023-05-15T09:45:00Z",
;;    :data {:name "John", :email "john.doe@example.com"}}
;; ]
```

ChronDB also provides low-level access to Git-based document history, allowing you to retrieve detailed commit information and document content at each version:

```clojure
;; Get detailed document history with Git commit metadata
(def git-history (chrondb/get-document-history db "user:1"))

;; Example git-history result
;; [
;;   {:commit-id "8f7e6d5c4b3a2...",
;;    :commit-time #inst "2023-05-15T09:45:00Z",
;;    :commit-message "Save document",
;;    :committer-name "ChronDB",
;;    :committer-email "system@chrondb.com",
;;    :document {:name "John", :email "john.doe@example.com"}}
;;   {:commit-id "1a2b3c4d5e6f7...",
;;    :commit-time #inst "2023-05-10T14:30:00Z",
;;    :commit-message "Save document",
;;    :committer-name "ChronDB",
;;    :committer-email "system@chrondb.com",
;;    :document {:name "John", :email "john@example.com"}}
;; ]
```

### Retrieving Specific Document Versions

You can retrieve a document at a specific commit by providing the commit hash. Note that each retrieval operation generates a new commit to maintain chronological integrity:

```clojure
;; Get a document at a specific commit
(def old-version (chrondb/get-document-at-commit db-repository "user:1" "1a2b3c4d5e6f7..."))

;; Example result
;; {:name "John", :email "john@example.com"}
```

Through other protocols, document history is also accessible:

**REST API:**

```
GET /api/v1/documents/user:1/history
```

**PostgreSQL Protocol:**

```sql
SELECT * FROM chrondb_history('user', '1');
```

**Redis Protocol:**

```
CHRONDB.HISTORY user:1
```

### Point-in-Time Access

A key feature of ChronDB is the ability to access any document as it existed at a specific point in time:

```clojure
;; Get document as it was on May 10, 2023
(def old-version (chrondb/get-at db "user:1" "2023-05-10T14:30:00Z"))
```

This enables:

- Historical reporting
- Audit trails
- Compliance requirements
- Debugging and troubleshooting
- "Time machine" capabilities for applications

## Branching

ChronDB extends Git's branching model to document databases, allowing multiple parallel versions of your data to exist simultaneously.

### Creating and Using Branches

```clojure
;; Create a new branch
(def test-db (chrondb/create-branch db "test"))

;; Make changes in the test branch
(chrondb/save test-db "user:1" {:name "John (Test)" :email "john.test@example.com"})

;; Changes in test branch don't affect main branch
(println (chrondb/get db "user:1"))        ;; Original version
(println (chrondb/get test-db "user:1"))   ;; Modified version

;; Switch the current branch
(def current-db (chrondb/switch-branch db "test"))
```

### Branch Use Cases

Branches enable powerful workflows:

1. **Development and Testing**
   - Use branches to develop and test new features without affecting production data
   - Validate data migrations before applying to production

2. **What-If Analysis**
   - Create branches to model different business scenarios
   - Run simulations with alternate data sets

3. **Multi-Tenancy**
   - Use branches to isolate data for different tenants
   - Apply schema changes to specific tenants

4. **Feature Toggles**
   - Implement unreleased features in separate branches
   - Merge when ready for production

5. **Temporary Workspaces**
   - Create temporary branches for exploratory analysis
   - Discard when no longer needed

### Branch Management

ChronDB provides APIs for managing branches:

```clojure
;; List all branches
(chrondb/list-branches db)  ;; => ["main", "test", "dev"]

;; Check current branch
(chrondb/current-branch db)  ;; => "main"

;; Delete a branch
(chrondb/delete-branch db "test")
```

## Merging

ChronDB allows changes from one branch to be incorporated into another through merging.

### Merge Operations

```clojure
;; Merge changes from test branch into main
(def merged-db (chrondb/merge-branch db "test" "main"))
```

### Merge Strategies

ChronDB supports different merge strategies:

1. **Fast-forward merge**
   - Applied when the target branch hasn't changed since the source branch was created
   - Results in a linear history

2. **Recursive merge**
   - Used when both branches have diverged
   - Combines changes from both branches

3. **Ours strategy**
   - Prioritizes the target branch in conflict resolution
   - Useful when you want to override changes from the source branch

4. **Theirs strategy**
   - Prioritizes the source branch in conflict resolution
   - Useful when you want to adopt all changes from the source branch

```clojure
;; Merge with a specific strategy
(chrondb/merge-branch db "test" "main" {:strategy :ours})
```

## Conflict Resolution

When changes in different branches conflict, ChronDB provides tools for resolution:

### Conflict Detection

ChronDB automatically detects conflicts during merge operations:

```clojure
;; Attempt merge that may have conflicts
(try
  (chrondb/merge-branch db "branch1" "branch2")
  (catch Exception e
    (if (= (:type (ex-data e)) :merge-conflict)
      (handle-conflict (:conflicts (ex-data e)))
      (throw e))))
```

### Conflict Resolution Strategies

1. **Manual Resolution**
   - Identify conflicting fields
   - Choose the correct value for each conflict
   - Create a resolution commit

2. **Automatic Policy-Based Resolution**
   - Define policies for automatic conflict resolution
   - Examples: "newest wins", "field-specific rules"

```clojure
;; Register a conflict resolution policy
(chrondb/register-conflict-resolver db
  (fn [doc1 doc2]
    ;; Implement your resolution logic
    (merge doc1 doc2)))
```

## Comparing and Diffing

ChronDB provides powerful tools for comparing document versions:

```clojure
;; Compare two versions of a document
(def diff (chrondb/diff db "user:1"
                       "2023-05-10T14:30:00Z"
                       "2023-05-15T09:45:00Z"))

;; Example diff result
;; {
;;   :added {}
;;   :removed {}
;;   :changed {:email ["john@example.com" "john.doe@example.com"]}
;; }
```

This enables:

- Visualizing changes over time
- Understanding data evolution
- Debugging unexpected changes
- Audit and compliance reporting

## Tagging and Snapshots

ChronDB allows you to tag specific points in your database's history:

```clojure
;; Create a tag/snapshot
(chrondb/create-tag db "release-2023-q2")

;; List all tags
(chrondb/list-tags db)  ;; => ["release-2023-q1", "release-2023-q2"]

;; Get database state at a specific tag
(def q2-db (chrondb/checkout-tag db "release-2023-q2"))
```

Use cases for tagging:

- Mark significant application releases
- Create quarterly snapshots for reporting
- Tag before major data migrations
- Create recovery points

## Auditing and Compliance

ChronDB's version control features naturally support auditing and compliance requirements:

### Audit Trail

Every change in ChronDB is tracked with:

- What changed (the data diff)
- When it changed (timestamp)
- Who changed it (committer information)
- Why it changed (commit message)

```clojure
;; Save with audit information
(chrondb/save db "user:1"
             {:name "John", :email "john@example.com"}
             {:message "Updated email address"
              :author "admin@example.com"})
```

### Compliance Features

ChronDB helps meet regulatory requirements:

- **GDPR**: Track all changes to personal data
- **HIPAA**: Maintain comprehensive audit trails
- **SOX**: Ensure financial data integrity and history
- **21 CFR Part 11**: Support for electronic records in regulated industries

## Rollback and Reversion

ChronDB makes it easy to revert to previous states:

```clojure
;; Revert a document to a previous state
(chrondb/revert db "user:1" "2023-05-10T14:30:00Z")

;; Revert an entire collection
(chrondb/revert-collection db "user" "2023-05-10T14:30:00Z")

;; Revert the entire database
(chrondb/revert-all db "2023-05-10T14:30:00Z")
```

For git-based storage, you can also restore a document to a specific version by commit hash while preserving the complete history:

```clojure
;; Restore a document to a specific version by commit hash
(def restored-doc (chrondb/restore-document-version db "user:1" "1a2b3c4d5e6f7..."))

;; This creates a new commit that reverts the document to that version,
;; while preserving the complete history of changes
```

Unlike a traditional rollback that might discard history, this approach adds a new restoration commit that preserves the complete chronology of changes, ensuring full audit trails are maintained.

This enables:

- Recovering from errors
- Undoing problematic changes
- Testing with real data then reverting
- Maintaining a complete audit trail of all operations

## Performance Considerations

Version control adds powerful capabilities but requires understanding some performance implications:

1. **Storage Growth**
   - Historical versions increase storage requirements
   - Git's compression and deduplication help minimize impact

2. **Operation Overhead**
   - Each write includes versioning overhead
   - Reading the latest version remains efficient
   - Historical queries have additional cost

3. **Optimization Strategies**
   - Use `git gc` for repository optimization
   - Consider repository sharding for very large datasets
   - Use appropriate indexing for common queries

## Best Practices

### Commit Messages and Metadata

Add meaningful context to changes:

```clojure
(chrondb/save db "order:1234"
             updated-order
             {:message "Fixed incorrect shipping address"
              :author "customer-service@example.com"})
```

### Branch Organization

Establish branch naming conventions:

- `feature/...` for new features
- `fix/...` for bug fixes
- `customer/...` for customer-specific branches

### Backup and Migration

Even though ChronDB preserves history, regular backups are recommended:

```clojure
;; Create a backup
(chrondb/backup db "/path/to/backup/location")
```

## Conclusion

ChronDB's version control features provide unprecedented capabilities for tracking, managing, and leveraging data history. By combining the power of Git with a document database, ChronDB enables new workflows and solutions to problems that traditional databases can't easily address.

Whether you need audit trails for compliance, branches for development and testing, or time-travel for analysis, ChronDB's version control features offer a robust foundation for building temporally-aware applications.

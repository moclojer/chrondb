# Remote Storage

ChronDB supports remote Git repositories as a synchronization backend. This enables distributed deployments where multiple ChronDB nodes share the same database through a central Git remote (GitHub, GitLab, or any Git server).

## Architecture

```
┌─────────────────┐     push/pull     ┌─────────────────┐
│  ChronDB Node A │ ◄──────────────── │  Remote Git     │
│  (bare repo)    │ ──────────────► │  (GitHub, etc)  │
└─────────────────┘                   └─────────────────┘
                                             ▲
┌─────────────────┐     push/pull            │
│  ChronDB Node B │ ◄───────────────────────►│
│  (bare repo)    │                          │
└─────────────────┘
```

Each ChronDB node maintains a local bare Git repository. On every write operation (save/delete), changes are committed locally and optionally pushed to the remote. On initialization, nodes can pull the latest state from the remote.

## Configuration

Add the remote settings to your `config.edn`:

```clojure
{:git {:committer-name "ChronDB"
       :committer-email "chrondb@example.com"
       :default-branch "main"

       ;; Remote repository URL (SSH or HTTPS)
       :remote-url "git@github.com:your-org/your-db-repo.git"

       ;; Push settings
       :push-enabled true     ; Enable automatic push after commits
       :push-notes true       ; Push transaction metadata (refs/notes/chrondb)
       :push-mode :sync       ; :sync = per-commit, :batch = deferred

       ;; Pull settings
       :pull-on-start true    ; Pull latest on storage initialization

       ;; SSH settings (only for git@ URLs)
       :ssh {}                ; Uses system ssh-agent by default
       }

 :storage {:data-dir "data"}}
```

### Configuration Options

| Key | Default | Description |
|-----|---------|-------------|
| `:remote-url` | `nil` | Remote repository URL. `nil` = local only |
| `:push-enabled` | `true` | Push to remote after each commit |
| `:push-notes` | `true` | Include transaction notes in push/fetch |
| `:push-mode` | `:sync` | `:sync` = push per commit, `:batch` = deferred |
| `:pull-on-start` | `true` | Fetch & merge from remote on init |
| `:ssh` | `{}` | SSH config (`:ssh-dir`, `:auth-methods`) |

### SSH Authentication

ChronDB uses the system's SSH agent for authentication. Ensure your SSH key is loaded:

```bash
# Verify ssh-agent has your key
ssh-add -l

# If not, add it
ssh-add ~/.ssh/id_rsa
```

Custom SSH settings:

```clojure
:ssh {:ssh-dir "/custom/path/.ssh"
      :auth-methods "publickey"}
```

## Usage

### Basic: Local + Remote Sync

```clojure
(require '[chrondb.storage.git.core :as git-core]
         '[chrondb.storage.protocol :as storage])

;; Config with remote-url will auto-setup origin and pull on start
(def my-storage (git-core/create-git-storage "/path/to/db"))

;; Every save automatically pushes to remote
(storage/save-document my-storage {:id "user:1" :_table "user" :name "Alice"})

;; Close when done
(storage/close my-storage)
```

### Clone from Existing Remote

Initialize a new node from an existing remote database:

```clojure
;; Clone downloads all data from remote
(def my-storage
  (git-core/clone-git-storage
    "git@github.com:your-org/your-db-repo.git"
    "/path/to/local/db"))

;; All documents are immediately available
(storage/get-document my-storage "user:1")
;; => {:id "user:1" :_table "user" :name "Alice"}
```

### Reopen Existing Storage

Open a previously created repository without reinitializing:

```clojure
(def my-storage (git-core/open-git-storage "/path/to/db"))
```

### Batch Push (Performance)

When saving multiple documents, use batch mode to defer the push to the end:

```clojure
;; Without batch: 5 documents = 5 pushes = ~10 seconds
;; With batch:    5 documents = 1 push   = ~2 seconds

(git-core/with-batch my-storage
  (storage/save-document my-storage {:id "order:1" :_table "order" :total 100})
  (storage/save-document my-storage {:id "order:2" :_table "order" :total 200})
  (storage/save-document my-storage {:id "order:3" :_table "order" :total 300})
  (storage/save-document my-storage {:id "order:4" :_table "order" :total 400})
  (storage/save-document my-storage {:id "order:5" :_table "order" :total 500}))
;; Single push happens here with all 5 commits
```

### Manual Pull/Push/Fetch

```clojure
;; Pull latest from remote (fetch + fast-forward)
(git-core/pull my-storage)
;; => :pulled, :current, :skipped, or :conflict

;; Push local changes to remote
(git-core/push my-storage)
;; => :pushed, :skipped, :deferred

;; Fetch without merging
(git-core/fetch my-storage)
;; => :fetched, :skipped
```

## Transaction Notes

ChronDB stores transaction metadata as Git notes at `refs/notes/chrondb`. When `:push-notes` is enabled, these are synchronized with the remote, providing a complete audit trail across all nodes.

Each commit note contains:
- `tx_id` - Transaction ID
- `origin` - API origin (REST, Redis, SQL)
- `timestamp` - Operation timestamp
- `operation` - save-document, delete-document
- `document_id` - Affected document
- `branch` - Target branch

## Push Modes

### Sync Mode (`:sync`)

Default. Each `save-document` or `delete-document` triggers an immediate push. Best for:
- Single-node deployments
- Low write volume
- Real-time sync requirements

### Batch Mode (`:batch`)

Push is deferred until the end of a `with-batch` block. Best for:
- Bulk imports
- High write volume
- ETL pipelines
- Migrations

## Error Handling

### Push Failures

Push failures do **not** block local operations. Documents are saved locally and can be retried:

```clojure
;; Push failure doesn't prevent local save
(try
  (storage/save-document my-storage doc)
  (catch Exception e
    ;; Document is saved locally, push failed
    ;; Retry push later:
    (git-core/push my-storage)))
```

### Conflict Resolution

When local and remote have diverged (concurrent writes from different nodes), `pull` returns `:conflict`. ChronDB uses force-push semantics - the last writer wins at the remote level, but all history is preserved locally through Git's commit graph.

For production multi-writer setups, consider:
- Using separate branches per node
- Implementing application-level conflict resolution
- Using a locking mechanism for critical writes

## Network Requirements

| Operation | Network Required | Fallback |
|-----------|-----------------|----------|
| `save-document` | Only for push | Saves locally, push retried later |
| `get-document` | No | Always reads from local |
| `with-batch` | Only at end | All commits saved locally |
| `pull` | Yes | Returns `:skipped` if no remote |
| `clone-git-storage` | Yes | Fails if unreachable |

## Dependencies

Remote storage requires the following JGit SSH dependencies (already included in ChronDB):

```clojure
org.eclipse.jgit/org.eclipse.jgit.ssh.apache       {:mvn/version "6.7.0.202309050840-r"}
org.eclipse.jgit/org.eclipse.jgit.ssh.apache.agent  {:mvn/version "6.7.0.202309050840-r"}
```

These provide:
- Apache MINA SSHD for SSH transport
- SSH agent support for passwordless authentication (macOS, Linux)

## Protocols (Redis/SQL/REST)

Remote sync works transparently with all ChronDB protocols. When a client saves a document via Redis `SET`, PostgreSQL `INSERT`, or REST `POST`, the commit is pushed to the remote automatically.

```bash
# Via Redis
redis-cli SET user:alice '{"name":"Alice","email":"alice@example.com"}'
# → Committed and pushed to remote

# Via PostgreSQL
psql -c "INSERT INTO users VALUES ('alice', 'Alice', 'alice@example.com')"
# → Committed and pushed to remote

# Via REST
curl -X POST http://localhost:3000/api/documents \
  -d '{"id":"user:alice","name":"Alice"}'
# → Committed and pushed to remote
```

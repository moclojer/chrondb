# ChronDB: The Time-Traveling Database

**Welcome to ChronDB!** A modern, Git-based database that remembers everything.

## What is ChronDB?

ChronDB is a chronological database built on Git's powerful architecture that gives your data complete version history. Think of it as "Git for your database" - every change is tracked, every version is accessible, and you can time-travel through your data's complete history.

## Why Choose ChronDB?

- **Complete History** - Never lose data again. Every change is preserved in your database's timeline.
- **Time Travel** - Query your data as it existed at any point in time.
- **Branching & Merging** - Create isolated environments for testing, development, or experimentation.
- **Multiple Interfaces** - Connect through Clojure, REST API, Redis protocol, or even PostgreSQL protocol.
- **ACID Compliance** - Enjoy full transaction support with commit, rollback, and consistency guarantees.
- **Schema-less** - Store any JSON-compatible data without rigid structure requirements.

## How It Works

ChronDB leverages Git's internal structure as a storage engine:

```
Repository (database)
  └── Branches (schemas/environments)
       └── Directories (tables)
            └── Files (documents as JSON)
```

Each document is a file in the Git repository, and each change creates a commit in the history. This gives you all the power of Git for your database:

- See diffs between versions
- Browse commit history
- Create branches for development
- Merge changes between branches
- Tag important versions
- Hook into events

## Quick Example

Here's how simple it is to use ChronDB with its Clojure API:

```clojure
;; Create/connect to a database
(def db (chrondb/create-chrondb))

;; Create a user document
(chrondb/save db "user:1" {:name "Alice" :email "alice@example.com"})

;; Update the document
(chrondb/save db "user:1" {:name "Alice" :email "alice@example.com" :role "admin"})

;; Time travel: see how the document looked yesterday
(def yesterday (chrondb/get-at db "user:1" "2023-07-15T00:00:00Z"))

;; See all versions of the document
(def history (chrondb/history db "user:1"))
```

## Choose Your Interface

ChronDB speaks multiple protocols so you can connect with your favorite tools:

- **Native Clojure API** - Direct, powerful access from Clojure applications
- **REST API** - HTTP interface for any language or client
- **Redis Protocol** - Use Redis clients to connect directly
- **PostgreSQL Protocol** - Connect with SQL clients and tools

## Start Your Journey

New to ChronDB? Here's how to get started:

1. [Quick Start Guide](quickstart) - Up and running in minutes
2. [Core Concepts](data-model) - Understand ChronDB's data model
3. [Examples](examples) - See ChronDB in action
4. [API Reference](api) - Complete API documentation

## Ready to dive in?

→ [Get Started Now](quickstart)

## Community Resources

- **Official Documentation**: [chrondb.moclojer.com](https://chrondb.moclojer.com/)
- **Join our Community**: [Discord](https://discord.com/channels/1099017682487087116/1353399752636497992)
- **Discussions & Ideas**: [GitHub Discussions](https://github.com/moclojer/chrondb/discussions)

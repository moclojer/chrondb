# ChronDB Documentation

ChronDB is a chronological database based on Git's internal architecture, implementing a key/value storage system with complete modification history.

## Key Features

- Immutable database with atomic transactions
- ACID compliance
- Schema-less
- Complete chronological storage
- Multiple access interfaces:
  - Native Clojure/Java API
  - **REST API:** communication via HTTP protocol
  - **Redis protocol:** use the Redis driver to communicate with chrondb
  - **PostgreSQL protocol:** in query within document, via PostgreSQL clients

Understand how and when changes were made. **chrondb** stores all history, and lets you query against any point in time.

[Git structure](https://git-scm.com/book/en/v2/Git-Internals-Git-Objects) is a powerful solution for storing **"data"** (files) in chronological order, _chrondb_ uses git core as a data structure to structure the data timeline, making it possible to return to any necessary point and bringing all git functions for a database:

- diff
- notes
- restore
- branch
- checkout
- revert
- merge
- log
- blame
- archive
- [hooks](https://git-scm.com/docs/githooks#_hooks)
- ... [git high-level commands (porcelain)](https://git-scm.com/docs/git#_high_level_commands_porcelain)

## Concept

ChronDB leverages Git's internal structure as a storage engine, offering all the advantages of version control for your data:

- Access to complete modification history
- Ability to access any previous version of data
- Operations like diff, branch, merge and more
- Complete audit trail of changes

> The goal is to speak the same language as the database world

- **database:** _git_ repository (local or remotely)
- **scheme:** _git_ branch
- **table:** directory added on _git_ repository
- **field struct:** json (document) - will be persisted in a file and indexed in _lucene_

## Getting Started

Navigate through the documentation to learn how to:

- [Configure](configuration.md) your ChronDB instance
- Use the [API](api.md) for database operations
- Connect through various [protocols](protocols.md)
- See [examples](examples.md) of common use cases

# chrondb

_Chronological **key/value** Database storing based on database-shaped `git` (core) architecture and Lucene for indexing._

## Features

We are:

- Immutable and atomic data
- ACID transactions
- Schemaless
- Chronological
- **RESTfull server:** communication via HTTP protocol
- **Redis protocol:** use the Redis driver to communicate with chrondb
- **SQL compliance:** in query within document

Understand how and when changes were made. **chrondb** stores all history, and lets you query against any point in time.

Git structure is a powerful solution for storing **"data"** (files) in chronological order, _chrondb_ uses git core as a data structure to structure the data timeline, making it possible to return to any necessary point and bringing all git functions for a database:

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

## Term alignment, from database to git

> The goal is to speak the same language as the database world

- database: _git_ repository (local or remotely)
- scheme: _git_ branch
- table: directory added on _git_ repository
- field struct: json (document) - will be persisted in a file and indexed in _lucene_

## Configuration

ChronDB uses an EDN configuration file to define its settings. To get started, copy the `config.example.edn` file to `config.edn`:

```bash
cp config.example.edn config.edn
```

### Configuration File Structure

The configuration file is divided into three main sections:

#### 1. Git Configuration (:git)

```clojure
:git {
  :committer-name "ChronDB"      ; Name used in commits
  :committer-email "chrondb@example.com"  ; Email used in commits
  :default-branch "main"         ; Default repository branch
  :sign-commits false            ; Whether to sign commits with GPG
}
```

#### 2. Storage Configuration (:storage)

```clojure
:storage {
  :data-dir "data"              ; Directory where data will be stored
}
```

#### 3. Logging Configuration (:logging)

```clojure
:logging {
  :level :info                  ; Log level (:debug, :info, :warn, :error)
  :output :stdout               ; Log output (:stdout or :file)
  :file "chrondb.log"          ; Log file (when output is :file)
}
```

### Log Levels

- `:debug` - Detailed information for development/debugging
- `:info` - General operation information
- `:warn` - Warnings that don't affect functionality
- `:error` - Errors that may affect functionality

### Usage Example

```clojure
(require '[chrondb.config :as config])

;; Load configuration from config.edn
(def chrondb-config (config/load-config))

;; Or specify a different file
(def custom-config (config/load-config "custom-config.edn"))
```

## Development

To run the example with default configuration:

```bash
clj -X:example
```

To run tests:

```bash
clj -X:test
```

## Installation

Add the following dependency to your `deps.edn`:

```clojure
{:deps {com.github.chrondb/chrondb {:git/tag "v0.1.0"
                                   :git/sha "..."}}}
```

## Quick Start

```clojure
(require '[chrondb.core :as chrondb])

;; Create a new ChronDB instance
(def db (chrondb/create-chrondb))

;; Save a document
(chrondb/save db "user:1" {:name "John" :age 30})

;; Get a document
(chrondb/get db "user:1")

;; Search documents
(chrondb/search db "name:John")

;; Get document history
(chrondb/history db "user:1")

;; Get document at specific point in time
(chrondb/get-at db "user:1" "2024-01-01T00:00:00Z")
```

## Requirements

- Java 11 or later
- Git 2.25.0 or later

## Documentation

For more detailed documentation, see:

- [Configuration Guide](docs/configuration.md)
- [API Reference](docs/api.md)
- [Example Usage](src/chrondb/example.clj)

For generated API documentation, run:

```bash
clj -X:codox
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.

## License

This project is licensed under the **MIT License** - see the [LICENSE](LICENSE) file for details.

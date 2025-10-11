# ChronDB: The Time-Traveling Database

_Chronological **key/value** Database built on Git architecture with complete version history._

[![GitHub License](https://img.shields.io/github/license/moclojer/chrondb)](https://github.com/moclojer/chrondb/blob/main/LICENSE)
[![Discord](https://img.shields.io/discord/1099017682487087116?label=discord)](https://discord.gg/w3Rnafyc)
[![Documentation](https://img.shields.io/badge/docs-chrondb.moclojer.com-blue)](https://chrondb.moclojer.com/)

## ✨ Features

ChronDB offers unique capabilities for your data:

- **Complete History** - Every change is preserved, nothing is ever truly deleted
- **Time Travel** - Query your data as it existed at any point in time
- **Multiple Interfaces**:
  - 🧩 **Native Clojure API** - Direct integration for JVM applications
  - 📡 **REST API** - HTTP interface for any language
  - 🔄 **Redis Protocol** - Connect using Redis clients
  - 🗄️ **PostgreSQL Protocol** - Connect with SQL clients
- **ACID Transactions** - Guaranteed consistency and durability
- **Schemaless Design** - Store any JSON-compatible data
- **Git Foundation** - Leverage Git's powerful features:
  - Branching and merging for isolated environments
  - Diffs between versions
  - Complete audit trails with commit messages

## 🚀 Quick Start

### Using Docker (Simplest)

```bash
# Start ChronDB with all protocols enabled
docker run -d --name chrondb \
  -p 3000:3000 \  # REST API
  -p 6379:6379 \  # Redis protocol
  -p 5432:5432 \  # PostgreSQL protocol
  -v chrondb-data:/data \
  moclojer/chrondb:latest
```

### Using Clojure

In your `deps.edn`:

```clojure
{:deps {com.github.moclojer/chrondb {:git/tag "v0.1.0"
                                     :git/sha "..."}}}
```

Then in your code:

```clojure
(require '[chrondb.core :as chrondb])

;; Create a database connection
(def db (chrondb/create-chrondb))

;; Create a document
(chrondb/save db "user:1" {:name "Alice" :email "alice@example.com"})

;; Update the document
(chrondb/save db "user:1" {:name "Alice" :email "alice@example.com" :role "admin"})

;; Time travel: see how the document looked at a specific point in time
(def yesterday (chrondb/get-at db "user:1" "2023-07-15T00:00:00Z"))

;; See all versions of the document
(def history (chrondb/history db "user:1"))
```

### Running the Server

```bash
# Clone the repository
git clone https://github.com/moclojer/chrondb.git
cd chrondb

# Run with default settings (REST on 3000, Redis on 6379, PostgreSQL on 5432)
clojure -M:run

# Or with custom ports
clojure -M:run 8080 6380 5433
```

## 🧠 Concept

ChronDB maps database concepts to Git's structure:

- **Database** → Git repository
- **Schema** → Git branch
- **Table** → Directory in repository
- **Document** → JSON file in directory
- **History** → Commit timeline

This approach gives you all the power of Git for your database operations.

## 📚 Documentation

Complete documentation is available at [chrondb.moclojer.com](https://chrondb.moclojer.com/)

- [Quick Start Guide](https://chrondb.moclojer.com/quickstart) - Up and running in minutes
- [Core Concepts](https://chrondb.moclojer.com/data-model) - Understand ChronDB's data model
- [Time Travel Tutorial](https://chrondb.moclojer.com/tutorials/time-travel-guide) - Learn to use versioning
- [Branching Guide](https://chrondb.moclojer.com/tutorials/branching-guide) - Master isolated environments

## 🛠️ Requirements

- Java 11 or later
- Git 2.25.0 or later

## 💬 Community

- [Discord Community](https://discord.com/channels/1099017682487087116/1353399752636497992) - Get help and chat with the team
- [GitHub Discussions](https://github.com/moclojer/chrondb/discussions) - Share ideas and use cases
- [GitHub Issues](https://github.com/moclojer/chrondb/issues) - Report bugs and request features
- [Reddit](https://www.reddit.com/r/chrondb/)

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.

## 📄 License

ChronDB is licensed under the terms of the GNU Affero General Public License v3.0 (AGPLv3).

This means:

- You are free to use, study, modify, and redistribute the software.
- If you run a modified version as a network service, you must make the source available to users of that service.
- See the [LICENSE](./LICENSE) file for full details or visit [gnu.org/licenses/agpl-3.0](https://www.gnu.org/licenses/agpl-3.0.html).

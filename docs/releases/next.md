# ChronDB - Version 0.1.0

This is the first official release of ChronDB, a chronological key/value database based on a Git architecture.

## Main Features

### Communication Protocols

- **PostgreSQL Protocol Support** - Implementation of the PostgreSQL protocol for communication with ChronDB, allowing compatibility with existing PostgreSQL tools and clients ([#16](https://github.com/moclojer/chrondb/pull/16))
- **Redis Protocol Server Implementation** - Support for the Redis protocol, expanding database communication options ([#11](https://github.com/moclojer/chrondb/pull/11))

### Storage

- **Virtual Git Storage Implementation** - Rewrite of the Git storage implementation to use virtual commits, improving performance and scalability ([#9](https://github.com/moclojer/chrondb/pull/9))
- **File Repository Support** - Added support for file-based storage ([#6](https://github.com/moclojer/chrondb/pull/6))
- **Initial JSON Compression** - Implementation of JSON compression to optimize data storage ([#1](https://github.com/moclojer/chrondb/pull/1))

## Performance Improvements

### SQL Driver

- **Removal of Unnecessary GROUP BY Operations** - Fixed an issue where the GROUP BY clause was being unnecessarily applied in SQL queries, significantly improving performance ([#17](https://github.com/moclojer/chrondb/pull/17))
- **Client Connection Handling Improvements** - Optimization of connection handling for greater stability and performance ([#17](https://github.com/moclojer/chrondb/pull/17))

## Tools and Utilities

- **Diagnostic and Dump Tools** - Added new tools for diagnostics and data dumping, facilitating debugging and analysis ([#17](https://github.com/moclojer/chrondb/pull/17))

## Refactoring and Internal Improvements

- **New Package Architecture** - Restructuring of the package architecture for better code organization and maintainability ([#7](https://github.com/moclojer/chrondb/pull/7))
- **GitHub Actions and Binding Renaming** - Implementation of GitHub Actions for continuous integration and adjustments to binding names ([#2](https://github.com/moclojer/chrondb/pull/2))

## Note

This release represents the collaborative effort of the community to create a robust and efficient chronological key/value database. We thank all contributors who made this version possible.

### Licensing Update

- ChronDB is now distributed under the GNU Affero General Public License v3.0 (AGPLv3) to ensure modifications deployed as network services remain available to the community.

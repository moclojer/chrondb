# Change Log

All notable changes to this project will be documented in this file. This change log follows the conventions of [keepachangelog.com](http://keepachangelog.com/).

## [Unreleased]

### Added

- DDL support: `CREATE TABLE`, `DROP TABLE`, `SHOW TABLES`, `SHOW SCHEMAS`, `DESCRIBE`
- Support for column constraints: `PRIMARY KEY`, `NOT NULL`, `UNIQUE`, `DEFAULT`
- `IF NOT EXISTS` for `CREATE TABLE` and `IF EXISTS` for `DROP TABLE`
- Schema storage in `_schema/` directory as JSON files
- Branch management functions: `chrondb_branch_list()`, `chrondb_branch_create()`, `chrondb_branch_checkout()`, `chrondb_branch_merge()`
- Schema inference from documents for tables without explicit schemas
- Schema-qualified table operations (e.g., `CREATE TABLE branch_name.table_name`)
- DDL documentation at `docs/ddl.md`

### Changed

- Add support for building ChronDB as a GraalVM native binary.

## [0.1.1] - 2019-12-13

### Changed

- Documentation on how to make the widgets.

### Removed

- `make-widget-sync` - we're all async, all the time.

### Fixed

- Fixed widget maker to keep working when daylight savings switches over.

## 0.1.0 - 2019-12-13

### Added

- Files from the new template.
- Widget maker public API - `make-widget-sync`.

[Unreleased]: https://github.com/avelino/chrondb/compare/0.1.1...HEAD
[0.1.1]: https://github.com/avelino/chrondb/compare/0.1.0...0.1.1

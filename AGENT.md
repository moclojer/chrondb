# GitHub Copilot Instructions for ChronDB

## Project Overview

ChronDB is a chronological key/value database built on Git architecture with complete version history. It's a Clojure project that provides time-travel capabilities for data storage and retrieval.

## Key Features

- Native Clojure API for JVM applications
- REST API for HTTP interface
- Redis Protocol compatibility
- Git-like versioning system for data
- Time-travel capabilities
- Document-oriented database functionality

## Code Style and Standards

### Clojure Conventions

- Follow standard Clojure naming conventions (kebab-case for functions and variables)
- Use meaningful function and variable names
- Prefer pure functions when possible
- Use proper docstrings for public functions
- Follow the project's existing indentation and formatting style

### Project Structure

- Main source code in `src/` directory
- Tests in `test/` directory
- Use `deps.edn` for dependency management
- Follow namespace conventions: `chrondb.*`

## Development Guidelines

### Database Operations

- When working with database operations, consider the chronological nature of the data
- Implement proper error handling for database operations
- Use appropriate data structures for time-based queries
- Consider performance implications of versioning operations

### API Development

- Maintain consistency across Native Clojure API, REST API, PostgreSQL Protocol and Redis Protocol
- Ensure proper input validation and error responses
- Follow RESTful principles for HTTP endpoints
- Document API changes and new endpoints

### Testing

- Write comprehensive tests for all new features
- Include unit tests for individual functions
- Add integration tests for API endpoints
- Test time-travel and versioning functionality thoroughly

### Git Architecture Integration

- Understand the Git-like architecture underlying the database
- Consider version history implications when implementing new features
- Ensure proper handling of branching and merging concepts in data operations
- more: https://www.moclojer.com/blog/git-as-database-harnessing-hidden-power-internals-chronological-data-storage/

## Specific Instructions

### When generating code:

1. Always consider the chronological aspect of data operations
2. Include proper error handling and logging
3. Use appropriate Clojure idioms and patterns
4. Ensure thread safety where applicable
5. Consider performance implications of Git-like operations

### When reviewing code:

1. Check for proper handling of time-based queries
2. Verify API consistency across different interfaces
3. Ensure proper testing coverage
4. Review for potential performance bottlenecks
5. Validate adherence to Clojure best practices

### Dependencies

- Use the project's existing dependency management approach
- Consider compatibility with the Git architecture
- Ensure new dependencies don't conflict with existing ones
- Document any new dependencies and their purpose
- ChronDB Relicensing, From MIT to GPLv3: we use GPL3, all dependencies used must be compatible with this license

## Documentation

- Update README.md for significant changes
- Document new API endpoints
- Include examples for new features
- Maintain clear docstrings for public functions

## Native Image Support

- Use `clojure -M:build` to gerar o artefato `target/chrondb.jar` e preparar arquivos auxiliares para o GraalVM
- O módulo `chrondb.native-image` cria `target/native-image-args`, `target/filter.json` e diretório `target/native-config`
- Para gerar o binário nativo localmente após rodar o build, execute `native-image @target/native-image-args -jar target/chrondb.jar -o target/chrondb_local`
- Evite rodar esse comando em paralelo com processos que utilizem as portas padrão (3000 etc.)
- O workflow `build-native-image.yml` é referência para o processo completo, incluindo smoke test via HTTP

# ChronDB Architecture

## Git as a Database

Git is traditionally known as a version control system for code, but its internal architecture presents characteristics that make it suitable for chronological data storage.

### Git Internal Structure

Git stores data in four main types of objects:

1. **Blobs**: Store file contents
2. **Trees**: Represent directories and contain references to blobs and other trees
3. **Commits**: Capture the state of the repository at a specific point in time
4. **Tags**: Point to specific commits with friendly names

This structure creates a content-addressed database, where each object is identified by the SHA-1 hash of its content.

### Alignment with Database Concepts

In ChronDB, these concepts are mapped to database terminology:

- **Git Repository** → Database
- **Git Branch** → Schema
- **Directory** → Table/Collection
- **File** → Document/Record
- **Commit** → Transaction
- **Commit Hash** → Transaction ID
- **Tag** → Named Snapshot

## ChronDB Architecture

ChronDB is built in layers:

1. **Storage Layer**: Uses JGit to interact with Git's internal structure
2. **Indexing Layer**: Implements Lucene for fast document search
3. **Access Layer**: Provides multiple interfaces (Clojure API, REST, Redis, PostgreSQL)
4. **Concurrency Layer**: Manages concurrent transactions and conflicts

### Data Flow

1. Write operations are converted to Git operations
2. Documents are serialized as JSON and stored as files
3. Each transaction results in a Git commit
4. Indices are updated to reflect changes
5. Reads can access any point in time using specific commits

## Architecture Benefits

- **Immutability**: Data is never overwritten, only added
- **Traceability**: Complete history of changes
- **Reliability**: Leveraging Git's proven robustness
- **Flexibility**: Support for multiple protocols and interfaces

# Frequently Asked Questions (FAQ)

## General

### What is ChronDB?

ChronDB is a chronological database based on Git's internal architecture. It allows storing data in key/value format with access to the complete history of modifications.

### Why use Git as a database foundation?

Git has ideal characteristics for chronological storage:

- Efficient storage of complete history
- Immutable data model
- Mature branching and merging system
- Proven reliability and robustness
- Integrity verification via SHA hashes

### What are the ideal use cases for ChronDB?

- Applications that need change auditing
- Data that benefits from historical tracking
- Scenarios where "time travel" through data is necessary
- Data that benefits from branching (testing, hypothetical scenarios)
- Systems where reliability and proof of integrity are essential

### How does ChronDB compare to traditional databases?

| Category | ChronDB | Traditional Databases |
|-----------|---------|---------------------|
| History | Complete | Limited/None |
| Immutability | Native | Requires implementation |
| Branches | Native | Not available |
| Queries | Basic + Lucene | Advanced (SQL) |
| Scalability | Limited | High |
| Write Performance | Moderate | High |
| Read Performance | High | High |

## Technical

### Is ChronDB suitable for large data volumes?

ChronDB is more suitable for medium data volumes. Since it stores complete history, disk usage grows over time. It's ideal for data that benefits from complete history, but not necessarily for big data.

### How are write conflicts resolved?

ChronDB uses Git's concurrency model:

1. Conflict detection based on commits
2. Automatic merge strategies when possible
3. Option for manual resolution in complex cases
4. Atomic transactions ensure consistency

### Does ChronDB support schemas?

ChronDB is schemaless by design, storing JSON documents. However:

- Schemas can be validated in the application
- Branches can be used as separate schemas
- The PostgreSQL interface can emulate SQL schemas

### How does search work?

ChronDB uses Lucene for indexing and searching:

- All document fields are automatically indexed
- Support for full-text and structured queries
- Temporal indices allow searching at any point in time

### What are the document size limits?

- Individual documents: up to 100MB (practical limit, not technical)
- Total database size: limited only by disk space
- Number of documents: virtually unlimited

## Deployment

### Can ChronDB run in a cluster?

Currently, ChronDB primarily functions as a single instance. However:

- Replication can be configured using Git replication
- Master-slave read model is possible
- Work in progress for full cluster support

### Is ChronDB suitable for production?

ChronDB is being used in production by various organizations, especially for use cases that benefit from complete history. Like any technology, evaluate your project's specific requirements.

### What are the recommended hardware requirements?

- CPU: 2+ cores
- RAM: 4GB+ (depending on data volume)
- Disk: SSD recommended for better performance
- Network: Standard

### Does ChronDB have commercial support?

Yes, commercial support is available through the Moclojer team. Contact for details.

## Development

### How can I contribute to ChronDB?

- Fork the repository: <https://github.com/moclojer/chrondb>
- Check the contribution guide
- Submit pull requests for fixes or new features
- Report issues on GitHub Issues

### Does ChronDB have a client library for my language?

ChronDB supports multiple protocols, allowing use with many languages:

- Via REST API: any language with HTTP support
- Via Redis: any language with Redis client
- Via PostgreSQL: any language with PostgreSQL driver
- Via Java/Clojure: direct native API

Specific client libraries are in development.

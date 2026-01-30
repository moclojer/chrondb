# Frequently Asked Questions

## General Questions

### What is ChronDB?

ChronDB is a chronological database that uses Git's internal architecture to store data with complete version history. It's a key-value database where every change is tracked, allowing you to access any previous version of your data.

### How is ChronDB different from other databases?

Unlike traditional databases that overwrite data when updated, ChronDB preserves every version of your data. The key differences are:

- **Complete history** - Nothing is ever deleted; all changes are preserved
- **Time travel** - Query data as it existed at any point in time
- **Branching** - Create isolated environments for testing or development
- **Multiple interfaces** - Connect through Clojure API, REST, Redis, or PostgreSQL protocols

### Is ChronDB production-ready?

Yes, ChronDB is designed for production use. However, as with any database, consider your specific requirements around performance, scaling, and backup strategies.

### What kinds of applications is ChronDB best suited for?

ChronDB excels in applications where data history and audit trails are important:

- Financial systems requiring complete audit trails
- Healthcare applications with strict data governance
- Collaborative tools where change history matters
- Systems needing point-in-time recovery capabilities
- Applications benefiting from isolated testing environments

## Technical Questions

### Is ChronDB ACID compliant?

Yes, ChronDB provides ACID (Atomicity, Consistency, Isolation, Durability) compliance:

- **Atomicity**: All operations in a transaction either succeed or fail together
- **Consistency**: The database remains in a valid state after each transaction
- **Isolation**: Transactions are isolated from each other
- **Durability**: Once a transaction is committed, it remains saved

### How does ChronDB handle schemas?

ChronDB is schema-less by default. Documents can have any structure, and documents in the same collection can have different fields. However, you can implement schema validation through hooks if needed.

### What's the maximum document size?

Individual documents are stored as files in the underlying Git repository, so practical size limits are similar to those of Git. We recommend keeping documents under 10MB for optimal performance.

### Does ChronDB support indexes?

Yes, ChronDB uses Lucene to provide full-text search capabilities. Fields that are frequently searched can be indexed for better performance.

## Performance Questions

### Is ChronDB as fast as traditional databases?

For most operations, ChronDB performance is competitive with other document databases. However, there are some differences:

- **Read performance**: Reading current data is fast
- **Time travel queries**: Reading historical data may be slower depending on history depth
- **Write performance**: Writes involve creating Git commits, which can be slower than write-optimized databases
- **Search performance**: Indexed searches are optimized but may not match dedicated search engines

### How does ChronDB scale?

ChronDB is primarily designed for vertical scaling (running on more powerful hardware). For horizontal scaling, consider these approaches:

- Separate databases for different domains of your application
- Read replicas for distributing read queries
- Sharding based on document collections

### What's the performance impact of storing all history?

Storing complete history means storage requirements grow over time. The impact on query performance is minimal for current data but can affect performance for deep historical queries.

## Storage and Operations

### How much disk space does ChronDB require?

Storage requirements depend on:

- The size of your data
- How frequently data changes
- How long you retain history

As a rough estimate, expect 2-5x the size of your current data if you have frequent updates and retain all history.

### Can I limit the history retention period?

Yes, ChronDB supports history pruning through Git's garbage collection mechanism. You can configure retention policies to balance storage needs with history requirements.

### How do I back up a ChronDB database?

Since ChronDB uses Git for storage, you can:

1. Use Git's native mechanisms (push to a remote repository)
2. Create point-in-time snapshots with `chrondb/backup`
3. Use filesystem-level backup tools

### Can I run ChronDB in the cloud?

Yes, ChronDB can run in any environment that supports Java and Docker:

- AWS, Google Cloud, or Azure VMs
- Kubernetes clusters
- Docker-based environments like AWS ECS

## Working with ChronDB

### How do I connect to ChronDB?

ChronDB supports multiple connection protocols:

1. **Clojure API**: Direct integration for Clojure applications
2. **REST API**: HTTP interface for any language
3. **Redis Protocol**: Connect using Redis clients
4. **PostgreSQL Protocol**: Connect using SQL clients

### Can I use ChronDB from JavaScript/Python/other languages?

Yes! While the native API is Clojure-based, you can use ChronDB from any language:

- JavaScript/Node.js: Use the REST API or Redis client
- Python: Use the REST API, Redis client, or PostgreSQL client
- Java: Use the REST API or JVM interop with the Clojure API
- Any language: Use the REST API

### How do I query data in ChronDB?

For current data:

- Clojure API: `(chrondb/get db "key")`
- REST API: `GET /api/v1/documents/key`
- Redis Protocol: `GET key`
- PostgreSQL Protocol: `SELECT * FROM collection WHERE id = 'key'`

For historical data:

- Clojure API: `(chrondb/get-at db "key" "2023-01-01T00:00:00Z")`
- REST API: `GET /api/v1/documents/key/at/2023-01-01T00:00:00Z`
- Redis Protocol: `CHRONDB.GETAT key 2023-01-01T00:00:00Z`
- PostgreSQL Protocol: `SELECT * FROM chrondb_at('collection', 'id', '2023-01-01T00:00:00Z')`

### How do I handle schema migrations?

Since ChronDB is schema-less, you don't need formal migrations. Options include:

1. **Progressive Enhancement**: Add new fields without modifying existing documents
2. **Batch Updates**: Run a process to update all documents to a new format
3. **Version Fields**: Include a schema version in documents
4. **Read-Time Transformation**: Transform documents to the latest schema when read

## Troubleshooting

### I can't connect to ChronDB

Check these common issues:

1. Ensure ChronDB is running (`docker ps` if using Docker)
2. Verify port mapping and network settings
3. Check firewall rules
4. Confirm correct protocol and port (3000 for REST, 6379 for Redis, 5432 for PostgreSQL)

### Searches aren't returning expected results

If searches aren't working as expected:

1. Verify your query syntax matches the protocol you're using
2. Check that documents are properly indexed
3. Try simplifying your query to isolate the issue
4. Use exact key lookups to confirm the document exists

### How do I resolve merge conflicts?

When merging branches, conflicts may occur if the same document has been modified differently in both branches. ChronDB provides these options:

1. **Automatic resolution**: ChronDB will attempt to merge changes automatically
2. **Manual resolution**: Specify which version to keep or provide a merged version
3. **Pre-merge checks**: Check for potential conflicts before merging

## Community and Support

### Where can I get help with ChronDB?

Several support options are available:

- [GitHub Issues](https://github.com/avelino/chrondb/issues) for bug reports and feature requests
- [Discord Community](https://discord.com/channels/1099017682487087116/1353399752636497992) for discussions and questions
- [Official Documentation](https://chrondb.avelino.run/) for guides and reference
- [GitHub Discussions](https://github.com/avelino/chrondb/discussions) for longer form conversations and ideas
- [Stack Overflow](https://stackoverflow.com/questions/tagged/chrondb) with the `chrondb` tag

### How can I contribute to ChronDB?

We welcome contributions! Ways to contribute include:

- Reporting bugs and suggesting features
- Improving documentation
- Creating example applications
- Submitting pull requests
- Sharing your ChronDB experience

### Is commercial support available?

Contact the team at [support@example.com](#) for information about commercial support options, including:

- Implementation consulting
- Custom feature development
- Production support agreements
- Training and workshops

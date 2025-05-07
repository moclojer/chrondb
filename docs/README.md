# ChronDB Documentation

This directory contains the complete documentation for ChronDB, a chronological database based on Git's internal architecture.

## Documentation Structure

The documentation is organized as follows:

### Getting Started

- **Introduction**: Overview of ChronDB and its key features
- **Quick Start Guide**: Step-by-step guide to get up and running quickly
- **Core Concepts**: Fundamental concepts of ChronDB's data model
- **FAQ**: Answers to frequently asked questions

### Tutorials

- **Time Travel Guide**: Practical guide to ChronDB's version history features
- **Branching Guide**: How to effectively use branching in your applications

### Reference

- **Data Model**: Detailed explanation of ChronDB's document structure
- **Version Control**: Complete guide to history and versioning features
- **API Reference**: Comprehensive API documentation

### Connection Methods

- **Protocols Overview**: Summary of all connection protocols
- **REST API Examples**: Using ChronDB with HTTP clients
- **Redis Protocol Examples**: Using ChronDB with Redis clients
- **PostgreSQL Protocol Examples**: Using ChronDB with SQL clients
- **Clojure API**: Native API for JVM applications

### Operations

- **Configuration**: Setup and configuration options
- **Performance**: Tuning and optimization guidelines
- **Operations Guide**: Administration and monitoring

## Documentation Standards

This documentation follows these principles:

1. **User-focused**: Written from the user's perspective, addressing their needs
2. **Progressive disclosure**: Start with simple concepts before introducing complexity
3. **Practical examples**: Each concept is illustrated with concrete code examples
4. **Multiple interfaces**: Examples cover all supported connection protocols
5. **Visual aids**: Diagrams and illustrations clarify complex concepts

## Contributing to Documentation

Contributions to improve the documentation are welcome! Please consider:

1. Adding more practical examples for different use cases
2. Creating tutorials for specific applications
3. Improving explanations of complex concepts
4. Fixing errors or clarifying confusing sections
5. Adding diagrams or visual illustrations

Submit a pull request with your improvements.

## Building the Documentation

The documentation is built using [GitBook](https://www.gitbook.com/) and is available online at [chrondb.moclojer.com](https://chrondb.moclojer.com/).

When adding links between pages, remember:

1. Do not include the `.md` extension in links between pages
2. Use relative paths for links to other pages in the documentation
3. For example, use `[Data Model](data-model)` instead of `[Data Model](data-model.md)`
4. External links should still include their full URLs and extensions

This ensures compatibility with GitBook's rendering system.

## Community Resources

- **Documentation**: [chrondb.moclojer.com](https://chrondb.moclojer.com/)
- **Discord Community**: [Join our Discord](https://discord.com/channels/1099017682487087116/1353399752636497992)
- **Discussion Forum**: [GitHub Discussions](https://github.com/moclojer/chrondb/discussions)

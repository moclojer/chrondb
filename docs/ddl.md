# ChronDB DDL (Data Definition Language)

This document describes the DDL features in ChronDB, including table creation, schema management, and branch operations.

## ChronDB is Schemaless by Default

**Important:** ChronDB is a schemaless database. Creating explicit schemas with `CREATE TABLE` is **completely optional**. You can start inserting documents immediately without defining any schema:

```sql
-- No CREATE TABLE needed! Just insert data directly:
INSERT INTO users (id, name, email) VALUES ('1', 'John', 'john@example.com');

-- ChronDB automatically creates the "users" table structure from your data
SELECT * FROM users;
```

Explicit schemas (`CREATE TABLE`) are useful when you want to:
- Document the expected structure of your data
- Enforce data types and constraints at the application level
- Provide metadata about your tables (column types, defaults, etc.)
- Use `DESCRIBE` to show table structure

**Tables without explicit schemas work exactly the same as tables with schemas.** The only difference is metadata availability.

## Overview

ChronDB provides DDL support that maps SQL concepts to Git structures:

| SQL Concept | ChronDB/Git Mapping |
|-------------|---------------------|
| Schema | Git branch |
| Table | Directory + schema file |
| Table metadata | JSON file in `_schema/` directory |

## Schema Storage

When you create a table with `CREATE TABLE`, ChronDB stores the schema definition as a JSON file in the `_schema/` directory:

```
_schema/
  users.json
  products.json
  orders.json
```

Each schema file contains:

```json
{
  "table": "users",
  "columns": [
    {"name": "id", "type": "TEXT", "primary_key": true},
    {"name": "name", "type": "TEXT", "nullable": false},
    {"name": "email", "type": "TEXT", "unique": true},
    {"name": "created_at", "type": "TIMESTAMP", "default": "CURRENT_TIMESTAMP"}
  ],
  "created_at": "2024-01-15T10:00:00Z"
}
```

## CREATE TABLE

Creates a new table with an explicit schema.

### Syntax

```sql
CREATE TABLE [IF NOT EXISTS] [schema.]table_name (
    column_name data_type [constraints],
    ...
);
```

### Column Constraints

- `PRIMARY KEY` - Marks the column as the primary key
- `NOT NULL` - Column cannot contain null values
- `UNIQUE` - Column values must be unique
- `DEFAULT value` - Default value for the column

### Supported Data Types

- `TEXT` / `VARCHAR` - Text strings
- `INTEGER` / `INT` - Integer numbers
- `NUMERIC` / `DECIMAL` / `FLOAT` / `DOUBLE` - Decimal numbers
- `BOOLEAN` / `BOOL` - Boolean values
- `TIMESTAMP` / `DATETIME` - Date/time values
- `JSONB` / `JSON` - JSON data

### Examples

```sql
-- Basic table creation
CREATE TABLE users (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT UNIQUE,
    age INTEGER,
    active BOOLEAN DEFAULT true
);

-- Create table only if it doesn't exist
CREATE TABLE IF NOT EXISTS products (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    price INTEGER DEFAULT 0,
    stock INTEGER
);

-- Create table in a specific schema (branch)
CREATE TABLE feature_branch.new_table (
    id TEXT PRIMARY KEY,
    data TEXT
);
```

## DROP TABLE

Removes a table schema from the database.

### Syntax

```sql
DROP TABLE [IF EXISTS] [schema.]table_name;
```

### Examples

```sql
-- Drop a table
DROP TABLE users;

-- Drop table only if it exists (no error if not found)
DROP TABLE IF EXISTS old_table;

-- Drop table from a specific schema (branch)
DROP TABLE feature_branch.temp_table;
```

## SHOW TABLES

Lists all tables in the database.

### Syntax

```sql
SHOW TABLES [FROM schema_name];
```

### Output

Returns a table with columns:
- `table_name` - Name of the table
- `has_schema` - Whether the table has an explicit schema (`YES`/`NO`)

### Examples

```sql
-- List all tables in the current schema
SHOW TABLES;

-- List tables from a specific schema (branch)
SHOW TABLES FROM feature_branch;
```

### Implicit Tables

ChronDB supports "implicit tables" - tables that exist because they contain documents but don't have an explicit schema. These tables are also listed in `SHOW TABLES` with `has_schema = NO`.

## SHOW SCHEMAS / SHOW DATABASES

Lists all schemas (Git branches) in the database.

### Syntax

```sql
SHOW SCHEMAS;
-- or
SHOW DATABASES;
```

### Output

Returns a table with column:
- `schema_name` - Name of the schema (branch)

Note: The `main` Git branch is mapped to the `public` schema for PostgreSQL compatibility.

### Examples

```sql
-- List all schemas
SHOW SCHEMAS;
```

## DESCRIBE / SHOW COLUMNS

Shows the structure of a table.

### Syntax

```sql
DESCRIBE [schema.]table_name;
-- or
SHOW COLUMNS FROM [schema.]table_name;
```

### Output

Returns a table with columns:
- `Field` - Column name
- `Type` - Data type
- `Null` - Whether nullable (`YES`/`NO`)
- `Key` - Key type (`PRI` for primary key, empty otherwise)
- `Default` - Default value

### Examples

```sql
-- Describe a table
DESCRIBE users;

-- Alternative syntax
SHOW COLUMNS FROM users;

-- Describe table in a specific schema
DESCRIBE feature_branch.users;
```

### Schema Inference

If a table has documents but no explicit schema, `DESCRIBE` will infer the schema from the existing documents by sampling up to 10 documents and detecting column types from the values.

## Branch Management Functions

ChronDB provides special functions to manage Git branches through SQL.

### chrondb_branch_list()

Lists all branches in the repository.

```sql
SELECT * FROM chrondb_branch_list();
```

Output columns:
- `branch_name` - Name of the branch
- `is_default` - Whether this is the default branch (`YES`/`NO`)

### chrondb_branch_create(name)

Creates a new branch from the current HEAD.

```sql
SELECT * FROM chrondb_branch_create('feature-x');
```

Output columns:
- `success` - Whether the operation succeeded (`true`/`false`)
- `message` - Result message

### chrondb_branch_checkout(name)

Switches the current session to a different branch.

```sql
SELECT * FROM chrondb_branch_checkout('feature-x');
```

Output columns:
- `success` - Whether the operation succeeded
- `message` - Result message
- `current_branch` - The branch now active

### chrondb_branch_merge(source, target)

Merges one branch into another.

```sql
SELECT * FROM chrondb_branch_merge('feature-x', 'main');
```

Output columns:
- `success` - Whether the merge succeeded
- `message` - Result message (may indicate conflicts)

## Schema to Branch Mapping

ChronDB maps SQL schemas to Git branches:

| SQL Schema | Git Branch |
|------------|------------|
| `public` | `main` |
| `<schema_name>` | `<schema_name>` |
| (no schema) | `main` |

This allows you to work with different "databases" using Git branching:

```sql
-- Create a table in the main branch
CREATE TABLE public.users (id TEXT);

-- Create a table in a feature branch
CREATE TABLE feature_branch.users (id TEXT);

-- Query from the main branch
SELECT * FROM public.users;

-- Query from a feature branch
SELECT * FROM feature_branch.users;
```

## Schemaless vs Schema-defined Tables

ChronDB treats schemaless tables and schema-defined tables identically for all operations:

| Feature | Schemaless Table | Schema-defined Table |
|---------|-----------------|---------------------|
| INSERT | Works | Works |
| SELECT | Works | Works |
| UPDATE | Works | Works |
| DELETE | Works | Works |
| SHOW TABLES | Listed (`has_schema=NO`) | Listed (`has_schema=YES`) |
| DESCRIBE | Infers from documents | Shows defined schema |

### When to use explicit schemas

- **Documentation**: When you want to document expected data structure
- **Migrations**: When you want to track schema changes in Git history
- **Type hints**: When you want to provide type information for clients
- **Defaults**: When you want to define default values

### When schemaless is better

- **Rapid prototyping**: Just insert data and iterate
- **Flexible documents**: When document structure varies
- **Simple use cases**: When you don't need formal schema definitions

## Limitations

- Foreign key constraints are not enforced (documents are stored independently)
- Check constraints are not supported
- Auto-increment columns are not supported (use UUIDs or application-generated IDs)
- Schema alterations (`ALTER TABLE`) are not yet supported
- Transactions across multiple documents are not atomic

## Best Practices

1. **Use explicit schemas** for tables that require type validation or documentation
2. **Use schema qualifiers** when working with multiple branches
3. **Create branches** before making experimental changes to schemas
4. **Use IF NOT EXISTS** for idempotent schema creation in migrations

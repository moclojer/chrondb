# Schema Validation

ChronDB is schemaless by default, but supports **optional JSON Schema validation** per namespace (table). This allows you to enforce data integrity where needed while keeping the flexibility of schemaless storage elsewhere.

## Overview

Validation schemas are:

- **Optional** - Only namespaces with defined schemas are validated
- **Per-namespace** - Each table/namespace can have its own schema
- **Version-controlled** - Schema changes are tracked in Git history
- **Mode-configurable** - Choose between strict rejection or warning-only logging

## Validation Modes

| Mode | Behavior |
|------|----------|
| `strict` | Rejects invalid documents with an error |
| `warning` | Logs violations but allows the write (ideal for gradual migration) |
| `disabled` | No validation (default behavior) |

## Creating a Validation Schema

### Via REST API

```bash
# Create schema for the "users" namespace
curl -X PUT http://localhost:3000/api/v1/schemas/validation/users \
  -H "Content-Type: application/json" \
  -d '{
    "mode": "strict",
    "schema": {
      "$schema": "http://json-schema.org/draft-07/schema#",
      "type": "object",
      "required": ["id", "email"],
      "properties": {
        "id": { "type": "string" },
        "email": { "type": "string", "format": "email" },
        "name": { "type": "string" },
        "age": { "type": "integer", "minimum": 0 }
      },
      "additionalProperties": false
    }
  }'
```

### Via Redis Protocol

```redis
SCHEMA.SET users '{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "required": ["id", "email"],
  "properties": {
    "id": { "type": "string" },
    "email": { "type": "string", "format": "email" }
  }
}' MODE strict
```

### Via SQL (PostgreSQL Protocol)

```sql
CREATE VALIDATION SCHEMA FOR users AS '{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "required": ["id", "email"],
  "properties": {
    "id": { "type": "string" },
    "email": { "type": "string", "format": "email" }
  }
}' MODE STRICT;
```

## Querying Schemas

### List All Schemas

**REST:**
```bash
curl http://localhost:3000/api/v1/schemas/validation
```

**Redis:**
```redis
SCHEMA.LIST
```

**SQL:**
```sql
SHOW VALIDATION SCHEMAS;
```

### Get Specific Schema

**REST:**
```bash
curl http://localhost:3000/api/v1/schemas/validation/users
```

**Redis:**
```redis
SCHEMA.GET users
```

**SQL:**
```sql
SHOW VALIDATION SCHEMA FOR users;
```

### View Schema History

**REST:**
```bash
curl http://localhost:3000/api/v1/schemas/validation/users/history
```

## Dry-Run Validation

Test if a document is valid before saving:

**REST:**
```bash
curl -X POST http://localhost:3000/api/v1/schemas/validation/users/validate \
  -H "Content-Type: application/json" \
  -d '{"id": "users:1", "name": "John"}'
```

Response (invalid document):
```json
{
  "valid": false,
  "errors": [
    {
      "path": "$.email",
      "message": "required property 'email' not found",
      "keyword": "required"
    }
  ]
}
```

**Redis:**
```redis
SCHEMA.VALIDATE users '{"id": "users:1", "name": "John"}'
```

## Updating a Schema

Simply send a new schema with PUT - version history is automatically maintained:

```bash
curl -X PUT http://localhost:3000/api/v1/schemas/validation/users \
  -H "Content-Type: application/json" \
  -d '{
    "mode": "strict",
    "schema": {
      "type": "object",
      "required": ["id", "email", "name"],
      "properties": { ... }
    }
  }'
```

## Changing the Mode

```bash
# Change from strict to warning
curl -X PUT http://localhost:3000/api/v1/schemas/validation/users \
  -H "Content-Type: application/json" \
  -d '{"mode": "warning", "schema": { ... }}'
```

## Removing Validation

**REST:**
```bash
curl -X DELETE http://localhost:3000/api/v1/schemas/validation/users
```

**Redis:**
```redis
SCHEMA.DEL users
```

**SQL:**
```sql
DROP VALIDATION SCHEMA FOR users;
```

## Validation Errors

When `mode: strict` and a document is invalid:

### REST API (HTTP 400)

```json
{
  "error": "VALIDATION_ERROR",
  "namespace": "users",
  "document_id": "users:1",
  "mode": "strict",
  "violations": [
    {"path": "$.email", "message": "required property 'email' not found", "keyword": "required"},
    {"path": "$.age", "message": "must be >= 0", "keyword": "minimum"}
  ]
}
```

### Redis Protocol

```
-ERR VALIDATION_ERROR users: required property 'email' not found at $.email
```

### PostgreSQL Protocol

```
ERROR: Validation failed for table 'users': $.email - required property 'email' not found
```

## Gradual Migration Strategy

When adding validation to existing data:

1. **Create schema in warning mode:**
   ```bash
   curl -X PUT .../schemas/validation/users -d '{"mode": "warning", "schema": {...}}'
   ```

2. **Monitor logs** to identify existing invalid documents

3. **Fix existing data** that violates the schema

4. **Switch to strict mode:**
   ```bash
   curl -X PUT .../schemas/validation/users -d '{"mode": "strict", "schema": {...}}'
   ```

## JSON Schema Reference

ChronDB supports JSON Schema Draft-07, 2019-09, and 2020-12. Common patterns:

```json
{
  "type": "object",
  "required": ["field1", "field2"],
  "properties": {
    "string_field": { "type": "string", "minLength": 1, "maxLength": 100 },
    "email_field": { "type": "string", "format": "email" },
    "number_field": { "type": "number", "minimum": 0, "maximum": 100 },
    "integer_field": { "type": "integer" },
    "boolean_field": { "type": "boolean" },
    "array_field": { "type": "array", "items": { "type": "string" } },
    "enum_field": { "enum": ["value1", "value2", "value3"] },
    "nullable_field": { "type": ["string", "null"] }
  },
  "additionalProperties": false
}
```

### Nested Objects

```json
{
  "type": "object",
  "properties": {
    "address": {
      "type": "object",
      "required": ["city"],
      "properties": {
        "street": { "type": "string" },
        "city": { "type": "string" },
        "zip": { "type": "string", "pattern": "^[0-9]{5}$" }
      }
    }
  }
}
```

### Array Validation

```json
{
  "type": "object",
  "properties": {
    "tags": {
      "type": "array",
      "items": { "type": "string" },
      "minItems": 1,
      "uniqueItems": true
    }
  }
}
```

For complete JSON Schema documentation, see: https://json-schema.org/

## Storage Details

Validation schemas are stored in Git at:

```
_schema/validation/{namespace}.json
```

Each schema file contains:

```json
{
  "namespace": "users",
  "version": 1,
  "mode": "strict",
  "schema": { ... },
  "created_at": "2024-01-15T10:00:00Z",
  "created_by": "admin"
}
```

Schema changes are versioned and can be time-traveled like any other data in ChronDB.

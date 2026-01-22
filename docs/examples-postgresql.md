# ChronDB PostgreSQL Protocol Examples

This document provides examples for using the ChronDB PostgreSQL protocol interface with psql and JavaScript clients.

## PostgreSQL Protocol Overview

ChronDB implements a subset of the PostgreSQL protocol, allowing you to connect using standard PostgreSQL clients and drivers. This makes it easy to integrate with existing applications that already use PostgreSQL or to leverage the SQL query capability.

The PostgreSQL protocol server can be configured in the `config.edn` file:

```clojure
:servers {
  :postgresql {
    :enabled true
    :host "0.0.0.0"
    :port 5432
    :username "chrondb"
    :password "chrondb"
  }
}
```

## Data Model Mapping

ChronDB maps its document structure to SQL tables based on document keys:

- Document keys with the format `collection:id` are mapped to tables
- The part before the colon becomes the table name
- The part after the colon becomes the row's ID
- Document fields become columns in the table

For example:

- A document with key `user:1` becomes a row in the `user` table with `id='1'`
- A document with key `product:xyz` becomes a row in the `product` table with `id='xyz'`

## Supported SQL Features

ChronDB supports the following SQL operations:

### DML (Data Manipulation Language)
- `SELECT` - Query documents
- `INSERT` - Create documents
- `UPDATE` - Update documents
- `DELETE` - Delete documents

### DDL (Data Definition Language)
**Note:** ChronDB is schemaless by default. `CREATE TABLE` is optional - you can insert data directly without defining a schema first.

- `CREATE TABLE` - Create table with explicit schema (optional)
- `CREATE TABLE IF NOT EXISTS` - Create table only if it doesn't exist
- `DROP TABLE` - Delete table schema
- `DROP TABLE IF EXISTS` - Delete table only if it exists
- `SHOW TABLES` - List all tables (both schemaless and schema-defined)
- `SHOW SCHEMAS` - List all schemas (Git branches)
- `DESCRIBE table` / `SHOW COLUMNS FROM table` - Show table structure (infers from data if no schema)

### Validation Schema DDL

ChronDB supports optional JSON Schema validation per namespace:

- `CREATE VALIDATION SCHEMA FOR namespace AS 'json-schema' MODE strict|warning` - Create validation schema
- `DROP VALIDATION SCHEMA FOR namespace` - Delete validation schema
- `SHOW VALIDATION SCHEMA FOR namespace` - Get validation schema
- `SHOW VALIDATION SCHEMAS` - List all validation schemas

## Special Functions

ChronDB provides special SQL functions to access version control features:

- `chrondb_history(table_name, id)` - Get document history
- `chrondb_at(table_name, id, timestamp)` - Get document at a point in time
- `chrondb_diff(table_name, id, t1, t2)` - Compare document versions
- `chrondb_branch_list()` - List branches
- `chrondb_branch_create(name)` - Create a new branch
- `chrondb_branch_checkout(name)` - Switch to a branch
- `chrondb_branch_merge(source, target)` - Merge branches

## Examples with psql

### Connecting to ChronDB

```bash
# Connect to ChronDB using psql
psql -h localhost -p 5432 -U chrondb -d chrondb
```

### Document Operations

#### Creating Documents (INSERT)

```sql
-- Create a user
INSERT INTO user (id, name, email, age, roles)
VALUES ('1', 'John Doe', 'john@example.com', 30, '["admin", "user"]');

-- Create another user
INSERT INTO user (id, name, email, age, roles)
VALUES ('2', 'Jane Smith', 'jane@example.com', 28, '["editor"]');

-- Create a product
INSERT INTO product (id, name, price, category, in_stock)
VALUES ('laptop1', 'Premium Laptop', 1299.99, 'Electronics', true);
```

#### Querying Documents (SELECT)

```sql
-- Get all users
SELECT * FROM user;

-- Get a specific user
SELECT * FROM user WHERE id = '1';

-- Get users with specific attributes
SELECT * FROM user WHERE age > 25;

-- Get specific fields
SELECT name, email FROM user;

-- Count documents
SELECT COUNT(*) FROM user;
```

#### Updating Documents (UPDATE)

```sql
-- Update a user
UPDATE user
SET email = 'john.doe@example.com', roles = '["admin", "user", "reviewer"]'
WHERE id = '1';

-- Update with JSON operations
UPDATE product
SET price = 1199.99, tags = '["sale", "featured"]'
WHERE id = 'laptop1';
```

#### Deleting Documents (DELETE)

```sql
-- Delete a specific user
DELETE FROM user WHERE id = '2';

-- Delete all products in a category
DELETE FROM product WHERE category = 'Discontinued';
```

### Version Control Operations

```sql
-- Get document history
SELECT * FROM chrondb_history('user', '1');

-- Get document at a specific point in time
SELECT * FROM chrondb_at('user', '1', '2023-10-10T09:15:00Z');

-- Compare document versions
SELECT * FROM chrondb_diff('user', '1',
                         '2023-10-10T09:15:00Z',
                         '2023-10-15T14:30:00Z');
```

### DDL Operations

```sql
-- Create a table with explicit schema
CREATE TABLE users (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT UNIQUE,
    age INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create table only if it doesn't exist
CREATE TABLE IF NOT EXISTS products (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    price INTEGER DEFAULT 0
);

-- List all tables
SHOW TABLES;

-- List tables from a specific schema (branch)
SHOW TABLES FROM feature_branch;

-- List all schemas (branches)
SHOW SCHEMAS;

-- Describe table structure
DESCRIBE users;
-- or
SHOW COLUMNS FROM users;

-- Drop a table
DROP TABLE users;

-- Drop table only if it exists
DROP TABLE IF EXISTS old_table;
```

### Schema Validation Operations

ChronDB supports optional JSON Schema validation per namespace. See the [Schema Validation](validation.md) documentation for full details.

```sql
-- Create a validation schema with strict mode
CREATE VALIDATION SCHEMA FOR users AS '{
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
}' MODE STRICT;

-- Create a validation schema with warning mode (logs violations but allows writes)
CREATE VALIDATION SCHEMA FOR products AS '{
  "type": "object",
  "required": ["id", "name"],
  "properties": {
    "id": { "type": "string" },
    "name": { "type": "string" },
    "price": { "type": "number", "minimum": 0 }
  }
}' MODE WARNING;

-- List all validation schemas
SHOW VALIDATION SCHEMAS;

-- Get a specific validation schema
SHOW VALIDATION SCHEMA FOR users;

-- Drop a validation schema
DROP VALIDATION SCHEMA FOR users;
```

#### Validation Errors

When inserting invalid data with a strict schema:

```sql
-- This will fail because email is required
INSERT INTO users (id, name) VALUES ('1', 'John');
-- ERROR: Validation failed for table 'users': $.email - required property 'email' not found

-- This will fail because age must be >= 0
INSERT INTO users (id, email, name, age) VALUES ('1', 'john@example.com', 'John', -5);
-- ERROR: Validation failed for table 'users': $.age - must be >= 0

-- Valid insert
INSERT INTO users (id, email, name, age) VALUES ('1', 'john@example.com', 'John', 30);
-- INSERT 0 1
```

### Branch Operations

```sql
-- List all branches
SELECT * FROM chrondb_branch_list();

-- Create a new branch
SELECT * FROM chrondb_branch_create('feature-reporting');

-- Switch to a branch
SELECT * FROM chrondb_branch_checkout('feature-reporting');

-- Merge branches
SELECT * FROM chrondb_branch_merge('feature-reporting', 'main');

-- Create table in a specific branch/schema
CREATE TABLE feature_branch.new_feature_data (id TEXT);
```

### Advanced Queries

```sql
-- Join documents from different collections
SELECT u.name, u.email, o.total, o.status
FROM user u
JOIN order o ON o.user_id = u.id
WHERE o.status = 'pending';

-- Aggregation
SELECT category, COUNT(*) as product_count, AVG(price) as avg_price
FROM product
GROUP BY category
ORDER BY product_count DESC;

-- Filtering with JSON
SELECT * FROM user
WHERE roles::jsonb ? 'admin';
```

## Full-Text Search with to_tsquery

ChronDB supports PostgreSQL-style full-text search using the familiar `@@` operator and `to_tsquery` function syntax. This allows you to perform efficient text searches across your documents with a PostgreSQL-compatible syntax.

### FTS Syntax

```sql
-- Basic full-text search using to_tsquery
SELECT * FROM products
WHERE name @@ to_tsquery('laptop');

-- Search for multiple words
SELECT * FROM articles
WHERE content @@ to_tsquery('database AND performance');

-- Search with wildcards
SELECT * FROM documents
WHERE description @@ to_tsquery('cloud*');
```

### How It Works

When you use the `field @@ to_tsquery('term')` syntax:

1. ChronDB parses the query and recognizes it as a full-text search condition
2. The search term is normalized (lowercase, accent removal)
3. If the term is short (less than 4 characters), wildcards are automatically added
4. The query is then passed to the underlying index (Lucene) for efficient search
5. Results are filtered to only include matches from the specified collection/table

### FTS Field Optimization

For better full-text search performance, you can create specialized FTS fields with the `_fts` suffix:

```sql
-- Insert a document with a dedicated FTS field
INSERT INTO articles (id, title, content, content_fts)
VALUES ('article:1', 'Introduction to Databases', 'Long article text...', 'optimized searchable content');

-- Search using the dedicated FTS field
SELECT * FROM articles
WHERE content_fts @@ to_tsquery('database');
```

When an indexed field ends with `_fts`, ChronDB will use it specifically for full-text search operations.

### Comparison with FTS_MATCH

ChronDB also supports the `FTS_MATCH` function for backward compatibility:

```sql
-- Traditional FTS_MATCH syntax
SELECT * FROM products
WHERE FTS_MATCH(name, 'laptop');

-- Equivalent to_tsquery syntax
SELECT * FROM products
WHERE name @@ to_tsquery('laptop');
```

The `to_tsquery` approach is recommended as it:
- Follows standard PostgreSQL syntax
- Provides better compatibility with existing PostgreSQL tools
- Supports the same normalization and text processing features

### Index Implementation Details

Full-text search operations are powered by:
- Lucene index for efficient text search (when enabled in configuration)
- Automatic text normalization and accent handling
- Wildcard prefix matching for better search results
- Fallback to basic string matching with MemoryIndex implementation

For optimal performance:
- Enable the Lucene index in your configuration
- Use dedicated FTS fields with the `_fts` suffix for frequently searched content
- Utilize more specific search terms to reduce result sets

## Examples with JavaScript (node-postgres)

The following examples use the [node-postgres](https://node-postgres.com/) package, a popular PostgreSQL client for Node.js.

### Setting Up

```javascript
// Install the pg package:
// npm install pg

const { Pool } = require('pg');

// Create a connection pool
const pool = new Pool({
  host: 'localhost',
  port: 5432,
  user: 'chrondb',
  password: 'chrondb',
  database: 'chrondb'
});

// Helper function for queries
async function query(text, params) {
  const client = await pool.connect();
  try {
    const result = await client.query(text, params);
    return result.rows;
  } finally {
    client.release();
  }
}
```

### Document Operations

```javascript
// Create a document (INSERT)
async function createUser(id, userData) {
  const { name, email, age, roles } = userData;

  const text = `
    INSERT INTO user (id, name, email, age, roles)
    VALUES ($1, $2, $3, $4, $5)
    RETURNING *
  `;

  // Convert JavaScript array to JSON string
  const rolesJson = JSON.stringify(roles);
  const values = [id, name, email, age, rolesJson];

  const rows = await query(text, values);
  return rows[0];
}

// Get a document (SELECT)
async function getUser(id) {
  const text = 'SELECT * FROM user WHERE id = $1';
  const values = [id];

  const rows = await query(text, values);

  if (rows.length === 0) {
    return null;
  }

  // Parse JSON string fields back to JavaScript objects
  const user = rows[0];
  if (user.roles) {
    user.roles = JSON.parse(user.roles);
  }

  return user;
}

// Update a document (UPDATE)
async function updateUser(id, updates) {
  // First get the current document
  const user = await getUser(id);
  if (!user) {
    throw new Error(`User ${id} not found`);
  }

  // Prepare the update fields
  const fields = [];
  const values = [];
  let index = 1;

  for (const [key, value] of Object.entries(updates)) {
    let formattedValue = value;

    // Convert arrays or objects to JSON strings
    if (typeof value === 'object' && value !== null) {
      formattedValue = JSON.stringify(value);
    }

    fields.push(`${key} = $${index}`);
    values.push(formattedValue);
    index++;
  }

  // Add the timestamp and id
  fields.push(`updated_at = $${index}`);
  values.push(new Date().toISOString());
  index++;
  values.push(id);

  const text = `
    UPDATE user
    SET ${fields.join(', ')}
    WHERE id = $${index}
    RETURNING *
  `;

  const rows = await query(text, values);

  // Parse JSON fields
  const updatedUser = rows[0];
  if (updatedUser.roles) {
    updatedUser.roles = JSON.parse(updatedUser.roles);
  }

  return updatedUser;
}

// Delete a document (DELETE)
async function deleteUser(id) {
  const text = 'DELETE FROM user WHERE id = $1 RETURNING id';
  const values = [id];

  const rows = await query(text, values);
  return rows.length > 0;
}

// Search documents
async function searchUsers(conditions = {}, options = {}) {
  const { limit = 10, offset = 0, orderBy = 'id', order = 'ASC' } = options;

  const clauses = [];
  const values = [];
  let index = 1;

  // Build WHERE clauses
  for (const [key, value] of Object.entries(conditions)) {
    clauses.push(`${key} = $${index}`);
    values.push(value);
    index++;
  }

  const whereClause = clauses.length > 0 ? `WHERE ${clauses.join(' AND ')}` : '';

  const text = `
    SELECT * FROM user
    ${whereClause}
    ORDER BY ${orderBy} ${order}
    LIMIT $${index} OFFSET $${index + 1}
  `;

  values.push(limit, offset);

  const rows = await query(text, values);

  // Parse JSON fields
  return rows.map(user => {
    if (user.roles) {
      user.roles = JSON.parse(user.roles);
    }
    return user;
  });
}
```

### Version Control Operations

```javascript
// Get document history
async function getUserHistory(id) {
  const text = 'SELECT * FROM chrondb_history($1, $2)';
  const values = ['user', id];

  const rows = await query(text, values);

  // Parse JSON data in each version
  return rows.map(version => {
    return {
      timestamp: version.timestamp,
      data: JSON.parse(version.data)
    };
  });
}

// Get document at a point in time
async function getUserAtTime(id, timestamp) {
  const text = 'SELECT * FROM chrondb_at($1, $2, $3)';
  const values = ['user', id, timestamp];

  const rows = await query(text, values);

  if (rows.length === 0) {
    return null;
  }

  // Parse JSON fields
  const user = rows[0];
  if (user.roles) {
    user.roles = JSON.parse(user.roles);
  }

  return user;
}

// Compare document versions
async function compareUserVersions(id, timestamp1, timestamp2) {
  const text = 'SELECT * FROM chrondb_diff($1, $2, $3, $4)';
  const values = ['user', id, timestamp1, timestamp2];

  const rows = await query(text, values);

  if (rows.length === 0) {
    return null;
  }

  const diff = rows[0];

  // Parse JSON diff data
  return {
    added: JSON.parse(diff.added || '{}'),
    removed: JSON.parse(diff.removed || '{}'),
    changed: JSON.parse(diff.changed || '{}')
  };
}
```

### Branch Operations

```javascript
// List branches
async function listBranches() {
  const text = 'SELECT * FROM chrondb_branch_list()';
  const rows = await query(text);
  return rows.map(row => row.branch_name);
}

// Create a branch
async function createBranch(name) {
  const text = `SELECT * FROM chrondb_branch_create('${name}')`;
  const rows = await query(text);
  return rows[0].success === 'true';
}

// Switch to a branch
async function switchBranch(name) {
  const text = `SELECT * FROM chrondb_branch_checkout('${name}')`;
  const rows = await query(text);
  return rows[0].success === 'true';
}

// Merge branches
async function mergeBranches(source, target) {
  const text = `SELECT * FROM chrondb_branch_merge('${source}', '${target}')`;
  const rows = await query(text);
  return rows[0].success === 'true';
}
```

### Complete Example: Customer Order System

```javascript
const { Pool } = require('pg');
const pool = new Pool({
  host: 'localhost',
  port: 5432,
  user: 'chrondb',
  password: 'chrondb',
  database: 'chrondb'
});

// OrderSystem class
class OrderSystem {
  constructor() {
    this.pool = pool;
  }

  async query(text, params) {
    const client = await this.pool.connect();
    try {
      const result = await client.query(text, params);
      return result.rows;
    } finally {
      client.release();
    }
  }

  // Initialize the database with tables
  async initialize() {
    const tables = [
      `CREATE TABLE IF NOT EXISTS customer (
         id TEXT PRIMARY KEY,
         name TEXT,
         email TEXT,
         address TEXT,
         created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
       )`,
      `CREATE TABLE IF NOT EXISTS product (
         id TEXT PRIMARY KEY,
         name TEXT,
         price NUMERIC,
         description TEXT,
         stock INTEGER,
         created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
       )`,
      `CREATE TABLE IF NOT EXISTS order_item (
         id TEXT PRIMARY KEY,
         order_id TEXT,
         product_id TEXT,
         quantity INTEGER,
         price NUMERIC,
         created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
       )`,
      `CREATE TABLE IF NOT EXISTS customer_order (
         id TEXT PRIMARY KEY,
         customer_id TEXT,
         total NUMERIC,
         status TEXT,
         created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
         updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
       )`
    ];

    for (const table of tables) {
      await this.query(table);
    }

    console.log('Database initialized');
  }

  // Customer methods
  async createCustomer(id, data) {
    const { name, email, address } = data;

    const text = `
      INSERT INTO customer (id, name, email, address)
      VALUES ($1, $2, $3, $4)
      RETURNING *
    `;

    const values = [id, name, email, address];
    const rows = await this.query(text, values);
    return rows[0];
  }

  async getCustomer(id) {
    const text = 'SELECT * FROM customer WHERE id = $1';
    const rows = await this.query(text, [id]);
    return rows[0] || null;
  }

  async updateCustomer(id, updates) {
    const fields = [];
    const values = [];
    let index = 1;

    for (const [key, value] of Object.entries(updates)) {
      fields.push(`${key} = $${index}`);
      values.push(value);
      index++;
    }

    values.push(id);

    const text = `
      UPDATE customer
      SET ${fields.join(', ')}
      WHERE id = $${index}
      RETURNING *
    `;

    const rows = await this.query(text, values);
    return rows[0] || null;
  }

  // Product methods
  async createProduct(id, data) {
    const { name, price, description, stock } = data;

    const text = `
      INSERT INTO product (id, name, price, description, stock)
      VALUES ($1, $2, $3, $4, $5)
      RETURNING *
    `;

    const values = [id, name, price, description, stock];
    const rows = await this.query(text, values);
    return rows[0];
  }

  async getProduct(id) {
    const text = 'SELECT * FROM product WHERE id = $1';
    const rows = await this.query(text, [id]);
    return rows[0] || null;
  }

  async updateProductStock(id, adjustment) {
    const text = `
      UPDATE product
      SET stock = stock + $1
      WHERE id = $2
      RETURNING *
    `;

    const values = [adjustment, id];
    const rows = await this.query(text, values);
    return rows[0] || null;
  }

  // Order methods
  async createOrder(data) {
    const { id, customer_id, items } = data;

    // Start transaction
    const client = await this.pool.connect();

    try {
      await client.query('BEGIN');

      // Calculate total
      let total = 0;
      for (const item of items) {
        const product = await this.getProduct(item.product_id);
        if (!product) {
          throw new Error(`Product ${item.product_id} not found`);
        }

        if (product.stock < item.quantity) {
          throw new Error(`Insufficient stock for product ${item.product_id}`);
        }

        total += product.price * item.quantity;

        // Update stock
        await client.query(
          'UPDATE product SET stock = stock - $1 WHERE id = $2',
          [item.quantity, item.product_id]
        );

        // Create order item
        const orderItemId = `item:${id}-${item.product_id}`;
        await client.query(
          `INSERT INTO order_item (id, order_id, product_id, quantity, price)
           VALUES ($1, $2, $3, $4, $5)`,
          [orderItemId, id, item.product_id, item.quantity, product.price]
        );
      }

      // Create the order
      const orderResult = await client.query(
        `INSERT INTO customer_order (id, customer_id, total, status)
         VALUES ($1, $2, $3, $4)
         RETURNING *`,
        [id, customer_id, total, 'pending']
      );

      await client.query('COMMIT');
      return orderResult.rows[0];

    } catch (e) {
      await client.query('ROLLBACK');
      throw e;
    } finally {
      client.release();
    }
  }

  async getOrder(id) {
    const orderText = 'SELECT * FROM customer_order WHERE id = $1';
    const orderRows = await this.query(orderText, [id]);

    if (orderRows.length === 0) {
      return null;
    }

    const order = orderRows[0];

    // Get order items
    const itemsText = 'SELECT * FROM order_item WHERE order_id = $1';
    const items = await this.query(itemsText, [id]);

    return {
      ...order,
      items
    };
  }

  async updateOrderStatus(id, status) {
    const text = `
      UPDATE customer_order
      SET status = $1, updated_at = CURRENT_TIMESTAMP
      WHERE id = $2
      RETURNING *
    `;

    const values = [status, id];
    const rows = await this.query(text, values);
    return rows[0] || null;
  }

  // Reporting methods
  async getCustomerOrders(customerId) {
    const text = `
      SELECT co.*, c.name as customer_name, c.email
      FROM customer_order co
      JOIN customer c ON co.customer_id = c.id
      WHERE co.customer_id = $1
      ORDER BY co.created_at DESC
    `;

    return await this.query(text, [customerId]);
  }

  async getOrderHistory(orderId) {
    const text = 'SELECT * FROM chrondb_history($1, $2)';
    const rows = await this.query(text, ['customer_order', orderId]);

    return rows.map(version => ({
      timestamp: version.timestamp,
      data: JSON.parse(version.data)
    }));
  }

  async getOrderStatistics() {
    const text = `
      SELECT
        status,
        COUNT(*) as count,
        SUM(total) as total_value,
        MIN(total) as min_value,
        MAX(total) as max_value,
        AVG(total) as avg_value
      FROM customer_order
      GROUP BY status
    `;

    return await this.query(text);
  }
}

// Usage example
async function runOrderSystemExample() {
  const orderSystem = new OrderSystem();

  try {
    // Initialize database
    await orderSystem.initialize();

    // Create customers
    await orderSystem.createCustomer('cust1', {
      name: 'Alice Johnson',
      email: 'alice@example.com',
      address: '123 Main St, Anytown, CA'
    });

    await orderSystem.createCustomer('cust2', {
      name: 'Bob Smith',
      email: 'bob@example.com',
      address: '456 Oak St, Somewhere, NY'
    });

    // Create products
    await orderSystem.createProduct('prod1', {
      name: 'Mechanical Keyboard',
      price: 129.99,
      description: 'Mechanical keyboard with RGB lighting',
      stock: 50
    });

    await orderSystem.createProduct('prod2', {
      name: 'Wireless Mouse',
      price: 49.99,
      description: 'Ergonomic wireless mouse',
      stock: 100
    });

    await orderSystem.createProduct('prod3', {
      name: 'Monitor Stand',
      price: 79.99,
      description: 'Adjustable monitor stand',
      stock: 30
    });

    // Create an order
    const order = await orderSystem.createOrder({
      id: 'order1',
      customer_id: 'cust1',
      items: [
        { product_id: 'prod1', quantity: 1 },
        { product_id: 'prod2', quantity: 2 }
      ]
    });

    console.log('Created order:', order);

    // Get order details
    const orderDetails = await orderSystem.getOrder('order1');
    console.log('Order details:', orderDetails);

    // Update order status
    await orderSystem.updateOrderStatus('order1', 'shipped');
    console.log('Updated order status');

    // Create another order
    await orderSystem.createOrder({
      id: 'order2',
      customer_id: 'cust2',
      items: [
        { product_id: 'prod3', quantity: 1 },
        { product_id: 'prod1', quantity: 1 }
      ]
    });

    // Get order history
    const history = await orderSystem.getOrderHistory('order1');
    console.log('Order history:', history);

    // Get customer orders
    const customerOrders = await orderSystem.getCustomerOrders('cust1');
    console.log('Customer orders:', customerOrders);

    // Get order statistics
    const statistics = await orderSystem.getOrderStatistics();
    console.log('Order statistics:', statistics);

  } catch (err) {
    console.error('Error:', err);
  } finally {
    await pool.end();
  }
}

// Run the example
runOrderSystemExample();
```

## Schema Validation with JavaScript

ChronDB supports optional JSON Schema validation per namespace. See the [Schema Validation](validation.md) documentation for full details.

```javascript
const { Pool } = require('pg');

const pool = new Pool({
  host: 'localhost',
  port: 5432,
  user: 'chrondb',
  password: 'chrondb',
  database: 'chrondb'
});

// Schema validation operations
class SchemaValidation {
  constructor() {
    this.pool = pool;
  }

  async query(text, params) {
    const client = await this.pool.connect();
    try {
      const result = await client.query(text, params);
      return result.rows;
    } finally {
      client.release();
    }
  }

  // Create a validation schema
  async createSchema(namespace, schema, mode = 'STRICT') {
    const schemaJson = JSON.stringify(schema);
    const text = `CREATE VALIDATION SCHEMA FOR ${namespace} AS '${schemaJson}' MODE ${mode}`;
    return await this.query(text);
  }

  // Get a validation schema
  async getSchema(namespace) {
    const text = `SHOW VALIDATION SCHEMA FOR ${namespace}`;
    return await this.query(text);
  }

  // List all validation schemas
  async listSchemas() {
    const text = 'SHOW VALIDATION SCHEMAS';
    return await this.query(text);
  }

  // Drop a validation schema
  async dropSchema(namespace) {
    const text = `DROP VALIDATION SCHEMA FOR ${namespace}`;
    return await this.query(text);
  }
}

// Usage example
async function schemaValidationExample() {
  const validation = new SchemaValidation();

  try {
    // Create a validation schema for users
    const userSchema = {
      '$schema': 'http://json-schema.org/draft-07/schema#',
      type: 'object',
      required: ['id', 'email'],
      properties: {
        id: { type: 'string' },
        email: { type: 'string', format: 'email' },
        name: { type: 'string' },
        age: { type: 'integer', minimum: 0 }
      },
      additionalProperties: false
    };

    await validation.createSchema('users', userSchema, 'STRICT');
    console.log('Validation schema created');

    // List all schemas
    const schemas = await validation.listSchemas();
    console.log('Validation schemas:', schemas);

    // Get the specific schema
    const schema = await validation.getSchema('users');
    console.log('User schema:', schema);

    // Try to insert valid data
    await validation.query(
      "INSERT INTO users (id, email, name, age) VALUES ($1, $2, $3, $4)",
      ['1', 'john@example.com', 'John', 30]
    );
    console.log('Valid document inserted');

    // Try to insert invalid data (will fail)
    try {
      await validation.query(
        "INSERT INTO users (id, name) VALUES ($1, $2)",
        ['2', 'Jane']
      );
    } catch (err) {
      console.log('Expected validation error:', err.message);
    }

    // Drop the schema
    await validation.dropSchema('users');
    console.log('Validation schema dropped');

  } catch (err) {
    console.error('Error:', err);
  } finally {
    await pool.end();
  }
}

schemaValidationExample();

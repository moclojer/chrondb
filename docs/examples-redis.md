# ChronDB Redis Protocol Examples

This document provides examples for using the ChronDB Redis protocol interface with redis-cli and JavaScript.

## Redis Protocol Overview

ChronDB implements a subset of the Redis protocol, allowing you to connect using standard Redis clients. This makes it easy to integrate with existing applications that already use Redis or to leverage the simplicity of the Redis command structure.

The Redis protocol server can be configured in the `config.edn` file:

```clojure
:servers {
  :redis {
    :enabled true
    :host "0.0.0.0"
    :port 6379
  }
}
```

## Standard Redis Commands

ChronDB supports the following standard Redis commands:

- `GET` - Retrieve a document
- `SET` - Create or update a document
- `DEL` - Delete a document
- `EXISTS` - Check if a document exists
- `KEYS` - List keys matching a pattern
- `HGET` - Get a specific field from a document
- `HSET` - Set a specific field in a document
- `HDEL` - Remove a specific field from a document
- `HGETALL` - Get all fields from a document

## RESP3 Protocol Support

ChronDB supports the RESP3 protocol, which provides richer data types and better client communication. Use the `HELLO` command to negotiate the protocol version:

```bash
# Negotiate RESP3 protocol
HELLO 3

# HELLO with client name
HELLO 3 SETNAME myclient

# Returns server info as a map in RESP3 mode
```

**RESP3 Data Types:**
- Null (`_`)
- Double (`,`)
- Boolean (`#`)
- Map (`%`)
- Set (`~`)

Connections without `HELLO` continue using RESP2 for backward compatibility.

## SCAN Commands

ChronDB supports the standard Redis SCAN family of commands for incremental iteration:

### SCAN - Iterate keys

```bash
# Basic scan from the beginning
SCAN 0

# Scan with pattern matching
SCAN 0 MATCH user:*

# Scan with count hint
SCAN 0 MATCH user:* COUNT 100

# Scan with type filter
SCAN 0 TYPE string

# Combined options
SCAN 0 MATCH user:* COUNT 50 TYPE hash
```

### HSCAN - Iterate hash fields

```bash
# Iterate fields in a hash
HSCAN myhash 0

# With pattern matching
HSCAN myhash 0 MATCH field:*

# With count hint
HSCAN myhash 0 COUNT 20
```

### SSCAN - Iterate set members

```bash
# Iterate members in a set
SSCAN myset 0

# With pattern matching
SSCAN myset 0 MATCH prefix:*

# With count hint
SSCAN myset 0 COUNT 50
```

**Cursor Convention:**
- `0` marks both the start and end of iteration
- Non-zero cursors represent pagination offsets
- Continue calling SCAN until cursor returns `0`

## ChronDB-Specific Commands

In addition to standard Redis commands, ChronDB provides special commands:

- `HISTORY` - Get document history with pagination (see below)
- `CHRONDB.HISTORY` - Get document history (legacy)
- `CHRONDB.GETAT` - Get document at a point in time
- `CHRONDB.DIFF` - Compare document versions
- `CHRONDB.BRANCH.LIST` - List branches
- `CHRONDB.BRANCH.CREATE` - Create a new branch
- `CHRONDB.BRANCH.CHECKOUT` - Switch to a branch
- `CHRONDB.BRANCH.MERGE` - Merge branches

### HISTORY Command (Time-Travel)

The `HISTORY` command provides paginated access to document history, enabling time-travel queries through the Redis protocol:

```bash
# Get history of a key (default: last 100 entries)
HISTORY user:123

# Limit number of results
HISTORY user:123 COUNT 10

# Filter by timestamp (Unix timestamp in seconds)
HISTORY user:123 SINCE 1705920000

# Paginate using cursor
HISTORY user:123 CURSOR <base64-cursor>

# Combined options
HISTORY user:123 SINCE 1705920000 COUNT 50
```

**Response Format:**
```
1) "0"                        # Next cursor ("0" means no more data)
2) 1) 1) "1705920000000"      # Timestamp (milliseconds)
      2) "{\"name\":\"John\"}" # Value at that time
   2) 1) "1705919000000"
      2) "{\"name\":\"Jane\"}"
```

**Use Cases:**
- Audit trails: see all changes to a record
- Debugging: understand how data evolved
- Compliance: prove data state at specific times
- Undo: recover previous values

## Examples with redis-cli

### Connecting to ChronDB

```bash
# Connect to ChronDB using redis-cli
redis-cli -h localhost -p 6379
```

### Document Operations

```bash
# Create a document
SET user:1 '{"name":"John Doe","email":"john@example.com","age":30}'

# Get a document
GET user:1
# Output: {"name":"John Doe","email":"john@example.com","age":30}

# Check if a document exists
EXISTS user:1
# Output: (integer) 1

# Delete a document
DEL user:1
# Output: (integer) 1

# Create a document with expiration (ChronDB supports TTL)
SET user:1 '{"name":"John Doe"}' EX 3600  # Expires in 1 hour
```

### Working with Document Fields

```bash
# Set a document with multiple fields
HSET product:1 name "Laptop" price 999.99 category "Electronics" stock 50

# Get a specific field
HGET product:1 price
# Output: "999.99"

# Get all fields
HGETALL product:1
# Output:
# 1) "name"
# 2) "Laptop"
# 3) "price"
# 4) "999.99"
# 5) "category"
# 6) "Electronics"
# 7) "stock"
# 8) "50"

# Delete a field
HDEL product:1 stock
```

### Searching for Documents

```bash
# Find all user keys
KEYS user:*
# Output:
# 1) "user:1"
# 2) "user:2"
# 3) "user:3"

# Find keys with wildcard
KEYS *:laptop*

# Full-text search using AST (new in Lucene migration)
SEARCH Software
# Output: Array of JSON documents matching "Software" in content field

# Search with limit
SEARCH Software LIMIT 10

# Search with limit and offset
SEARCH Software LIMIT 10 OFFSET 5

# Search with sorting
SEARCH Software SORT age:asc
SEARCH Software SORT age:desc

# Search on specific branch
SEARCH Software BRANCH main

# Combined options
SEARCH Software LIMIT 10 OFFSET 0 SORT name:asc BRANCH main

# Using FT.SEARCH alias (compatible with RediSearch)
FT.SEARCH Software LIMIT 10
```

### Version Control Commands

```bash
# Get document history
CHRONDB.HISTORY user:1
# Output: Array of document versions with timestamps

# Get document at a specific point in time
CHRONDB.GETAT user:1 "2023-10-15T14:30:00Z"
# Output: Document as it existed at that timestamp

# Compare document versions
CHRONDB.DIFF user:1 "2023-10-10T09:15:00Z" "2023-10-15T14:30:00Z"
# Output: Differences between versions
```

### Branch Operations

```bash
# List all branches
CHRONDB.BRANCH.LIST
# Output:
# 1) "main"
# 2) "dev"
# 3) "test"

# Create a new branch
CHRONDB.BRANCH.CREATE feature-login
# Output: OK

# Switch to a branch
CHRONDB.BRANCH.CHECKOUT feature-login
# Output: OK

# Merge branches
CHRONDB.BRANCH.MERGE feature-login main
# Output: OK
```

## Examples with JavaScript

### Setting Up

The following examples use the standard [redis](https://www.npmjs.com/package/redis) package for Node.js.

```javascript
// Install the redis package:
// npm install redis

const redis = require('redis');

// Create a Redis client connected to ChronDB
const client = redis.createClient({
  url: 'redis://localhost:6379'
});

// Connect to the server
async function connect() {
  await client.connect();
  console.log('Connected to ChronDB via Redis protocol');
}

// Handle connection errors
client.on('error', (err) => {
  console.error('Redis Client Error:', err);
});
```

### Document Operations

```javascript
// Basic CRUD operations
async function documentOperations() {
  try {
    // Create or update a document
    await client.set('user:1', JSON.stringify({
      name: 'Jane Smith',
      email: 'jane@example.com',
      age: 28,
      roles: ['editor']
    }));
    console.log('Document created');

    // Get a document
    const userJson = await client.get('user:1');
    const user = JSON.parse(userJson);
    console.log('Retrieved user:', user);

    // Update a document
    user.roles.push('reviewer');
    await client.set('user:1', JSON.stringify(user));
    console.log('Document updated');

    // Check if a document exists
    const exists = await client.exists('user:1');
    console.log('Document exists:', exists === 1);

    // Delete a document
    await client.del('user:1');
    console.log('Document deleted');

  } catch (err) {
    console.error('Error in document operations:', err);
  }
}
```

### Working with Document Fields (Hash Operations)

```javascript
// Using Redis hash operations for field-level access
async function fieldOperations() {
  try {
    // Create document with fields
    await client.hSet('product:1', {
      name: 'Smartphone',
      price: '799.99',
      category: 'Electronics',
      stock: '100',
      specs: JSON.stringify({
        screen: '6.5 inch',
        ram: '8GB',
        storage: '128GB'
      })
    });
    console.log('Product created with fields');

    // Get a specific field
    const price = await client.hGet('product:1', 'price');
    console.log('Product price:', price);

    // Get all fields
    const product = await client.hGetAll('product:1');
    console.log('Complete product:', product);

    // Parse JSON fields if needed
    if (product.specs) {
      product.specs = JSON.parse(product.specs);
    }

    // Update specific fields
    await client.hSet('product:1', 'stock', '95');
    await client.hSet('product:1', 'price', '749.99');
    console.log('Product fields updated');

    // Delete a field
    await client.hDel('product:1', 'specs');
    console.log('Specs field removed');

  } catch (err) {
    console.error('Error in field operations:', err);
  }
}
```

### Searching for Documents

```javascript
// Finding documents with key patterns
async function searchOperations() {
  try {
    // Generate some test data
    await client.set('user:101', JSON.stringify({ name: 'Alice', dept: 'Engineering' }));
    await client.set('user:102', JSON.stringify({ name: 'Bob', dept: 'Marketing' }));
    await client.set('user:103', JSON.stringify({ name: 'Charlie', dept: 'Engineering' }));

    // Find all user keys
    const userKeys = await client.keys('user:*');
    console.log('All user keys:', userKeys);

    // Get multiple documents at once
    if (userKeys.length > 0) {
      const usersData = await client.mGet(userKeys);
      const users = usersData.map(data => JSON.parse(data));
      console.log('All users:', users);
    }

    // Find specific keys
    const engineeringUserKeys = await Promise.all((await client.keys('user:*')).map(async key => {
      const userData = await client.get(key);
      const user = JSON.parse(userData);
      return user.dept === 'Engineering' ? key : null;
    })).then(keys => keys.filter(Boolean));

    console.log('Engineering users:', engineeringUserKeys);

  } catch (err) {
    console.error('Error in search operations:', err);
  }
}
```

### RESP3 Protocol and SCAN Operations

```javascript
// Using RESP3 protocol and SCAN commands
async function resp3AndScanOperations() {
  try {
    // Note: Node.js redis client automatically handles HELLO for RESP3
    // You can also send it manually:
    const serverInfo = await client.sendCommand(['HELLO', '3']);
    console.log('Server info (RESP3):', serverInfo);

    // Create some test data
    await client.set('user:1', JSON.stringify({ name: 'Alice', age: 30 }));
    await client.set('user:2', JSON.stringify({ name: 'Bob', age: 25 }));
    await client.set('user:3', JSON.stringify({ name: 'Charlie', age: 35 }));
    await client.set('product:1', JSON.stringify({ name: 'Laptop' }));

    // SCAN - iterate through all keys matching a pattern
    let cursor = '0';
    const allUserKeys = [];

    do {
      const result = await client.sendCommand(['SCAN', cursor, 'MATCH', 'user:*', 'COUNT', '10']);
      cursor = result[0];
      allUserKeys.push(...result[1]);
    } while (cursor !== '0');

    console.log('All user keys:', allUserKeys);

    // HSCAN - iterate through hash fields
    await client.hSet('config', {
      'db.host': 'localhost',
      'db.port': '5432',
      'cache.enabled': 'true',
      'cache.ttl': '3600'
    });

    cursor = '0';
    const dbSettings = [];

    do {
      const result = await client.sendCommand(['HSCAN', 'config', cursor, 'MATCH', 'db.*']);
      cursor = result[0];
      dbSettings.push(...result[1]);
    } while (cursor !== '0');

    console.log('Database settings:', dbSettings);

    // SSCAN - iterate through set members
    await client.sAdd('tags', ['javascript', 'typescript', 'java', 'python', 'rust', 'go']);

    cursor = '0';
    const jTags = [];

    do {
      const result = await client.sendCommand(['SSCAN', 'tags', cursor, 'MATCH', 'j*']);
      cursor = result[0];
      jTags.push(...result[1]);
    } while (cursor !== '0');

    console.log('Tags starting with j:', jTags);

  } catch (err) {
    console.error('Error in RESP3/SCAN operations:', err);
  }
}
```

### Using HISTORY for Time-Travel

```javascript
// Time-travel queries using the HISTORY command
async function historyOperations() {
  try {
    // Create initial document
    await client.set('doc:1', JSON.stringify({ title: 'Draft', content: 'Initial content' }));

    // Simulate updates over time
    await new Promise(resolve => setTimeout(resolve, 100));
    await client.set('doc:1', JSON.stringify({ title: 'Review', content: 'Updated content' }));

    await new Promise(resolve => setTimeout(resolve, 100));
    await client.set('doc:1', JSON.stringify({ title: 'Final', content: 'Final content' }));

    // Get document history with pagination
    let cursor = '0';
    const history = [];

    do {
      const result = await client.sendCommand(['HISTORY', 'doc:1', 'COUNT', '10', 'CURSOR', cursor]);
      cursor = result[0];

      // result[1] contains [[timestamp, value], [timestamp, value], ...]
      for (const entry of result[1]) {
        history.push({
          timestamp: parseInt(entry[0]),
          value: JSON.parse(entry[1])
        });
      }
    } while (cursor !== '0');

    console.log('Document history:', history);

    // Get history since a specific timestamp (Unix timestamp in seconds)
    const oneMinuteAgo = Math.floor(Date.now() / 1000) - 60;
    const recentHistory = await client.sendCommand([
      'HISTORY', 'doc:1', 'SINCE', String(oneMinuteAgo), 'COUNT', '50'
    ]);

    console.log('Recent changes:', recentHistory);

    // Build an audit trail
    console.log('\n--- Audit Trail for doc:1 ---');
    for (const entry of history) {
      const date = new Date(entry.timestamp);
      console.log(`${date.toISOString()}: ${entry.value.title}`);
    }

  } catch (err) {
    console.error('Error in history operations:', err);
  }
}
```

### Version Control Operations

```javascript
// Using ChronDB-specific commands for version control
async function versionControlOperations() {
  try {
    // Create a document with initial version
    await client.set('doc:1', JSON.stringify({ title: 'Initial Document', content: 'Draft content' }));
    console.log('Document created');

    // Wait a moment to ensure different timestamps
    await new Promise(resolve => setTimeout(resolve, 1000));

    // Update the document
    await client.set('doc:1', JSON.stringify({
      title: 'Updated Document',
      content: 'Revised content',
      lastModified: new Date().toISOString()
    }));
    console.log('Document updated');

    // Get document history
    const history = await client.sendCommand(['CHRONDB.HISTORY', 'doc:1']);
    console.log('Document history:', history);

    // Get document at a specific time (using the first version's timestamp)
    if (history && history.length > 0) {
      const firstVersion = JSON.parse(history[0]);
      const oldDoc = await client.sendCommand([
        'CHRONDB.GETAT',
        'doc:1',
        firstVersion.timestamp
      ]);
      console.log('Original version:', oldDoc);

      // Compare versions
      if (history.length > 1) {
        const secondVersion = JSON.parse(history[1]);
        const diff = await client.sendCommand([
          'CHRONDB.DIFF',
          'doc:1',
          firstVersion.timestamp,
          secondVersion.timestamp
        ]);
        console.log('Changes between versions:', diff);
      }
    }

  } catch (err) {
    console.error('Error in version control operations:', err);
  }
}
```

### Branch Operations

```javascript
// Working with branches
async function branchOperations() {
  try {
    // List all branches
    const branches = await client.sendCommand(['CHRONDB.BRANCH.LIST']);
    console.log('Available branches:', branches);

    // Create a new branch
    await client.sendCommand(['CHRONDB.BRANCH.CREATE', 'feature-search']);
    console.log('Created feature-search branch');

    // Switch to the new branch
    await client.sendCommand(['CHRONDB.BRANCH.CHECKOUT', 'feature-search']);
    console.log('Switched to feature-search branch');

    // Create a document in the feature branch
    await client.set('search:config', JSON.stringify({
      indexFields: ['title', 'content', 'tags'],
      caseSensitive: false,
      maxResults: 100
    }));
    console.log('Created document in feature branch');

    // Switch back to main
    await client.sendCommand(['CHRONDB.BRANCH.CHECKOUT', 'main']);

    // Verify document doesn't exist in main
    const exists = await client.exists('search:config');
    console.log('Document exists in main branch:', exists === 1);

    // Merge feature branch into main
    await client.sendCommand(['CHRONDB.BRANCH.MERGE', 'feature-search', 'main']);
    console.log('Merged feature-search into main');

    // Verify document now exists in main
    const existsAfterMerge = await client.exists('search:config');
    console.log('Document exists in main after merge:', existsAfterMerge === 1);

  } catch (err) {
    console.error('Error in branch operations:', err);
  }
}
```

### Complete Example: Inventory Tracking System

```javascript
const redis = require('redis');

// Inventory Management System using ChronDB via Redis protocol
class InventorySystem {
  constructor() {
    this.client = redis.createClient({
      url: 'redis://localhost:6379'
    });
    this.client.on('error', err => console.error('Redis Client Error:', err));
  }

  async connect() {
    await this.client.connect();
    console.log('Connected to ChronDB');
    return this;
  }

  async disconnect() {
    await this.client.quit();
    console.log('Disconnected from ChronDB');
  }

  // Product management
  async addProduct(id, details) {
    const productKey = `product:${id}`;
    const productData = {
      ...details,
      createdAt: new Date().toISOString(),
      lastModified: new Date().toISOString()
    };

    await this.client.set(productKey, JSON.stringify(productData));
    return productData;
  }

  async getProduct(id) {
    const productKey = `product:${id}`;
    const data = await this.client.get(productKey);
    return data ? JSON.parse(data) : null;
  }

  async updateProduct(id, updates) {
    const productKey = `product:${id}`;
    const currentData = await this.getProduct(id);

    if (!currentData) {
      throw new Error(`Product ${id} not found`);
    }

    const updatedData = {
      ...currentData,
      ...updates,
      lastModified: new Date().toISOString()
    };

    await this.client.set(productKey, JSON.stringify(updatedData));
    return updatedData;
  }

  async deleteProduct(id) {
    const productKey = `product:${id}`;
    return await this.client.del(productKey);
  }

  // Inventory operations
  async adjustStock(id, adjustment) {
    const product = await this.getProduct(id);

    if (!product) {
      throw new Error(`Product ${id} not found`);
    }

    const currentStock = product.stock || 0;
    const newStock = currentStock + adjustment;

    if (newStock < 0) {
      throw new Error(`Insufficient stock for product ${id}`);
    }

    return this.updateProduct(id, { stock: newStock });
  }

  // Version control features
  async getProductHistory(id) {
    const productKey = `product:${id}`;
    return await this.client.sendCommand(['CHRONDB.HISTORY', productKey]);
  }

  async getProductAtTime(id, timestamp) {
    const productKey = `product:${id}`;
    const data = await this.client.sendCommand(['CHRONDB.GETAT', productKey, timestamp]);
    return data ? JSON.parse(data) : null;
  }

  // Searching inventory
  async findProducts(pattern) {
    const keys = await this.client.keys(pattern);
    if (keys.length === 0) return [];

    const productsData = await this.client.mGet(keys);
    return productsData.map(data => JSON.parse(data));
  }

  // Bulk operations with transactions
  async bulkUpdateStock(updates) {
    const multi = this.client.multi();

    for (const [id, adjustment] of Object.entries(updates)) {
      const productKey = `product:${id}`;
      const productData = await this.client.get(productKey);

      if (!productData) {
        throw new Error(`Product ${id} not found`);
      }

      const product = JSON.parse(productData);
      const currentStock = product.stock || 0;
      const newStock = currentStock + adjustment;

      if (newStock < 0) {
        throw new Error(`Insufficient stock for product ${id}`);
      }

      product.stock = newStock;
      product.lastModified = new Date().toISOString();

      multi.set(productKey, JSON.stringify(product));
    }

    return await multi.exec();
  }
}

// Usage example
async function runInventoryExample() {
  const inventory = await new InventorySystem().connect();

  try {
    // Add products
    await inventory.addProduct('1001', {
      name: 'Ergonomic Chair',
      category: 'Furniture',
      price: 299.99,
      stock: 20
    });

    await inventory.addProduct('1002', {
      name: 'Standing Desk',
      category: 'Furniture',
      price: 499.99,
      stock: 15
    });

    await inventory.addProduct('2001', {
      name: 'Wireless Keyboard',
      category: 'Electronics',
      price: 79.99,
      stock: 50
    });

    // Get a product
    const chair = await inventory.getProduct('1001');
    console.log('Product details:', chair);

    // Update a product
    await inventory.updateProduct('1001', { price: 279.99, featured: true });
    console.log('Product updated');

    // Record sales (reduce stock)
    await inventory.adjustStock('1001', -5);
    await inventory.adjustStock('2001', -10);
    console.log('Stock adjusted after sales');

    // Get product history
    const history = await inventory.getProductHistory('1001');
    console.log('Chair price history:', history);

    // Find all furniture products
    const furniture = await inventory.findProducts('product:*');
    const furnitureItems = furniture.filter(item => item.category === 'Furniture');
    console.log('Furniture products:', furnitureItems);

    // Bulk restock
    await inventory.bulkUpdateStock({
      '1001': 10,
      '1002': 5,
      '2001': 25
    });
    console.log('Bulk restock completed');

    // Check current stock levels
    const currentStock = await Promise.all([
      inventory.getProduct('1001'),
      inventory.getProduct('1002'),
      inventory.getProduct('2001')
    ]);

    console.log('Current stock levels:', currentStock.map(p => ({ id: p.id, name: p.name, stock: p.stock })));

  } catch (err) {
    console.error('Inventory system error:', err);
  } finally {
    await inventory.disconnect();
  }
}

// Run the example
runInventoryExample();

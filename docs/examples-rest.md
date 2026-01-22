# ChronDB REST API Examples

This document provides examples for using the ChronDB REST API with curl and JavaScript.

## REST API Overview

ChronDB provides a RESTful API that allows you to interact with the database over HTTP. This makes it easy to integrate with any programming language or environment that can make HTTP requests.

The REST API server can be configured in the `config.edn` file:

```clojure
:servers {
  :rest {
    :enabled true
    :host "0.0.0.0"
    :port 3000
  }
}
```

## Examples with curl

### Document Operations

#### Create or Update a Document

```bash
# Create a new document
curl -X POST http://localhost:3000/api/v1/documents/user:1 \
  -H "Content-Type: application/json" \
  -d '{
    "name": "John Doe",
    "email": "john@example.com",
    "age": 30,
    "roles": ["admin", "user"]
  }'

# Response
# {"status":"success","key":"user:1"}
```

#### Get a Document

```bash
# Retrieve a document by key
curl http://localhost:3000/api/v1/documents/user:1

# Response
# {"name":"John Doe","email":"john@example.com","age":30,"roles":["admin","user"]}
```

#### Delete a Document

```bash
# Delete a document by key
curl -X DELETE http://localhost:3000/api/v1/documents/user:1

# Response
# {"status":"success","key":"user:1"}
```

### Search Operations

```bash
# Search for documents by field
curl http://localhost:3000/api/v1/search?q=name:John

# Search with multiple criteria
curl http://localhost:3000/api/v1/search?q=name:John%20AND%20age:%5B25%20TO%2035%5D

# Search with pagination
curl http://localhost:3000/api/v1/search?q=roles:admin&limit=5&offset=0
```

### Version Control Operations

#### Get Document History

```bash
# Get the history of a document
curl http://localhost:3000/api/v1/documents/user:1/history

# Response
# [
#   {
#     "timestamp": "2023-10-15T14:30:45Z",
#     "data": {"name":"John Doe","email":"john@example.com","age":30,"roles":["admin","user"]}
#   },
#   {
#     "timestamp": "2023-10-10T09:15:22Z",
#     "data": {"name":"John Doe","email":"john@example.com","roles":["user"]}
#   }
# ]
```

#### Get Document at a Point in Time

```bash
# Get a document as it was at a specific time
curl http://localhost:3000/api/v1/documents/user:1/at/2023-10-10T09:15:22Z

# Response
# {"name":"John Doe","email":"john@example.com","roles":["user"]}
```

#### Compare Document Versions

```bash
# Compare two versions of a document
curl http://localhost:3000/api/v1/documents/user:1/diff?t1=2023-10-10T09:15:22Z&t2=2023-10-15T14:30:45Z

# Response
# {
#   "added": {"age":30,"roles":["admin"]},
#   "removed": {"roles":["user"]},
#   "changed": {}
# }
```

### Branch Operations

#### List Branches

```bash
# List all branches
curl http://localhost:3000/api/v1/branches

# Response
# ["main","dev","test"]
```

#### Create a Branch

```bash
# Create a new branch
curl -X POST http://localhost:3000/api/v1/branches/feature-login

# Response
# {"status":"success","branch":"feature-login"}
```

#### Switch Branch

```bash
# Switch to a different branch
curl -X PUT http://localhost:3000/api/v1/branches/feature-login/checkout

# Response
# {"status":"success","branch":"feature-login"}
```

#### Merge Branches

```bash
# Merge one branch into another
curl -X POST http://localhost:3000/api/v1/branches/feature-login/merge/main

# Response
# {"status":"success","source":"feature-login","target":"main"}
```

## Examples with JavaScript

### Setting Up

```javascript
// Utility function for API requests
async function chronDBRequest(endpoint, method = 'GET', body = null) {
  const options = {
    method,
    headers: {
      'Content-Type': 'application/json',
      'Accept': 'application/json'
    }
  };

  if (body) {
    options.body = JSON.stringify(body);
  }

  const response = await fetch(`http://localhost:3000/api/v1/${endpoint}`, options);
  return response.json();
}
```

### Document Operations

```javascript
// Create or update a document
async function saveDocument(key, data) {
  return chronDBRequest(`documents/${key}`, 'POST', data);
}

// Get a document
async function getDocument(key) {
  return chronDBRequest(`documents/${key}`);
}

// Delete a document
async function deleteDocument(key) {
  return chronDBRequest(`documents/${key}`, 'DELETE');
}

// Usage examples
async function documentExamples() {
  // Create a user
  const user = {
    name: "Jane Smith",
    email: "jane@example.com",
    age: 28,
    roles: ["editor"]
  };

  await saveDocument('user:2', user);

  // Retrieve the user
  const savedUser = await getDocument('user:2');
  console.log('Retrieved user:', savedUser);

  // Update the user
  savedUser.roles.push('reviewer');
  await saveDocument('user:2', savedUser);

  // Delete the user
  await deleteDocument('user:2');
}
```

### Search Operations

```javascript
// Search for documents
async function searchDocuments(query, limit = 10, offset = 0) {
  const params = new URLSearchParams({
    q: query,
    limit,
    offset
  });

  return chronDBRequest(`search?${params.toString()}`);
}

// Usage examples
async function searchExamples() {
  // Simple search
  const admins = await searchDocuments('roles:admin');
  console.log('Admins:', admins);

  // Complex search
  const activeUsers = await searchDocuments('lastLogin:[NOW-7DAYS TO NOW]');
  console.log('Recently active users:', activeUsers);

  // Pagination
  const page1 = await searchDocuments('type:article', 5, 0);
  const page2 = await searchDocuments('type:article', 5, 5);
  console.log('Articles page 1:', page1);
  console.log('Articles page 2:', page2);
}
```

### Version Control Operations

```javascript
// Get document history
async function getDocumentHistory(key) {
  return chronDBRequest(`documents/${key}/history`);
}

// Get document at a point in time
async function getDocumentAt(key, timestamp) {
  return chronDBRequest(`documents/${key}/at/${encodeURIComponent(timestamp)}`);
}

// Compare document versions
async function compareDocuments(key, timestamp1, timestamp2) {
  const params = new URLSearchParams({
    t1: timestamp1,
    t2: timestamp2
  });

  return chronDBRequest(`documents/${key}/diff?${params.toString()}`);
}

// Usage examples
async function versionControlExamples() {
  // Get document history
  const history = await getDocumentHistory('user:1');
  console.log('User history:', history);

  // Get version from a week ago
  const oneWeekAgo = new Date();
  oneWeekAgo.setDate(oneWeekAgo.getDate() - 7);
  const oldVersion = await getDocumentAt('user:1', oneWeekAgo.toISOString());
  console.log('User from one week ago:', oldVersion);

  // Compare current version with week-old version
  const diff = await compareDocuments('user:1', oneWeekAgo.toISOString(), new Date().toISOString());
  console.log('Changes in the last week:', diff);
}
```

### Branch Operations

```javascript
// List all branches
async function listBranches() {
  return chronDBRequest('branches');
}

// Create a new branch
async function createBranch(name) {
  return chronDBRequest(`branches/${name}`, 'POST');
}

// Switch to a branch
async function switchBranch(name) {
  return chronDBRequest(`branches/${name}/checkout`, 'PUT');
}

// Merge branches
async function mergeBranches(source, target) {
  return chronDBRequest(`branches/${source}/merge/${target}`, 'POST');
}

// Usage examples
async function branchExamples() {
  // List all branches
  const branches = await listBranches();
  console.log('Available branches:', branches);

  // Create a feature branch
  await createBranch('feature-newui');

  // Switch to the feature branch
  await switchBranch('feature-newui');

  // Make changes in the feature branch
  await saveDocument('ui:config', { theme: 'dark', layout: 'responsive' });

  // Merge back to main
  await mergeBranches('feature-newui', 'main');
}
```

### Complete Example: User Management System

```javascript
// User management module
const UserAPI = {
  baseUrl: 'http://localhost:3000/api/v1',

  async request(endpoint, method = 'GET', body = null) {
    const options = {
      method,
      headers: {
        'Content-Type': 'application/json',
        'Accept': 'application/json'
      }
    };

    if (body) {
      options.body = JSON.stringify(body);
    }

    const response = await fetch(`${this.baseUrl}/${endpoint}`, options);
    if (!response.ok) {
      throw new Error(`API request failed: ${response.statusText}`);
    }
    return response.json();
  },

  // User operations
  async createUser(userData) {
    const userId = `user:${Date.now()}`;
    await this.request(`documents/${userId}`, 'POST', {
      ...userData,
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString()
    });
    return userId;
  },

  async updateUser(userId, updates) {
    const user = await this.request(`documents/${userId}`);
    const updatedUser = {
      ...user,
      ...updates,
      updatedAt: new Date().toISOString()
    };
    return this.request(`documents/${userId}`, 'POST', updatedUser);
  },

  async getUser(userId) {
    return this.request(`documents/${userId}`);
  },

  async deleteUser(userId) {
    return this.request(`documents/${userId}`, 'DELETE');
  },

  async searchUsers(query) {
    return this.request(`search?q=${encodeURIComponent(query)}`);
  },

  async getUserHistory(userId) {
    return this.request(`documents/${userId}/history`);
  }
};

// Usage
async function userManagementExample() {
  try {
    // Create a user
    const userId = await UserAPI.createUser({
      name: "Alice Johnson",
      email: "alice@example.com",
      role: "developer",
      skills: ["JavaScript", "React", "Node.js"]
    });
    console.log(`Created user with ID: ${userId}`);

    // Get the user
    const user = await UserAPI.getUser(userId);
    console.log("Retrieved user:", user);

    // Update the user
    await UserAPI.updateUser(userId, {
      role: "senior developer",
      skills: [...user.skills, "TypeScript"]
    });
    console.log("Updated user skills and role");

    // Get user history
    const history = await UserAPI.getUserHistory(userId);
    console.log("User modification history:", history);

    // Search for developers
    const developers = await UserAPI.searchUsers("role:*developer*");
    console.log("Found developers:", developers);

  } catch (error) {
    console.error("Error in user management:", error);
  }
}

// Run the example
userManagementExample();
```

## Schema Validation Operations

ChronDB supports optional JSON Schema validation per namespace. See the [Schema Validation](validation.md) documentation for full details.

### Managing Validation Schemas

#### Create a Validation Schema

```bash
# Create schema with strict mode for the "users" namespace
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

# Response
# {"namespace":"users","version":1,"mode":"strict","schema":{...}}
```

#### List All Validation Schemas

```bash
curl http://localhost:3000/api/v1/schemas/validation

# Response
# [{"namespace":"users","version":1,"mode":"strict"},{"namespace":"products","version":2,"mode":"warning"}]
```

#### Get a Specific Schema

```bash
curl http://localhost:3000/api/v1/schemas/validation/users

# Response
# {"namespace":"users","version":1,"mode":"strict","schema":{...},"created_at":"2024-01-15T10:00:00Z"}
```

#### Get Schema History

```bash
curl http://localhost:3000/api/v1/schemas/validation/users/history

# Response
# [{"commit-id":"abc123","timestamp":"2024-01-15T10:00:00Z","message":"Updated validation schema for users"}]
```

#### Delete a Validation Schema

```bash
curl -X DELETE http://localhost:3000/api/v1/schemas/validation/users

# Response
# {"status":"success","namespace":"users"}
```

### Dry-Run Validation

Test if a document is valid before saving:

```bash
# Valid document
curl -X POST http://localhost:3000/api/v1/schemas/validation/users/validate \
  -H "Content-Type: application/json" \
  -d '{"id": "users:1", "email": "john@example.com", "name": "John"}'

# Response (valid)
# {"valid":true,"errors":[]}

# Invalid document (missing required email)
curl -X POST http://localhost:3000/api/v1/schemas/validation/users/validate \
  -H "Content-Type: application/json" \
  -d '{"id": "users:1", "name": "John"}'

# Response (invalid)
# {"valid":false,"errors":[{"path":"$.email","message":"required property 'email' not found","keyword":"required"}]}
```

### Validation Error Response

When saving a document that fails validation in `strict` mode:

```bash
curl -X POST http://localhost:3000/api/v1/documents/users:1 \
  -H "Content-Type: application/json" \
  -d '{"name": "John", "age": -5}'

# Response (HTTP 400)
# {
#   "error": "VALIDATION_ERROR",
#   "namespace": "users",
#   "document_id": "users:1",
#   "mode": "strict",
#   "violations": [
#     {"path": "$.email", "message": "required property 'email' not found", "keyword": "required"},
#     {"path": "$.age", "message": "must be >= 0", "keyword": "minimum"}
#   ]
# }
```

### JavaScript Validation Examples

```javascript
// Schema management functions
const SchemaAPI = {
  baseUrl: 'http://localhost:3000/api/v1',

  async request(endpoint, method = 'GET', body = null) {
    const options = {
      method,
      headers: {
        'Content-Type': 'application/json',
        'Accept': 'application/json'
      }
    };
    if (body) {
      options.body = JSON.stringify(body);
    }
    const response = await fetch(`${this.baseUrl}/${endpoint}`, options);
    return response.json();
  },

  // Create or update a validation schema
  async setSchema(namespace, schema, mode = 'strict') {
    return this.request(`schemas/validation/${namespace}`, 'PUT', { mode, schema });
  },

  // Get a validation schema
  async getSchema(namespace) {
    return this.request(`schemas/validation/${namespace}`);
  },

  // List all schemas
  async listSchemas() {
    return this.request('schemas/validation');
  },

  // Delete a schema
  async deleteSchema(namespace) {
    return this.request(`schemas/validation/${namespace}`, 'DELETE');
  },

  // Validate a document without saving
  async validateDocument(namespace, document) {
    return this.request(`schemas/validation/${namespace}/validate`, 'POST', document);
  },

  // Get schema history
  async getSchemaHistory(namespace) {
    return this.request(`schemas/validation/${namespace}/history`);
  }
};

// Usage example
async function schemaValidationExample() {
  // Create a schema for users
  const userSchema = {
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

  await SchemaAPI.setSchema('users', userSchema, 'strict');
  console.log('Schema created');

  // Validate a document before saving
  const validDoc = { id: 'users:1', email: 'john@example.com', name: 'John' };
  const validResult = await SchemaAPI.validateDocument('users', validDoc);
  console.log('Valid document:', validResult.valid); // true

  const invalidDoc = { id: 'users:2', name: 'Jane' }; // missing email
  const invalidResult = await SchemaAPI.validateDocument('users', invalidDoc);
  console.log('Invalid document:', invalidResult.valid); // false
  console.log('Errors:', invalidResult.errors);
}

schemaValidationExample();
```

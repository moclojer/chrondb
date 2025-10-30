# AST Query System

ChronDB uses an Abstract Syntax Tree (AST) to represent queries uniformly across all protocols (REST, Redis, SQL, CLI). This enables consistent query behavior and advanced features like pagination, sorting, and filtering.

## Overview

The AST query system provides:

- **Unified query representation** across all protocols
- **Advanced pagination** with cursor-based navigation
- **Flexible sorting** by any document field
- **Type-aware queries** for numeric, temporal, and geospatial data
- **Composable query building** with helper functions

## AST Structure

### Basic Query

```clojure
(require '[chrondb.query.ast :as ast])

;; Simple FTS query
(ast/query [(ast/fts "content" "search term")]
           {:limit 10
            :offset 0
            :branch "main"})
```

### Query Components

#### FTS Clause

Full-text search clause:

```clojure
(ast/fts "content" "search term")
(ast/fts "title" "Lucene" {:analyzer :fts})
```

#### Sort Descriptors

```clojure
(ast/sort-by "age" :asc)
(ast/sort-by "name" :desc)
(ast/sort-by "score" :desc)
```

#### Range Queries

```clojure
;; Numeric ranges
(ast/range-long "age" 18 65)
(ast/range-double "price" 10.0 100.0)

;; Date ranges
(ast/range-long "timestamp" start-ts end-ts)
```

#### Query Options

```clojure
(ast/query [clauses]
           {:limit 10           ; Max results
            :offset 0            ; Skip results
            :after cursor-map    ; searchAfter cursor for pagination
            :sort [sort-desc]    ; Sort descriptors
            :branch "main"})     ; Branch name
```

## Usage Examples

### REST API

```bash
# Basic search
GET /api/v1/search?q=Software

# With pagination
GET /api/v1/search?q=Software&limit=10&offset=0

# With sorting
GET /api/v1/search?q=Software&sort=age:asc,name:desc

# With cursor-based pagination
GET /api/v1/search?q=Software&limit=10&after=<base64-cursor>

# Structured AST query (EDN)
GET /api/v1/search?query={:clauses [{:type :fts :field "content" :value "Software"}]}
```

### Redis Protocol

```bash
# Basic search
SEARCH Software

# With options
SEARCH Software LIMIT 10 OFFSET 0 SORT age:asc BRANCH main

# Using FT.SEARCH alias
FT.SEARCH Software LIMIT 10
```

### CLI

```bash
# Basic search
chrondb search --q Software

# With options
chrondb search --q Software --limit 10 --offset 0 --sort age:asc --branch main

# Structured query
chrondb search --query '{:clauses [{:type :fts :field "content" :value "Software"}]}'
```

## Helper Functions

### Building Queries

```clojure
;; With pagination
(ast/with-pagination (ast/query [clauses]) {:limit 10 :offset 0})

;; With sorting
(ast/with-sort (ast/query [clauses]) [(ast/sort-by "age" :asc)])

;; With searchAfter cursor
(ast/with-search-after (ast/query [clauses]) {:doc 123 :score 1.5})
```

### Complex Queries

```clojure
;; Multiple clauses
(ast/query [(ast/fts "content" "Software")
            (ast/range-long "age" 25 65)]
           {:limit 10
            :sort [(ast/sort-by "age" :asc)
                   (ast/sort-by "score" :desc)]
            :branch "main"})
```

## Pagination

### Offset-based Pagination

```clojure
;; First page
(ast/query [clauses] {:limit 10 :offset 0})

;; Second page
(ast/query [clauses] {:limit 10 :offset 10})
```

### Cursor-based Pagination (searchAfter)

More efficient for large result sets:

```clojure
;; Initial query
(let [result (index/search-query index ast-query branch opts)
      next-cursor (:next-cursor result)]
  ;; Use next-cursor for next page
  (ast/query [clauses] {:limit 10 :after next-cursor}))
```

The `next-cursor` is a map with `:doc` (document ID) and `:score` (relevance score) that can be serialized for API responses.

## Sorting

Sort by multiple fields:

```clojure
(ast/query [clauses]
           {:sort [(ast/sort-by "age" :asc)
                   (ast/sort-by "name" :desc)]})
```

Sorting is applied in order: first by age ascending, then by name descending.

## Protocol Consistency

The AST ensures that queries behave identically across all protocols:

- **REST**: `/api/v1/search?q=term&sort=age:asc&limit=10`
- **Redis**: `SEARCH term SORT age:asc LIMIT 10`
- **CLI**: `chrondb search --q term --sort age:asc --limit 10`
- **Direct AST**: `(ast/query [(ast/fts "content" "term")] {:sort [(ast/sort-by "age" :asc)] :limit 10})`

All return the same results with the same ordering and pagination.

## Type Safety

The AST supports type-aware queries:

```clojure
;; Numeric ranges
(ast/range-long "age" 18 65)
(ast/range-double "price" 10.0 100.0)

;; These ensure proper Lucene numeric field handling
```

## Migration Notes

The AST system replaces the legacy query format:

**Legacy:**

```clojure
{:field "content" :value "term"}
```

**New AST:**

```clojure
(ast/fts "content" "term")
```

Both are supported during migration, but new code should use AST.

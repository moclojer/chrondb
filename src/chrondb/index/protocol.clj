(ns chrondb.index.protocol
  "Defines the protocol for ChronDB index implementations.
   This protocol abstracts the underlying indexing mechanism (like Lucene)
   to provide search capabilities.")

(defprotocol Index
  "Protocol defining the indexing and search operations for ChronDB."
  (index-document [this doc] "Indexes a single document.")
  (delete-document [this id] "Deletes a document from the index.")
  (search [this field query-string branch] "Legacy search API (simple field/query lookup).
   Pending removal once all callers use `search-query`.")
  (search-query [this query-map branch opts]
    "Executes a structured query described by `query-map` within `branch` and optional overrides `opts`.
     `query-map` typically contains `:clauses`, `:sort`, `:limit`, `:offset`, etc.
     Returns a map {:ids [...], :total n, :limit l, :offset o, :sort sort-applied}.")
  (close [this] "Closes the index resources, releasing any held files or connections."))
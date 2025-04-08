(ns chrondb.index.protocol
  "Defines the protocol for ChronDB index implementations.
   This protocol abstracts the underlying indexing mechanism (like Lucene)
   to provide search capabilities.")

(defprotocol Index
  "Protocol defining the indexing and search operations for ChronDB."
  (index-document [this doc] "Indexes a single document.")
  (delete-document [this id] "Deletes a document from the index.")
  (search [this field query-string branch] "Searches the index based on a query string for a specific field and branch.")
  (close [this] "Closes the index resources, releasing any held files or connections."))
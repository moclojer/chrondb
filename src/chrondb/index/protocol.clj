(ns chrondb.index.protocol
  "Protocol definition for ChronDB indexing implementations.
   This protocol defines the core operations that any index backend must implement
   to provide search capabilities.")

(defprotocol Index
  "Protocol defining the indexing and search operations for ChronDB.
   Any index implementation must provide these core operations
   for document indexing and retrieval."

  (index-document [this doc]
    "Indexes a document for future searching.
     Parameters:
     - doc: The document to index (must include an :id field)
     Returns: The indexed document.")

  (delete-document [this id]
    "Removes a document from the index.
     Parameters:
     - id: The unique identifier of the document to remove
     Returns: nil")

  (search [this query]
    "Searches for documents matching the given query.
     Parameters:
     - query: The search query string
     Returns: A sequence of matching documents.")

  (close [this]
    "Closes the index and releases any resources.
     Returns: nil"))
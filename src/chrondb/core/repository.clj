(ns chrondb.core.repository)

(defprotocol Repository
  (save-document [this doc] "Save a document")
  (get-document [this id] "Get a document by ID")
  (delete-document [this id] "Delete a document by ID")
  (search [this query] "Search for documents matching the query")
  (close [this] "Close the repository"))
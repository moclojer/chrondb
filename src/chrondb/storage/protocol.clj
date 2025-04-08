(ns chrondb.storage.protocol
  "Protocol definition for ChronDB storage implementations.
   This protocol defines the core operations that any storage backend must implement.")

(defprotocol Storage
  "Protocol defining the storage operations for ChronDB.
   Any storage implementation must provide these core operations
   for document persistence and retrieval."

  (save-document
    [this doc]
    [this doc branch]
    "Saves a document to the storage system.
     Parameters:
     - doc: A map containing the document data (must include an :id field)
     - branch: (Optional) The branch name to save the document to (default branch if nil)
     Returns: The saved document with any system-generated fields.")

  (get-document
    [this id]
    [this id branch]
    "Retrieves a document from storage by its ID.
     Parameters:
     - id: The unique identifier of the document
     - branch: (Optional) The branch name to get the document from (default branch if nil)
     Returns: The document if found, nil otherwise.")

  (delete-document
    [this id]
    [this id branch]
    "Removes a document from storage.
     Parameters:
     - id: The unique identifier of the document to delete
     - branch: (Optional) The branch name to delete the document from (default branch if nil)
     Returns: true if document was deleted, false if document was not found.")

  (get-documents-by-prefix
    [this prefix]
    [this prefix branch]
    "Retrieves all documents whose IDs start with the given prefix.
     Parameters:
     - prefix: The prefix to match against document IDs
     - branch: (Optional) The branch name to get documents from (default branch if nil)
     Returns: A sequence of documents whose IDs start with the prefix.")

  (get-documents-by-table
    [this table-name]
    [this table-name branch]
    "Retrieves all documents belonging to a specific table.
     Parameters:
     - table-name: The name of the table
     - branch: (Optional) The branch name to get documents from (default branch if nil)
     Returns: A sequence of documents for the table.")

  (close [this]
    "Closes the storage system and releases any resources.
     Should be called when the storage system is no longer needed.
     Returns: nil on success."))
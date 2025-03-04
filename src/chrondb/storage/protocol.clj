(ns chrondb.storage.protocol
  "Protocol definition for ChronDB storage implementations.
   This protocol defines the core operations that any storage backend must implement.")

(defprotocol Storage
  "Protocol defining the storage operations for ChronDB.
   Any storage implementation must provide these core operations
   for document persistence and retrieval."

  (save-document [this doc]
    "Saves a document to the storage system.
     Parameters:
     - doc: A map containing the document data (must include an :id field)
     Returns: The saved document with any system-generated fields.")

  (get-document [this id]
    "Retrieves a document from storage by its ID.
     Parameters:
     - id: The unique identifier of the document
     Returns: The document if found, nil otherwise.")

  (delete-document [this id]
    "Removes a document from storage.
     Parameters:
     - id: The unique identifier of the document to delete
     Returns: true if document was deleted, false if document was not found.")

  (get-documents-by-prefix [this prefix]
    "Retrieves all documents whose IDs start with the given prefix.
     Parameters:
     - prefix: The prefix to match against document IDs
     Returns: A sequence of documents whose IDs start with the prefix.")

  (close [this]
    "Closes the storage system and releases any resources.
     Should be called when the storage system is no longer needed.
     Returns: nil on success."))
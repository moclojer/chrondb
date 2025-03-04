(ns chrondb.storage.memory
  "In-memory storage implementation for ChronDB.
   Uses ConcurrentHashMap for thread-safe document storage."
  (:require [chrondb.storage.protocol :as protocol])
  (:import [java.util.concurrent ConcurrentHashMap]))

(defn save-document-memory
  "Saves a document to the in-memory store.
   Thread-safe operation using ConcurrentHashMap."
  [^ConcurrentHashMap data doc]
  (.put data (:id doc) doc)
  doc)

(defn get-document-memory
  "Retrieves a document from the in-memory store by its ID.
   Returns nil if the document doesn't exist."
  [^ConcurrentHashMap data id]
  (.get data id))

(defn delete-document-memory
  "Removes a document from the in-memory store.
   Returns true if the document was found and deleted."
  [^ConcurrentHashMap data id]
  (when (.containsKey data id)
    (.remove data id)
    true))

(defn get-documents-by-prefix-memory
  "Retrieves all documents from the in-memory store whose IDs start with the given prefix."
  [^ConcurrentHashMap data prefix]
  (let [keys (.keySet data)
        matching-keys (filter #(.startsWith % prefix) keys)]
    (map #(.get data %) matching-keys)))

(defn close-memory-storage
  "Clears all documents from memory and releases resources."
  [^ConcurrentHashMap data]
  (.clear data))

(defrecord MemoryStorage [^ConcurrentHashMap data]
  protocol/Storage
  (save-document [_ doc] (save-document-memory data doc))
  (get-document [_ id] (get-document-memory data id))
  (delete-document [_ id] (delete-document-memory data id))
  (get-documents-by-prefix [_ prefix] (get-documents-by-prefix-memory data prefix))
  (close [_]
    (close-memory-storage data)
    nil))

(defn create-memory-storage
  "Creates a new instance of MemoryStorage.
   Returns: A new MemoryStorage instance backed by a ConcurrentHashMap."
  []
  (->MemoryStorage (ConcurrentHashMap.)))
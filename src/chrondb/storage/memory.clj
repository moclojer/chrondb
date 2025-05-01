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

(defn get-documents-by-table-memory
  "Retrieves all documents from the in-memory store that belong to a specific table."
  [^ConcurrentHashMap data table-name]
  (->> (.values data)
       (filter #(= (:_table %) table-name))
       (into [])))

(defn close-memory-storage
  "Clears all documents from memory and releases resources."
  [^ConcurrentHashMap data]
  (.clear data))

(defn get-document-history-memory
  "Simulates document history for in-memory storage.
   Since memory storage doesn't track history, this returns just the current state."
  [^ConcurrentHashMap data id]
  (when-let [doc (get-document-memory data id)]
    [{:commit-id "memory-current"
      :commit-time (java.util.Date.)
      :commit-message "Current version"
      :committer-name "Memory Storage"
      :committer-email "memory@chrondb.com"
      :document doc}]))

(defrecord MemoryStorage [^ConcurrentHashMap data]
  protocol/Storage
  (save-document [_ doc] (save-document-memory data doc))
  (save-document [_ doc _branch] (save-document-memory data doc))

  (get-document [_ id] (get-document-memory data id))
  (get-document [_ id _branch] (get-document-memory data id))

  (delete-document [_ id] (delete-document-memory data id))
  (delete-document [_ id _branch] (delete-document-memory data id))

  (get-documents-by-prefix [_ prefix] (get-documents-by-prefix-memory data prefix))
  (get-documents-by-prefix [_ prefix _branch] (get-documents-by-prefix-memory data prefix))

  (get-documents-by-table [_ table-name] (get-documents-by-table-memory data table-name))
  (get-documents-by-table [_ table-name _branch] (get-documents-by-table-memory data table-name))

  (get-document-history [_ id] (get-document-history-memory data id))
  (get-document-history [_ id _branch] (get-document-history-memory data id))

  (close [_]
    (close-memory-storage data)
    nil))

(defn create-memory-storage
  "Creates a new instance of MemoryStorage.
   Returns: A new MemoryStorage instance backed by a ConcurrentHashMap."
  []
  (->MemoryStorage (ConcurrentHashMap.)))
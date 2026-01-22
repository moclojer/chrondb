;; This file is part of ChronDB.
;;
;; ChronDB is free software: you can redistribute it and/or modify
;; it under the terms of the GNU Affero General Public License as published
;; by the Free Software Foundation, either version 3 of the License,
;; or (at your option) any later version.
(ns chrondb.wal.core
  "Write-Ahead Logging for ChronDB.
   Provides durability guarantees by logging operations before execution."
  (:require [chrondb.util.logging :as log]
            [clojure.java.io :as io]
            [clojure.edn :as edn])
  (:import [java.nio.channels FileChannel]
           [java.nio.file StandardOpenOption]
           [java.time Instant]
           [java.util UUID]))

;; WAL Entry States
;; :pending       - Entry written to WAL, operation not started
;; :git-committed - Git commit succeeded, index pending
;; :index-committed - Index updated, cleanup pending
;; :completed    - Operation fully committed, safe to remove
;; :rolled-back  - Operation failed, entry can be removed

(defprotocol WAL
  "Write-Ahead Log protocol for durability guarantees."
  (append! [this entry]
    "Append an entry to the WAL. Returns the entry with assigned ID.")
  (mark-state! [this entry-id state]
    "Update the state of a WAL entry.")
  (get-entry [this entry-id]
    "Get a specific WAL entry by ID.")
  (pending-entries [this]
    "Get all entries that are not yet completed.")
  (truncate! [this]
    "Remove all completed entries from the WAL.")
  (close-wal [this]
    "Close the WAL and release resources."))

(defn- ensure-wal-dir [wal-dir]
  (let [dir (io/file wal-dir)]
    (when-not (.exists dir)
      (.mkdirs dir))
    dir))

(defn- entry-file [wal-dir entry-id]
  (io/file wal-dir (str entry-id ".wal")))

(defn- write-entry-sync!
  "Write entry to file with fsync for durability."
  [file entry]
  (let [content (pr-str entry)
        path (.toPath file)]
    (with-open [channel (FileChannel/open path
                                          (into-array [StandardOpenOption/CREATE
                                                       StandardOpenOption/WRITE
                                                       StandardOpenOption/TRUNCATE_EXISTING
                                                       StandardOpenOption/SYNC]))]
      (let [buffer (java.nio.ByteBuffer/wrap (.getBytes content "UTF-8"))]
        (.write channel buffer)
        (.force channel true)))))

(defn- read-entry
  "Read entry from file."
  [file]
  (when (.exists file)
    (try
      (edn/read-string (slurp file))
      (catch Exception e
        (log/log-error (str "Failed to read WAL entry: " (.getMessage e)))
        nil))))

(defn create-wal-entry
  "Create a new WAL entry for an operation."
  [{:keys [operation document-id branch content table]}]
  {:id (str (UUID/randomUUID))
   :timestamp (.toString (Instant/now))
   :operation operation
   :document-id document-id
   :branch (or branch "main")
   :table table
   :content content
   :state :pending
   :created-at (.toString (Instant/now))
   :updated-at (.toString (Instant/now))})

(defrecord FileWAL [wal-dir]
  WAL
  (append! [_ entry]
    (let [entry-id (:id entry)
          file (entry-file wal-dir entry-id)]
      (log/log-info (str "WAL: Appending entry " entry-id " for " (:operation entry)))
      (write-entry-sync! file entry)
      entry))

  (mark-state! [_ entry-id state]
    (let [file (entry-file wal-dir entry-id)
          entry (read-entry file)]
      (when entry
        (let [updated-entry (assoc entry
                                   :state state
                                   :updated-at (.toString (Instant/now)))]
          (log/log-info (str "WAL: Marking entry " entry-id " as " state))
          (write-entry-sync! file updated-entry)
          updated-entry))))

  (get-entry [_ entry-id]
    (read-entry (entry-file wal-dir entry-id)))

  (pending-entries [_]
    (let [dir (io/file wal-dir)
          files (when (.exists dir)
                  (filter #(.endsWith (.getName %) ".wal") (.listFiles dir)))]
      (->> files
           (map read-entry)
           (filter some?)
           (remove #(contains? #{:completed :rolled-back} (:state %)))
           (sort-by :timestamp))))

  (truncate! [_]
    (let [dir (io/file wal-dir)
          files (when (.exists dir)
                  (filter #(.endsWith (.getName %) ".wal") (.listFiles dir)))
          completed-files (->> files
                               (filter (fn [f]
                                         (let [entry (read-entry f)]
                                           (contains? #{:completed :rolled-back} (:state entry))))))]
      (log/log-info (str "WAL: Truncating " (count completed-files) " completed entries"))
      (doseq [file completed-files]
        (.delete file))
      (count completed-files)))

  (close-wal [_]
    (log/log-info "WAL: Closing")
    nil))

(defn create-file-wal
  "Create a new FileWAL instance."
  [wal-dir]
  (ensure-wal-dir wal-dir)
  (->FileWAL wal-dir))

;; Convenience functions for common operations

(defn wal-save-document!
  "Create and append a WAL entry for a save operation."
  [wal document branch]
  (append! wal (create-wal-entry {:operation :save
                                  :document-id (:id document)
                                  :branch branch
                                  :table (:_table document)
                                  :content document})))

(defn wal-delete-document!
  "Create and append a WAL entry for a delete operation."
  [wal document-id branch]
  (append! wal (create-wal-entry {:operation :delete
                                  :document-id document-id
                                  :branch branch
                                  :content nil})))

(defn wal-commit-git!
  "Mark WAL entry as git-committed."
  [wal entry-id]
  (mark-state! wal entry-id :git-committed))

(defn wal-commit-index!
  "Mark WAL entry as index-committed."
  [wal entry-id]
  (mark-state! wal entry-id :index-committed))

(defn wal-complete!
  "Mark WAL entry as fully completed."
  [wal entry-id]
  (mark-state! wal entry-id :completed))

(defn wal-rollback!
  "Mark WAL entry as rolled back."
  [wal entry-id]
  (mark-state! wal entry-id :rolled-back))

;; This file is part of ChronDB.
;;
;; ChronDB is free software: you can redistribute it and/or modify
;; it under the terms of the GNU Affero General Public License as published
;; by the Free Software Foundation, either version 3 of the License,
;; or (at your option) any later version.
(ns chrondb.storage.durable
  "Durable storage with WAL, OCC, and two-phase commit.
   Wraps the base storage with durability guarantees."
  (:require [chrondb.storage.protocol :as storage]
            [chrondb.index.protocol :as index]
            [chrondb.wal.core :as wal]
            [chrondb.wal.recovery :as recovery]
            [chrondb.concurrency.occ :as occ]
            [chrondb.observability.metrics :as metrics]
            [chrondb.util.logging :as log]
            [chrondb.config :as config]))

(defrecord DurableStorage [storage index wal version-tracker config]
  storage/Storage

  (save-document [this doc]
    (storage/save-document this doc nil))

  (save-document [_this doc branch]
    (let [start (System/currentTimeMillis)
          branch-name (or branch (get-in config [:git :default-branch] "main"))
          doc-id (:id doc)]

      ;; Use OCC retry for concurrent writes
      (occ/with-occ-retry
        (fn []
          (occ/with-branch-lock branch-name
            ;; Phase 1: Write to WAL
            (let [wal-entry (when wal
                              (wal/wal-save-document! wal doc branch-name))]
              (try
                ;; Phase 2: Write to Git storage
                (let [result (storage/save-document storage doc branch-name)]
                  (when wal-entry
                    (wal/wal-commit-git! wal (:id wal-entry)))

                  ;; Phase 3: Update index
                  (when index
                    (try
                      (index/index-document index result)
                      (when wal-entry
                        (wal/wal-commit-index! wal (:id wal-entry)))
                      (catch Exception e
                        (log/log-warn (str "Index update failed, will recover: " (.getMessage e))))))

                  ;; Mark WAL entry complete
                  (when wal-entry
                    (wal/wal-complete! wal (:id wal-entry)))

                  ;; Update version tracker
                  (when version-tracker
                    (occ/increment-version version-tracker doc-id branch-name))

                  ;; Record metrics
                  (let [elapsed (/ (- (System/currentTimeMillis) start) 1000.0)]
                    (metrics/record-write! elapsed))

                  result)

                (catch Exception e
                  ;; Rollback WAL on failure
                  (when wal-entry
                    (wal/wal-rollback! wal (:id wal-entry)))
                  (throw e))))))

        {:on-conflict (fn [_]
                        (metrics/record-occ-conflict!))
         :on-retry (fn [_ _]
                     (metrics/record-occ-retry!))})))

  (get-document [this id]
    (storage/get-document this id nil))

  (get-document [_ id branch]
    (let [start (System/currentTimeMillis)
          result (storage/get-document storage id branch)
          elapsed (/ (- (System/currentTimeMillis) start) 1000.0)]
      (metrics/record-read! elapsed)
      result))

  (delete-document [this id]
    (storage/delete-document this id nil))

  (delete-document [_this id branch]
    (let [_start (System/currentTimeMillis)
          branch-name (or branch (get-in config [:git :default-branch] "main"))]

      (occ/with-occ-retry
        (fn []
          (occ/with-branch-lock branch-name
            ;; Phase 1: Write to WAL
            (let [wal-entry (when wal
                              (wal/wal-delete-document! wal id branch-name))]
              (try
                ;; Phase 2: Delete from Git storage
                (let [result (storage/delete-document storage id branch-name)]
                  (when wal-entry
                    (wal/wal-commit-git! wal (:id wal-entry)))

                  ;; Phase 3: Remove from index
                  (when index
                    (try
                      (index/delete-document index id)
                      (when wal-entry
                        (wal/wal-commit-index! wal (:id wal-entry)))
                      (catch Exception e
                        (log/log-warn (str "Index delete failed, will recover: " (.getMessage e))))))

                  ;; Mark WAL entry complete
                  (when wal-entry
                    (wal/wal-complete! wal (:id wal-entry)))

                  ;; Record metrics
                  (when result
                    (metrics/record-delete!))

                  result)

                (catch Exception e
                  (when wal-entry
                    (wal/wal-rollback! wal (:id wal-entry)))
                  (throw e))))))

        {:on-conflict (fn [_] (metrics/record-occ-conflict!))
         :on-retry (fn [_ _] (metrics/record-occ-retry!))})))

  (get-documents-by-prefix [this prefix]
    (storage/get-documents-by-prefix this prefix nil))

  (get-documents-by-prefix [_ prefix branch]
    (storage/get-documents-by-prefix storage prefix branch))

  (get-documents-by-table [this table-name]
    (storage/get-documents-by-table this table-name nil))

  (get-documents-by-table [_ table-name branch]
    (storage/get-documents-by-table storage table-name branch))

  (get-document-history [this id]
    (storage/get-document-history this id nil))

  (get-document-history [_ id branch]
    (storage/get-document-history storage id branch))

  (close [_]
    (when wal
      (wal/close-wal wal))
    (when index
      (index/close index))
    (storage/close storage)))

(defn create-durable-storage
  "Create a new DurableStorage instance with WAL and OCC support.

   Parameters:
   - storage: Base storage implementation
   - index: Index implementation (optional)
   - options:
     - :wal-dir - Directory for WAL files (default: data/wal)
     - :enable-wal - Enable WAL (default: true)
     - :enable-occ - Enable OCC (default: true)
     - :recover-on-start - Run recovery on startup (default: true)"
  [storage index & {:keys [wal-dir enable-wal enable-occ recover-on-start]
                    :or {enable-wal true
                         enable-occ true
                         recover-on-start true}}]
  (let [config-map (config/load-config)
        wal-directory (or wal-dir
                          (str (get-in config-map [:storage :data-dir] "data") "/wal"))
        wal-instance (when enable-wal
                       (wal/create-file-wal wal-directory))
        version-tracker (when enable-occ
                          (occ/create-version-tracker))]

    ;; Run recovery if enabled
    (when (and recover-on-start wal-instance)
      (log/log-info "Running WAL recovery on startup...")
      (let [result (recovery/recover! storage index wal-instance)]
        (log/log-info (str "WAL recovery completed: " result))))

    (->DurableStorage storage index wal-instance version-tracker config-map)))

;; Utility functions
(defn get-current-version
  "Get the current version of a document."
  [^DurableStorage durable-storage doc-id branch]
  (when-let [tracker (:version-tracker durable-storage)]
    (occ/get-version tracker doc-id (or branch "main"))))

(defn save-with-expected-version
  "Save a document with optimistic locking based on expected version.
   Throws version-conflict-exception if the version has changed."
  [^DurableStorage durable-storage doc branch expected-version]
  (let [tracker (:version-tracker durable-storage)
        branch-name (or branch "main")
        doc-id (:id doc)]
    (when tracker
      (occ/verify-version tracker doc-id branch-name expected-version))
    (storage/save-document durable-storage doc branch)))

(defn get-wal-stats
  "Get WAL statistics."
  [^DurableStorage durable-storage]
  (when-let [wal-instance (:wal durable-storage)]
    (let [pending (wal/pending-entries wal-instance)]
      {:pending-count (count pending)
       :pending-entries (map #(select-keys % [:id :operation :document-id :state]) pending)})))

(defn force-wal-truncate!
  "Force truncation of completed WAL entries."
  [^DurableStorage durable-storage]
  (when-let [wal-instance (:wal durable-storage)]
    (wal/truncate! wal-instance)))

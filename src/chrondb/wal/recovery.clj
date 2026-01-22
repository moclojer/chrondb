;; This file is part of ChronDB.
;;
;; ChronDB is free software: you can redistribute it and/or modify
;; it under the terms of the GNU Affero General Public License as published
;; by the Free Software Foundation, either version 3 of the License,
;; or (at your option) any later version.
(ns chrondb.wal.recovery
  "WAL recovery logic for ChronDB.
   Handles crash recovery by replaying pending WAL entries."
  (:require [chrondb.wal.core :as wal]
            [chrondb.util.logging :as log]))

(defn- recover-save-entry!
  "Recover a save operation from WAL entry."
  [storage index entry]
  (let [{:keys [state content branch]} entry]
    (case state
      :pending
      ;; Full replay: save to storage, then index
      (do
        (log/log-info (str "Recovery: Replaying pending save for " (:document-id entry)))
        (when-let [save-fn (resolve 'chrondb.storage.protocol/save-document)]
          (save-fn storage content branch))
        :git-committed)

      :git-committed
      ;; Only need to index
      (do
        (log/log-info (str "Recovery: Indexing for " (:document-id entry)))
        (when (and index content)
          (when-let [index-fn (resolve 'chrondb.index.protocol/index-document)]
            (index-fn index content)))
        :completed)

      :index-committed
      ;; Just mark completed
      :completed

      ;; Already completed or rolled back
      state)))

(defn- recover-delete-entry!
  "Recover a delete operation from WAL entry."
  [storage index entry]
  (let [{:keys [state document-id branch]} entry]
    (case state
      :pending
      ;; Full replay: delete from storage, then index
      (do
        (log/log-info (str "Recovery: Replaying pending delete for " document-id))
        (when-let [delete-fn (resolve 'chrondb.storage.protocol/delete-document)]
          (delete-fn storage document-id branch))
        :git-committed)

      :git-committed
      ;; Only need to remove from index
      (do
        (log/log-info (str "Recovery: Removing from index for " document-id))
        (when index
          (when-let [delete-fn (resolve 'chrondb.index.protocol/delete-document)]
            (delete-fn index document-id)))
        :completed)

      :index-committed
      :completed

      state)))

(defn recover-entry!
  "Recover a single WAL entry based on its operation type."
  [storage index wal-instance entry]
  (let [{:keys [id operation]} entry
        new-state (case operation
                    :save (recover-save-entry! storage index entry)
                    :delete (recover-delete-entry! storage index entry)
                    ;; Unknown operation, mark as rolled back
                    (do
                      (log/log-warn (str "Recovery: Unknown operation " operation " for entry " id))
                      :rolled-back))]
    ;; Update WAL entry state
    (when (not= new-state (:state entry))
      (wal/mark-state! wal-instance id new-state))
    new-state))

(defn recover!
  "Recover all pending WAL entries.
   Should be called on startup before accepting new operations.
   Returns a map with recovery statistics."
  [storage index wal-instance]
  (log/log-info "WAL Recovery: Starting recovery process")
  (let [pending (wal/pending-entries wal-instance)
        count-pending (count pending)]
    (if (zero? count-pending)
      (do
        (log/log-info "WAL Recovery: No pending entries to recover")
        {:recovered 0 :failed 0 :entries []})
      (do
        (log/log-info (str "WAL Recovery: Found " count-pending " pending entries"))
        (let [results (reduce
                       (fn [acc entry]
                         (try
                           (let [new-state (recover-entry! storage index wal-instance entry)]
                             (update acc
                                     (if (= new-state :completed) :recovered :failed)
                                     inc))
                           (catch Exception e
                             (log/log-error (str "WAL Recovery: Failed to recover entry "
                                                 (:id entry) ": " (.getMessage e)))
                             (wal/mark-state! wal-instance (:id entry) :rolled-back)
                             (update acc :failed inc))))
                       {:recovered 0 :failed 0}
                       pending)]
          ;; Truncate completed entries
          (wal/truncate! wal-instance)
          (log/log-info (str "WAL Recovery: Completed. Recovered: " (:recovered results)
                             ", Failed: " (:failed results)))
          (assoc results :entries (map :id pending)))))))

(defn check-wal-health
  "Check if WAL is healthy and has no stale entries.
   Returns a health status map."
  [wal-instance max-pending-age-ms]
  (let [pending (wal/pending-entries wal-instance)
        now (System/currentTimeMillis)
        stale-entries (filter
                       (fn [entry]
                         (let [created (try
                                         (.toEpochMilli (java.time.Instant/parse (:created-at entry)))
                                         (catch Exception _ 0))
                               age (- now created)]
                           (> age max-pending-age-ms)))
                       pending)]
    {:healthy (empty? stale-entries)
     :pending-count (count pending)
     :stale-count (count stale-entries)
     :stale-entries (map :id stale-entries)}))

(ns chrondb.tools.migrator
  (:require [chrondb.storage.protocol :as storage]
            [chrondb.storage.git :as git]
            [chrondb.index.lucene :as lucene]
            [chrondb.index.protocol :as index]
            [chrondb.util.logging :as log]
            [clojure.string :as str]))

(defn- ensure-table-prefix
  "Ensures that the given ID has the table prefix"
  [id table]
  (if (str/includes? id (str table ":"))
    id
    (str table ":" id)))

(defn migrate-ids
  "Migrates existing IDs in the database to include the table prefix.
   Params:
   - storage: Storage implementation
   - index: Index implementation (optional)
   - table-name: Table name to migrate
   - table-fields: Fields that identify this table (ex: [:name :email] for users)
   Returns:
   - Number of migrated documents"
  [storage index table-name table-fields]
  (log/log-info (str "Starting ID migration for table: " table-name))

  (let [all-docs (storage/get-documents-by-prefix storage "")
        ; Filters documents that seem to belong to this table based on fields
        table-docs (filter (fn [doc]
                             (and (not (str/includes? (:id doc) (str table-name ":")))
                                  (every? #(contains? doc %) table-fields)))
                           all-docs)
        migrated-count (atom 0)]

    (log/log-info (str "Found " (count table-docs) " documents to migrate"))

    (doseq [doc table-docs]
      (let [old-id (:id doc)
            new-id (ensure-table-prefix old-id table-name)
            new-doc (assoc doc :id new-id)]

        (log/log-info (str "Migrating document: " old-id " -> " new-id))

        ; Saves with new ID
        (let [saved (storage/save-document storage new-doc)]
          (when index
            (index/index-document index saved))

          ; Removes old document
          (storage/delete-document storage old-id)
          (when index
            (index/delete-document index old-id))

          (swap! migrated-count inc))))

    (log/log-info (str "Migration complete. " @migrated-count " documents migrated"))
    @migrated-count))

(defn -main
  "ID migration utility.
   Args:
   - A table and fields (optional) to migrate (ex: 'user' or 'user:name,email')"
  [& args]
  (log/init! {:min-level :info})
  (let [data-dir "data"
        ; Creates or uses storage
        _ (log/log-info "Initializing storage and index")
        storage (git/create-git-storage data-dir)
        index (lucene/create-lucene-index (str data-dir "/index"))

        ; Processes arguments
        table-arg (first args)

        [table-name fields-str] (if (str/includes? table-arg ":")
                                  (str/split table-arg #":")
                                  [table-arg nil])

        table-fields (if fields-str
                       (mapv keyword (str/split fields-str #","))
                       ; Default values by table
                       (case table-name
                         "user" [:name :email]
                         []))]

    (try
      (log/log-info (str "Migrating table: " table-name " (fields: " table-fields ")"))
      (let [count (migrate-ids storage index table-name table-fields)]
        (log/log-info (str "Migration successfully completed: " count " documents migrated")))

      (catch Exception e
        (log/log-error (str "Error during migration: " (.getMessage e))))

      (finally
        (.close storage)
        (.close index)
        (log/log-info "Resources released")))))
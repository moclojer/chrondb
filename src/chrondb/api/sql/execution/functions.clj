(ns chrondb.api.sql.execution.functions
  "Implementation of SQL functions such as aggregations"
  (:require [chrondb.util.logging :as log]
            [chrondb.storage.protocol :as protocol]
            [chrondb.storage.git :as git]
            [clojure.string :as str]
            [clojure.data.json :as json]
            [clojure.set :as set])
  (:import [java.util Date]))

(defn process-aggregate-result
  "Formats an aggregate result to be displayed to the user"
  [function result field]
  (try
    (let [_ (log/log-info (str "Processing aggregate result - Function: " function
                               ", Result: " result
                               ", Field: " field))
          safe-result (if (nil? result)
                        (case function
                          :count 0
                          :sum 0
                          :avg 0
                          nil)
                        result)
          col-name (str (name function) "_" field)
          _ (log/log-info (str "Formatted aggregate result: " col-name " = " safe-result))]
      {(keyword col-name) safe-result})
    (catch Exception e
      (log/log-error (str "Error processing aggregation result: " (.getMessage e)
                          "\n" (.printStackTrace e)))
      {:error "Error processing aggregation result"})))

(defn execute-aggregate-function
  "Executes an aggregate function on a collection of documents.
   Parameters:
   - function: The aggregate function to execute (:count, :sum, :avg, :min, :max)
   - docs: The documents to operate on
   - field: The field to aggregate
   Returns: The result of the aggregate function"
  [function docs field]
  (try
    (log/log-info (str "Executing aggregate function: " function " on field: " field
                       ", Docs count: " (count docs)))
    (let [all-values (mapv #(get % (keyword field)) docs)
          _ (log/log-info (str "All values for field '" field "': " all-values))

          ;; Extract numeric part from field values (useful for IDs with prefixes like "user:1")
          numeric-values (keep (fn [v]
                                 (try
                                   (if (string? v)
                                     ; Handle both plain IDs and prefixed IDs
                                     (cond
                                       ; Try to extract numeric part from prefixed IDs like "user:1"
                                       (re-find #".*:(\d+)$" v)
                                       (Double/parseDouble (second (re-find #".*:(\d+)$" v)))

                                       ; Try to parse as plain numeric ID
                                       (re-matches #"^\d+(\.\d+)?$" v)
                                       (Double/parseDouble v)

                                       :else nil)
                                     (when (number? v) v))
                                   (catch Exception _
                                     nil)))
                               all-values)
          _ (log/log-info (str "Extracted numeric values: " numeric-values))]

      (case function
        :count (if (= field "*")
                 (count docs)  ;; For count(*), count all documents
                 (count (filter some? all-values)))  ;; For count(field), count only non-null values

        :sum (if (empty? numeric-values)
               0
               (reduce + numeric-values))

        :avg (if (empty? numeric-values)
               0
               (/ (reduce + numeric-values) (count numeric-values)))

        :min (if (empty? numeric-values)
               nil
               (apply min numeric-values))

        :max (if (empty? numeric-values)
               nil
               (apply max numeric-values))

        ;; Default case
        (do
          (log/log-warn (str "Unsupported aggregate function: " function))
          nil)))
    (catch Exception e
      (log/log-error (str "Error executing aggregation function: " (.getMessage e)))
      (let [sw (java.io.StringWriter.)
            pw (java.io.PrintWriter. sw)]
        (.printStackTrace e pw)
        (log/log-error (.toString sw)))
      nil)))

(defn execute-chrondb-history
  "Executes the chrondb_history function.
   Parameters:
   - storage: The storage implementation
   - table-name: The name of the table (collection)
   - id: The ID of the document in the table
   Returns: The document history"
  [storage table-name id]
  (log/log-info (str "✨ Executing chrondb_history for " table-name ", id=" id))

  (try
    (let [;; Explicitly construct the document ID with table prefix
          prefixed-id (if (str/includes? id ":")
                        id
                        (str table-name ":" id))]

      (log/log-info (str "Using prefixed ID for history: " prefixed-id))

      ;; Use the protocol function to get document history
      (let [history (protocol/get-document-history storage prefixed-id nil)]

        (log/log-info (str "History retrieved: " (count history) " entries"))

        ;; Convert to the format expected by SQL
        (mapv (fn [entry]
                {:commit_id (or (:commit-id entry) "unknown")
                 :timestamp (if (:commit-time entry)
                              (str (:commit-time entry))
                              (str (Date.)))
                 :committer (or (:committer-name entry) "unknown")
                 :data (pr-str (:document entry))})
              history)))

    (catch Exception e
      (log/log-error (str "❌ Error retrieving history: " (.getMessage e)))
      [])))

(defn execute-chrondb-at
  "Executes the chrondb_at function.
   Parameters:
   - storage: The storage implementation
   - table-name: The name of the table (collection)
   - id: The ID of the document in the table
   - commit: The commit hash to retrieve
   Returns: The document at the specified commit"
  [storage table-name id commit]
  (log/log-info (str "Executing chrondb_at for " table-name ", id=" id " at commit " commit))

  (try
    (let [repository (:repository storage)
          ;; Explicitly construct the document ID with table prefix
          prefixed-id (if (str/includes? id ":")
                        id
                        (str table-name ":" id))]

      (log/log-info (str "Using prefixed ID: " prefixed-id))

      ;; For the chrondb_at function, we still need to use the repository directly
      ;; since the protocol doesn't have a get-document-at-commit function
      (if repository
        (-> (git/get-document-at-commit repository prefixed-id commit)
            ;; Ensure document has the correct table
            (cond-> (not (str/includes? prefixed-id ":"))
              (assoc :_table table-name)))
        nil))

    (catch Exception e
      (log/log-error (str "Error retrieving document at commit: " (.getMessage e)))
      nil)))

(defn execute-chrondb-diff
  "Executes the chrondb_diff function.
   Parameters:
   - storage: The storage implementation
   - table-name: The name of the table (collection)
   - id: The ID of the document in the table
   - commit1: First commit hash to compare
   - commit2: Second commit hash to compare
   Returns: The diff between commits"
  [storage table-name id commit1 commit2]
  (log/log-info (str "Executing chrondb_diff for " table-name ", id=" id " between " commit1 " and " commit2))

  (try
    (let [repository (:repository storage)
          ;; Explicitly construct the document ID with table prefix
          prefixed-id (if (str/includes? id ":")
                        id
                        (str table-name ":" id))]

      (if repository
        (let [;; Get documents at both commits
              doc1 (git/get-document-at-commit repository prefixed-id commit1)
              doc2 (git/get-document-at-commit repository prefixed-id commit2)]

          ;; Check if both documents were found
          (if (and doc1 doc2)
            ;; Calculate differences
            (let [keys1 (set (keys doc1))
                  keys2 (set (keys doc2))
                  added-keys (clojure.set/difference keys2 keys1)
                  removed-keys (clojure.set/difference keys1 keys2)
                  common-keys (clojure.set/intersection keys1 keys2)

                  ;; Collect changes in common fields
                  changed-map (reduce (fn [acc k]
                                        (let [v1 (get doc1 k)
                                              v2 (get doc2 k)]
                                          (if (not= v1 v2)
                                            (assoc acc k {:old v1 :new v2})
                                            acc)))
                                      {}
                                      common-keys)

                  ;; Prepare results
                  added (when (seq added-keys)
                          (pr-str (select-keys doc2 added-keys)))
                  removed (when (seq removed-keys)
                            (pr-str (select-keys doc1 removed-keys)))
                  changed (when (seq changed-map)
                            (pr-str changed-map))]

              [{:id id
                :commit1 commit1
                :commit2 commit2
                :added added
                :removed removed
                :changed changed}])
            []))
        []))

    (catch Exception e
      (log/log-error (str "Error comparing document versions: " (.getMessage e)))
      [])))
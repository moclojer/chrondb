(ns chrondb.api.sql.execution.functions
  "Implementation of SQL functions such as aggregations"
  (:require [chrondb.util.logging :as log]
            [chrondb.storage.protocol :as protocol]
            [chrondb.storage.git.history :as git-history]
            [clojure.string :as str]
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

          ;; Extract numeric values from field data
          numeric-values (keep (fn [v]
                                 (try
                                   (cond
                                     (number? v) (double v)
                                     (string? v)
                                     (let [trimmed (str/trim v)]
                                       (cond
                                         (str/blank? trimmed) nil
                                         ;; Direct numeric parse (integers, decimals, negatives)
                                         (re-matches #"^-?\d+(\.\d+)?$" trimmed)
                                         (Double/parseDouble trimmed)
                                         ;; Extract numeric suffix from prefixed IDs like "user:1"
                                         (re-find #":(\d+)$" trimmed)
                                         (Double/parseDouble (second (re-find #":(\d+)$" trimmed)))
                                         :else nil))
                                     :else nil)
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
    (let [;; Check if the ID already has a table prefix or is bare
          has-table-prefix (str/includes? id ":")

          ;; Construct the document ID appropriately
          prefixed-id (if has-table-prefix
                        id  ;; Already has a prefix like "user:1"
                        (str table-name ":" id))  ;; Add the prefix

          ;; Keep track of the search ID format for strict matching
          search-id-format (if has-table-prefix
                             :prefixed    ;; User requested with prefix "user:1"
                             :bare)       ;; User requested with bare ID "1"

          _ (log/log-info (str "Using prefixed ID for history: " prefixed-id " (search format: " search-id-format ")"))

          ;; Use the protocol function to get document history
          history (protocol/get-document-history storage prefixed-id nil)

          ;; Filter results to only match the correct table and EXACT ID FORMAT that was requested
          filtered-history (filter (fn [entry]
                                     (let [doc (:document entry)
                                           doc-id (str (:id doc))
                                           doc-table (:_table doc)
                                           ;; Check if the document ID format matches what was requested
                                           id-format-matches? (case search-id-format
                                                                ;; When user asked for bare ID "1", only match docs with ID="1"
                                                                :bare (= doc-id id)
                                                                ;; When user asked for prefixed ID "user:1", only match docs with ID="user:1"
                                                                :prefixed (= doc-id prefixed-id))]

                                       ;; Keep entries where:
                                       ;; 1. The document is from the requested table, and
                                       ;; 2. The document ID format exactly matches what was requested
                                       (and (= doc-table table-name)
                                            id-format-matches?)))
                                   history)]

      (log/log-info (str "History retrieved: " (count filtered-history) " entries (filtered from " (count history) " total)"))

      ;; Convert to the format expected by SQL
      (mapv (fn [entry]
              {:commit_id (or (:commit-id entry) "unknown")
               :timestamp (if (:commit-time entry)
                            (str (:commit-time entry))
                            (str (Date.)))
               :committer (or (:committer-name entry) "unknown")
               :data (pr-str (:document entry))})
            filtered-history))

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
        (-> (git-history/get-document-at-commit repository prefixed-id commit)
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
              doc1 (git-history/get-document-at-commit repository prefixed-id commit1)
              doc2 (git-history/get-document-at-commit repository prefixed-id commit2)]

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
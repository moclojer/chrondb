(ns chrondb.api.sql.execution.query
  "SQL query execution"
  (:require [clojure.string :as str]
            [chrondb.util.logging :as log]
            [chrondb.api.sql.parser.statements :as statements]
            [chrondb.api.sql.protocol.messages :as messages]
            [chrondb.api.sql.execution.operators :as operators]
            [chrondb.api.sql.execution.functions :as functions]
            [chrondb.storage.protocol :as storage]
            [chrondb.index.protocol :as index]))

(defn- get-documents-by-id
  "Retrieves a document by ID"
  [storage where-condition]
  (let [id (str/replace (get-in where-condition [0 :value]) #"['\"]" "")]
    (log/log-debug (str "Searching document by ID: " id))
    (if-let [doc (storage/get-document storage id)]
      [doc]
      [])))

(defn- get-all-documents
  "Retrieves all documents"
  [storage]
  (log/log-debug "Performing full document scan")
  (let [docs (storage/get-documents-by-prefix storage "")]
    (log/log-debug (str "Retrieved " (count docs) " documents"))
    (or (seq docs) [])))

(defn- process-group-columns
  "Processes the columns of a group"
  [group columns]
  (reduce
   (fn [acc col-def]
     (case (:type col-def)
       :all
       (if (empty? group)
         acc
         (merge acc (first group)))

       :column
       (if-let [alias (:alias col-def)]
         (assoc acc (keyword alias) (get (first group) (keyword (:column col-def))))
         (assoc acc (keyword (:column col-def)) (get (first group) (keyword (:column col-def)))))

       :aggregate-function
       (let [fn-result (functions/execute-aggregate-function
                        (:function col-def)
                        group
                        (first (:args col-def)))]
         (assoc acc (keyword (str (name (:function col-def)) "_" (first (:args col-def)))) fn-result))

       ;; Default
       acc))
   {}
   columns))

(defn- process-groups
  "Processes all groups"
  [grouped-docs query]
  (mapv
   (fn [group]
     (process-group-columns group (:columns query)))
   grouped-docs))

(defn handle-select
  "Handles a SELECT query.
   Parameters:
   - storage: The storage implementation
   - query: The parsed query
   Returns: A sequence of result documents"
  [storage query]
  (try
    (let [where-condition (:where query)
          group-by (:group-by query)
          order-by (:order-by query)
          limit (:limit query)
          table-name (:table query)

         ;; Detailed logs for debugging
          _ (log/log-info (str "Executing SELECT on table: " table-name))
          _ (log/log-debug (str "WHERE condition: " where-condition))
          _ (log/log-debug (str "Columns: " (:columns query)))

         ;; Retrieve documents
          all-docs (if (and where-condition
                            (seq where-condition)
                            (= (count where-condition) 1)
                            (get-in where-condition [0 :field])
                            (= (get-in where-condition [0 :field]) "id")
                            (= (get-in where-condition [0 :op]) "="))
                     (get-documents-by-id storage where-condition)
                     (get-all-documents storage))

          _ (log/log-debug (str "Total documents retrieved: " (count all-docs)))

         ;; Apply WHERE filters
          filtered-docs (if where-condition
                          (do
                            (log/log-debug "Applying WHERE filters")
                            (operators/apply-where-conditions all-docs where-condition))
                          all-docs)

          _ (log/log-debug (str "After filtering: " (count filtered-docs) " documents"))

         ;; Group and process documents
          grouped-docs (operators/group-docs-by filtered-docs group-by)
          processed-groups (process-groups grouped-docs query)

         ;; Sort and limit results
          sorted-results (operators/sort-docs-by processed-groups order-by)
          limited-results (operators/apply-limit sorted-results limit)

          _ (log/log-info (str "Returning " (count limited-results) " results"))]

      (or limited-results []))
    (catch Exception e
      (let [sw (java.io.StringWriter.)
            pw (java.io.PrintWriter. sw)]
        (.printStackTrace e pw)
        (log/log-error (str "Error in handle-select: " (.getMessage e) "\n" (.toString sw))))
      [])))

(defn handle-insert
  "Handles an INSERT query.
   Parameters:
   - storage: The storage implementation
   - doc: The document to insert
   Returns: The saved document"
  [storage doc]
  (log/log-info (str "Inserting document: " doc))
  (let [result (storage/save-document storage doc)]
    (log/log-info (str "Document inserted successfully: " result))
    result))

(defn handle-update
  "Handles an UPDATE query.
   Parameters:
   - storage: The storage implementation
   - id: The ID of the document to update
   - updates: The updates to apply
   Returns: The updated document or nil if not found"
  [storage id updates]
  (try
    (log/log-debug (str "Trying to update document with ID: " id))
    (when-let [doc (storage/get-document storage id)]
      (log/log-debug (str "Document found for update: " doc))
      (let [updated-doc (merge doc updates)]
        (log/log-debug (str "Merged document: " updated-doc))
        (storage/save-document storage updated-doc)))
    (catch Exception e
      (let [sw (java.io.StringWriter.)
            pw (java.io.PrintWriter. sw)]
        (.printStackTrace e pw)
        (log/log-error (str "Error in handle-update: " (.getMessage e) "\n" (.toString sw))))
      nil)))

(defn handle-delete
  "Handles a DELETE query.
   Parameters:
   - storage: The storage implementation
   - id: The ID of the document to delete
   Returns: The deleted document or nil if not found"
  [storage id]
  (storage/delete-document storage id))

(defn handle-select-case
  "Handles the SELECT case of an SQL query"
  [storage out parsed]
  (let [results (handle-select storage parsed)]
    (if (empty? results)
      ;; For empty results - special format
      (do
        ;; Send an empty row description - this is crucial for 0 rows
        (messages/send-row-description out [])

        ;; Send a complete command with count 0
        ;; This specific format informs the client that there are no rows
        (messages/send-command-complete out "SELECT" 0))

      ;; For non-empty results, proceed normally
      (let [columns (mapv name (keys (first results)))]
        (messages/send-row-description out columns)
        (doseq [row results]
          (messages/send-data-row out (map #(get row (keyword %)) columns)))
        (messages/send-command-complete out "SELECT" (count results))))))

(defn handle-insert-case
  "Handles the INSERT case of an SQL query"
  [storage index out parsed]
  (try
    (log/log-info (str "Processing INSERT in table: " (:table parsed)))
    (log/log-debug (str "Values: " (:values parsed) ", columns: " (:columns parsed)))

    (let [values (:values parsed)
          columns (:columns parsed)
          clean-values (if (seq values)
                         (mapv #(str/replace % #"['\"]" "") values)
                         [])

          _ (log/log-debug (str "Clean values for INSERT: " clean-values))

          doc (if (and (seq clean-values) (seq columns))
                (let [doc-map (reduce (fn [acc [col val]]
                                        (assoc acc (keyword col) val))
                                      {:id (str (java.util.UUID/randomUUID))}
                                      (map vector columns clean-values))]
                  (log/log-debug (str "Document created with column mapping: " doc-map))
                  doc-map)
                (let [doc-map (cond
                                (>= (count clean-values) 2) {:id (first clean-values)
                                                             :value (second clean-values)}
                                (= (count clean-values) 1) {:id (str (java.util.UUID/randomUUID))
                                                            :value (first clean-values)}
                                :else {:id (str (java.util.UUID/randomUUID)) :value ""})]
                  (log/log-debug (str "Document created with default format: " doc-map))
                  doc-map))

          _ (log/log-info (str "Document to insert: " doc))
          saved (handle-insert storage doc)]

      (when index
        (log/log-debug "Indexing document")
        (index/index-document index saved))

      (log/log-info (str "INSERT completed successfully, ID: " (:id saved)))
      (messages/send-command-complete out "INSERT" 1))
    (catch Exception e
      (let [sw (java.io.StringWriter.)
            pw (java.io.PrintWriter. sw)]
        (.printStackTrace e pw)
        (log/log-error (str "Error processing INSERT: " (.getMessage e) "\n" (.toString sw))))
      (messages/send-error-response out (str "Error processing INSERT: " (.getMessage e)))
      (messages/send-command-complete out "INSERT" 0))))

(defn handle-update-case
  "Handles the UPDATE case of an SQL query"
  [storage index out parsed]
  (try
    (log/log-info (str "Processing UPDATE in table: " (:table parsed)))
    (let [where-conditions (:where parsed)
          updates (try
                    (reduce-kv
                     (fn [m k v]
                       (assoc m (keyword k) (str/replace v #"['\"]" "")))
                     {}
                     (:updates parsed))
                    (catch Exception e
                      (log/log-error (str "Error processing update values: " (.getMessage e)))
                      {}))]

      (log/log-debug (str "Update conditions: " where-conditions ", values: " updates))

      (if (and (seq where-conditions) (seq updates))
        (let [_ (log/log-info (str "Searching for documents matching: " where-conditions))
              all-docs (storage/get-documents-by-prefix storage "")
              matching-docs (operators/apply-where-conditions all-docs where-conditions)
              update-count (atom 0)]

          (if (seq matching-docs)
            (do
              (log/log-info (str "Found " (count matching-docs) " documents to update"))
              (doseq [doc matching-docs]
                (let [updated-doc (merge doc updates)
                      _ (log/log-debug (str "Updating document: " (:id doc) " with values: " updates))
                      _ (log/log-debug (str "Merged document: " updated-doc))
                      saved (storage/save-document storage updated-doc)]

                  (when (and saved index)
                    (log/log-debug (str "Re-indexing updated document: " (:id saved)))
                    (index/index-document index saved))

                  (swap! update-count inc)))

              (log/log-info (str "Updated " @update-count " documents successfully"))
              (messages/send-command-complete out "UPDATE" @update-count))

            (do
              (log/log-warn "No documents found matching WHERE conditions")
              (messages/send-error-response out "No documents found matching WHERE conditions")
              (messages/send-command-complete out "UPDATE" 0))))

        (do
          (log/log-warn "Invalid UPDATE query - missing WHERE clause or invalid update values")
          (let [error-msg (if (seq where-conditions)
                            "UPDATE failed: No valid values to update"
                            "UPDATE without WHERE clause is not supported. Please specify conditions.")]
            (log/log-error error-msg)
            (messages/send-error-response out error-msg)
            (messages/send-command-complete out "UPDATE" 0)))))
    (catch Exception e
      (let [sw (java.io.StringWriter.)
            pw (java.io.PrintWriter. sw)]
        (.printStackTrace e pw)
        (log/log-error (str "Error processing UPDATE: " (.getMessage e) "\n" (.toString sw))))
      (messages/send-error-response out (str "Error processing UPDATE: " (.getMessage e)))
      (messages/send-command-complete out "UPDATE" 0))))

(defn handle-delete-case
  "Handles the DELETE case of an SQL query"
  [storage index out parsed]
  (let [id (when (seq (:where parsed))
             (str/replace (get-in parsed [:where 0 :value]) #"['\"]" ""))]
    (if id
      (let [_ (log/log-info (str "Deleting document with ID: " id))
            deleted (handle-delete storage id)
            _ (when (and deleted index)
                (index/delete-document index id))]
        (log/log-info (str "Document " (if deleted "deleted successfully" "not found")))
        (messages/send-command-complete out "DELETE" (if deleted 1 0)))
      (do
        (log/log-warn "Attempt to DELETE without WHERE clause was rejected")
        (let [error-msg "DELETE without WHERE clause is not supported. Use WHERE id='value' to specify which document to delete."]
          (log/log-error error-msg)
          (messages/send-error-response out error-msg))))))

(defn handle-query
  "Handles an SQL query.
   Parameters:
   - storage: The storage implementation
   - index: The index implementation
   - out: The output stream to write results to
   - sql: The SQL query string
   Returns: nil"
  [storage index ^java.io.OutputStream out sql]
  (log/log-info (str "Executing query: " sql))
  (let [parsed (statements/parse-sql-query sql)]
    (log/log-debug (str "Query parsed: " parsed))
    (case (:type parsed)
      :select (handle-select-case storage out parsed)
      :insert (handle-insert-case storage index out parsed)
      :update (handle-update-case storage index out parsed)
      :delete (handle-delete-case storage index out parsed)
      (do
        (messages/send-error-response out (str "Unknown command: " sql))
        (messages/send-command-complete out "UNKNOWN" 0)))))
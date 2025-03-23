(ns chrondb.api.sql.execution.query
  "SQL query execution"
  (:require [clojure.string :as str]
            [clojure.set :as set]
            [clojure.data.json :as json]
            [clojure.string :as string]
            [chrondb.util.logging :as log]
            [chrondb.api.sql.parser.statements :as statements]
            [chrondb.api.sql.protocol.messages :as messages]
            [chrondb.api.sql.execution.operators :as operators]
            [chrondb.api.sql.execution.functions :as functions]
            [chrondb.storage.protocol :as storage]
            [chrondb.index.protocol :as index]
            [chrondb.storage.protocol :as protocol]))

(defn- get-documents-by-id
  "Retrieves a document by ID"
  [storage where-condition]
  (let [id (str/replace (get-in where-condition [0 :value]) #"['\"]" "")
        table-name (get-in where-condition [0 :table])
        prefixed-id (if (and (seq table-name) (not (str/includes? id (str table-name ":"))))
                      (str table-name ":" id)
                      id)]
    (log/log-debug (str "Searching document by ID: " id))
    (log/log-debug (str "Table name: " table-name))
    (log/log-debug (str "Using prefixed ID for search: " prefixed-id))
    (if-let [doc (or (storage/get-document storage prefixed-id)
                     (storage/get-document storage id))]
      [doc]
      [])))

(defn- get-all-documents
  "Retrieves all documents"
  [storage]
  (log/log-debug "Performing full document scan")
  (let [docs (storage/get-documents-by-prefix storage "")]
    (log/log-debug (str "Retrieved " (count docs) " documents"))
    (or (seq docs) [])))

(defn- get-all-documents-for-table
  "Retrieves all documents for a specific table"
  [storage table-name]
  (log/log-info (str "Starting document search for table: " table-name))
  (let [documents (storage/get-documents-by-table storage table-name)]
    (log/log-info (str "Found " (count documents) " documents for table: " table-name))
    (when (seq documents)
      (log/log-debug (str "Found document IDs: " (string/join ", " (map :id documents)))))
    documents))

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
          _ (log/log-debug (str "LIMIT: " limit))
          _ (log/log-debug (str "ORDER BY: " order-by))
          _ (log/log-debug (str "GROUP BY: " group-by))

         ;; Retrieve documents
          condition-by-id? (and where-condition
                                (seq where-condition)
                                (= (count where-condition) 1)
                                (get-in where-condition [0 :field])
                                (= (get-in where-condition [0 :field]) "id")
                                (= (get-in where-condition [0 :op]) "="))
          _ (log/log-debug (str "Search by ID? " condition-by-id?))

          all-docs (if condition-by-id?
                     (get-documents-by-id storage where-condition)
                     (if (seq table-name)
                       (get-all-documents-for-table storage table-name)
                       (get-all-documents storage)))

          _ (log/log-info (str "Total documents retrieved: " (count all-docs)))
          _ (log/log-info (str "Retrieved IDs: " (mapv :id all-docs)))
          _ (when (seq all-docs)
              (log/log-debug "Sample document: " (first all-docs))
              (log/log-debug "Document IDs: " (mapv :id all-docs)))

         ;; Apply WHERE filters
          filtered-docs (if where-condition
                          (do
                            (log/log-debug "Applying WHERE filters")
                            (let [result (operators/apply-where-conditions all-docs where-condition)]
                              (log/log-info (str "After WHERE filtering: " (count result) " documents, IDs: " (mapv :id result)))
                              result))
                          all-docs)

          _ (log/log-debug (str "After filtering: " (count filtered-docs) " documents"))
          _ (when (seq filtered-docs)
              (log/log-debug "Filtered document IDs: " (mapv :id filtered-docs)))

         ;; Group and process documents
          processed-docs (if (seq group-by)
                          ;; If there's a GROUP BY, group and process
                           (let [grouped-docs (operators/group-docs-by filtered-docs group-by)
                                 _ (log/log-info (str "After grouping: " (count grouped-docs) " groups"))]
                             (process-groups grouped-docs query))
                          ;; If there's no GROUP BY, don't group - process each document directly
                           (do
                             (log/log-info "No grouping: processing documents directly")
                            ;; When there's no GROUP BY, we simply take the filtered documents
                            ;; and apply column projection to each one
                             (mapv (fn [doc]
                                     (let [columns (:columns query)]
                                      ;; For each document, select columns according to the query
                                       (if (= 1 (count columns))
                                         (let [col (first columns)]
                                           (if (= :all (:type col))
                                            ;; If it's SELECT *, keep the document as is
                                             doc
                                            ;; Otherwise, select only the specific column
                                             {(keyword (:column col)) (get doc (keyword (:column col)))}))
                                        ;; If there are multiple columns, select each one
                                         (reduce (fn [acc col]
                                                   (if (= :all (:type col))
                                                    ;; If one of the columns is *, keep everything
                                                     (merge acc doc)
                                                    ;; Otherwise, add only the specific column
                                                     (assoc acc (keyword (:column col))
                                                            (get doc (keyword (:column col))))))
                                                 {}
                                                 columns))))
                                   filtered-docs)))

          _ (log/log-info (str "After processing: " (count processed-docs) " documents"))

         ;; Sort and limit results
          sorted-results (operators/sort-docs-by processed-docs order-by)
          _ (log/log-info (str "After sorting: " (count sorted-results) " documents"))

          limited-results (operators/apply-limit sorted-results limit)
          _ (log/log-info (str "After applying limit: " (count limited-results) " documents"))
          _ (log/log-info (str "Final documents: " (mapv :id limited-results)))

          _ (log/log-info (str "Returning " (count limited-results) " results"))
          _ (when (seq limited-results)
              (log/log-debug "Result document IDs: " (mapv :id limited-results)))]

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
  (log/log-info (str "Starting handle-select-case with parsed: " parsed))
  (let [results (handle-select storage parsed)]
    (log/log-info (str "Results obtained: " (count results) " documents"))
    (log/log-info (str "Complete details of results: " (mapv :id results)))

    (if (empty? results)
      ;; For empty results - special format
      (do
        ;; Send an empty row description - this is crucial for 0 rows
        (log/log-info "Sending empty row description for 0 results")
        (messages/send-row-description out [])

        ;; Send a complete command with count 0
        ;; This specific format informs the client that there are no rows
        (messages/send-command-complete out "SELECT" 0))

      ;; For non-empty results, proceed normally
      (let [columns (mapv name (keys (first results)))]
        (log/log-info (str "Sending column description: " columns))
        (messages/send-row-description out columns)
        (doseq [row results]
          (log/log-info (str "Sending row: " (:id row)))
          (log/log-debug (str "Complete row content: " row))
          (let [values (map #(get row (keyword %)) columns)]
            (log/log-info (str "Values sent: " values))
            (messages/send-data-row out values)))
        (messages/send-command-complete out "SELECT" (count results))))
    (log/log-info "SELECT query processing completed")))

(defn handle-insert-case
  "Handles the INSERT case of an SQL query"
  [storage index out parsed]
  (try
    (log/log-info (str "Processing INSERT in table: " (:table parsed)))
    (log/log-debug (str "Values: " (:values parsed) ", columns: " (:columns parsed)))

    (let [values (:values parsed)
          columns (:columns parsed)
          table-name (:table parsed)

          ;; Log the raw tokens for debugging
          _ (log/log-debug (str "Raw values tokens: " values))
          _ (log/log-debug (str "Raw columns tokens: " columns))

          ;; Make sure we only have valid column names
          clean-columns (when (seq columns)
                          (map (fn [col]
                                 ;; Ensure column names are valid identifiers
                                 (let [clean-col (-> col
                                                     (str/replace #"^['\"]|['\"]$" "") ;; Remove quotes
                                                     (str/replace #"[^\w\d_]" "_"))] ;; Replace invalid chars with underscore
                                   clean-col))
                               columns))

          _ (log/log-debug (str "Cleaned columns: " clean-columns))

          ;; Properly clean values - keep quotes while processing
          ;; and only remove them at the end to preserve values with spaces
          doc (if (and (seq values) (seq clean-columns))
                (let [raw-id (if (and (seq clean-columns) (= (first clean-columns) "id"))
                               ;; If there's an ID column, use its value
                               (str/replace (first values) #"^['\"]|['\"]$" "")
                               ;; Otherwise, generate UUID
                               (str (java.util.UUID/randomUUID)))
                      ;; Add table prefix to ID
                      doc-id (if (str/includes? raw-id (str table-name ":"))
                               raw-id
                               (str table-name ":" raw-id))
                      doc-map (reduce (fn [acc [col val]]
                                        ;; Skip invalid column names or values
                                        (if (or (nil? col) (nil? val) (= col ",") (= val ","))
                                          acc
                                          ;; Remove quotes only now, after having the column/value pair
                                          (assoc acc (keyword col) (str/replace val #"^['\"]|['\"]$" ""))))
                                      {:id doc-id}
                                      ;; Zip columns with values, ignoring ID if already processed
                                      (if (and (seq clean-columns) (= (first clean-columns) "id"))
                                        (map vector (rest clean-columns) (rest values))
                                        (map vector clean-columns values)))]
                  (log/log-debug (str "Document created with column mapping: " doc-map))
                  doc-map)
                ;; Fallback case (no explicit columns)
                (let [raw-id (if (>= (count values) 1)
                               (str/replace (first values) #"^['\"]|['\"]$" "")
                               (str (java.util.UUID/randomUUID)))
                      ;; Add table prefix to ID
                      doc-id (if (str/includes? raw-id (str table-name ":"))
                               raw-id
                               (str table-name ":" raw-id))
                      doc-map (cond
                                (>= (count values) 2) {:id doc-id
                                                       :value (str/replace (second values) #"^['\"]|['\"]$" "")}
                                (= (count values) 1) {:id doc-id
                                                      :value ""}
                                :else {:id doc-id :value ""})]
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
        (log/log-error (str "Error processing INSERT: " (.getMessage e) "\n" (.toString sw)))
        (messages/send-error-response out (str "Error processing INSERT: " (.getMessage e)))))))

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
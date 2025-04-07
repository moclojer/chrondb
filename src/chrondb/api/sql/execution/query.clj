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
    (if-let [doc (storage/get-document storage id)]
      [doc]
      [])))

(defn- get-all-documents
  "Retrieves all documents"
  [storage]
  (let [docs (storage/get-documents-by-prefix storage "")]
    (or (seq docs) [])))

(defn- get-all-documents-for-table
  "Retrieves all documents for a specific table"
  [storage table-name]
  (log/log-info (str "Starting document search for table: " table-name))
  (let [documents (storage/get-documents-by-table storage table-name)]
    (log/log-info (str "Found " (count documents) " documents for table: " table-name))
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
                        (first (:args col-def)))
             result-map (functions/process-aggregate-result
                         (:function col-def)
                         fn-result
                         (first (:args col-def)))]
         (merge acc result-map))

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

         ;; Retrieve documents
          condition-by-id? (and where-condition
                                (seq where-condition)
                                (= (count where-condition) 1)
                                (get-in where-condition [0 :field])
                                (= (get-in where-condition [0 :field]) "id")
                                (= (get-in where-condition [0 :op]) "="))

          all-docs (if condition-by-id?
                     (get-documents-by-id storage where-condition)
                     (if (seq table-name)
                       (get-all-documents-for-table storage table-name)
                       (get-all-documents storage)))

          _ (log/log-info (str "Total documents retrieved: " (count all-docs)))

         ;; Apply WHERE filters
          filtered-docs (if where-condition
                          (let [result (operators/apply-where-conditions all-docs where-condition)]
                            (log/log-info (str "After WHERE filtering: " (count result) " documents, IDs: " (mapv :id result)))
                            result)
                          all-docs)

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
    (when-let [doc (storage/get-document storage id)]
      (let [updated-doc (merge doc updates)]
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
  (let [results (handle-select storage parsed)
        columns-def (:columns parsed)
        is-count-star-query? (and (= 1 (count columns-def))
                                  (= :aggregate-function (:type (first columns-def)))
                                  (= :count (:function (first columns-def)))
                                  (= "*" (first (:args (first columns-def)))))
        is-aggregate-query? (and (= 1 (count columns-def))
                                 (= :aggregate-function (:type (first columns-def))))]

    (log/log-info (str "Results obtained: " (count results) " documents"))
    (log/log-info (str "Is count(*) query: " is-count-star-query?))
    (log/log-info (str "Is aggregate query: " is-aggregate-query?))
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

      ;; Non-empty results
      (cond
        ;; For COUNT(*) queries
        is-count-star-query?
        (do
          (log/log-info "Processing count(*) query")
          (messages/send-row-description out ["count"])
          (messages/send-data-row out [(str (count results))])
          (messages/send-command-complete out "SELECT" 1))

        ;; For other aggregate functions (sum, avg, min, max)
        is-aggregate-query?
        (do
          (log/log-info "Processing aggregate function query")
          (let [agg-function (:function (first columns-def))
                agg-field (first (:args (first columns-def)))
                column-name (name agg-function)
                _ (log/log-info (str "Aggregate function: " agg-function ", Field: " agg-field))

                ;; Execute aggregate function directly
                agg-result (functions/execute-aggregate-function
                            agg-function
                            (if (= agg-field "*")
                              results
                              results)
                            agg-field)
                _ (log/log-info (str "Aggregate result: " agg-result))]

            (messages/send-row-description out [column-name])
            (messages/send-data-row out [(if (nil? agg-result) "0" (str agg-result))])
            (messages/send-command-complete out "SELECT" 1)))

        ;; For normal queries
        :else
        (let [;; Get column names from the first result, excluding internal fields like :_table
              columns (try
                        (->> (keys (first results))
                             (filter #(not= % :_table)) ; Exclude :_table
                             (mapv #(if (keyword? %)
                                      (name %)
                                      (str %))))
                        (catch Exception e
                          (log/log-error (str "Erro ao extrair nomes de colunas: " (.getMessage e)))
                          ["resultado"]))]
          (log/log-info (str "Sending column description: " columns))
          (messages/send-row-description out columns)
          (doseq [row results]
            (log/log-info (str "Sending row: " (:id row)))
            ;; Map values according to the filtered columns
            (let [values (map #(let [val (get row (if (string? %) (keyword %) %))]
                                 (if (nil? val) "" (str val)))
                              columns)] ; Use the filtered columns here
              (log/log-info (str "Values sent: " values))
              (messages/send-data-row out values)))
          (messages/send-command-complete out "SELECT" (count results)))))

    (log/log-info "SELECT query processing completed")))

(defn handle-insert-case
  "Handles the INSERT case of an SQL query"
  [storage index out parsed]
  (try
    (log/log-info (str "Processing INSERT in table: " (:table parsed)))

    (let [values (:values parsed)
          columns (:columns parsed)
          table-name (:table parsed)

          ;; Make sure we only have valid column names
          clean-columns (when (seq columns)
                          (map (fn [col]
                                 ;; Ensure column names are valid identifiers
                                 (let [clean-col (-> col
                                                     (str/replace #"^['\"]|['\"]$" "") ;; Remove quotes
                                                     (str/replace #"[^\w\d_]" "_"))] ;; Replace invalid chars with underscore
                                   clean-col))
                               columns))

          ;; Properly clean values - keep quotes while processing
          ;; and only remove them at the end to preserve values with spaces
          doc (if (and (seq values) (seq clean-columns))
                (let [raw-id (if (and (seq clean-columns) (= (first clean-columns) "id"))
                               ;; If there's an ID column, use its value
                               (str/replace (first values) #"^['\"]|['\"]$" "")
                               ;; Otherwise, generate UUID
                               (str (java.util.UUID/randomUUID)))
                      ;; Add table prefix to ID
                      doc-id raw-id
                      doc-map (reduce (fn [acc [col val]]
                                        ;; Skip invalid column names or values
                                        (if (or (nil? col) (nil? val) (= col ",") (= val ","))
                                          acc
                                          ;; Remove quotes only now, after having the column/value pair
                                          (assoc acc (keyword col) (str/replace val #"^['\"]|['\"]$" ""))))
                                      {:id doc-id :_table table-name}
                                      ;; Zip columns with values, ignoring ID if already processed
                                      (if (and (seq clean-columns) (= (first clean-columns) "id"))
                                        (map vector (rest clean-columns) (rest values))
                                        (map vector clean-columns values)))]
                  doc-map)
                ;; Fallback case (no explicit columns)
                (let [raw-id (if (>= (count values) 1)
                               (str/replace (first values) #"^['\"]|['\"]$" "")
                               (str (java.util.UUID/randomUUID)))
                      ;; Add table prefix to ID
                      doc-id raw-id
                      doc-map (cond
                                (>= (count values) 2) {:id doc-id :_table table-name
                                                       :value (str/replace (second values) #"^['\"]|['\"]$" "")}
                                (= (count values) 1) {:id doc-id :_table table-name
                                                      :value ""}
                                :else {:id doc-id :_table table-name :value ""})]
                  doc-map))

          _ (log/log-info (str "Document to insert: " doc))
          saved (handle-insert storage doc)]

      (when index
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
    (let [table-name (:table parsed)
          where-conditions (:where parsed)
          updates (try
                    (reduce-kv
                     (fn [m k v]
                       (assoc m (keyword k) (str/replace v #"[\'\\\"]" "")))
                     {}
                     (:updates parsed))
                    (catch Exception e
                      (log/log-error (str "Error processing update values: " (.getMessage e)))
                      {}))]

      (if (and (seq where-conditions) (seq updates) (seq table-name))
        (let [_ (log/log-info (str "Searching for documents in table " table-name " matching: " where-conditions))
              ; Get documents for the specific table first
              table-docs (get-all-documents-for-table storage table-name)
              ; Apply WHERE conditions to the table documents
              matching-docs (operators/apply-where-conditions table-docs where-conditions)
              update-count (atom 0)]

          (if (seq matching-docs)
            (do
              (log/log-info (str "Found " (count matching-docs) " documents to update"))
              (doseq [doc matching-docs]
                (let [updated-doc (merge doc updates)
                      saved (storage/save-document storage updated-doc)]

                  (when (and saved index)
                    (index/index-document index saved))

                  (swap! update-count inc)))

              (log/log-info (str "Updated " @update-count " documents successfully"))
              (messages/send-command-complete out "UPDATE" @update-count))

            (do
              (log/log-warn "No documents found matching WHERE conditions")
              (messages/send-error-response out "No documents found matching WHERE conditions")
              (messages/send-command-complete out "UPDATE" 0))))

        (do
          (log/log-warn "Invalid UPDATE query - missing WHERE clause, table name, or invalid update values")
          (let [error-msg (cond
                            (not (seq table-name)) "UPDATE failed: Table name is missing."
                            (not (seq where-conditions)) "UPDATE without WHERE clause is not supported. Please specify conditions."
                            :else "UPDATE failed: No valid values to update")]
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
  (try
    (let [table-name (:table parsed)
          where-conditions (:where parsed)
          ;; Currently assumes WHERE clause is simple `id = 'value'`
          id (when (and (seq where-conditions)
                        (= (count where-conditions) 1)
                        (= (get-in where-conditions [0 :field]) "id")
                        (= (get-in where-conditions [0 :op]) "="))
               (str/replace (get-in where-conditions [0 :value]) #"[\'\\\"]" ""))]

      (if (and id (seq table-name))
        (if-let [doc-to-delete (storage/get-document storage id)]
          (if (= (:_table doc-to-delete) table-name)
            (let [_ (log/log-info (str "Deleting document with ID: " id " from table " table-name))
                  deleted (handle-delete storage id)
                  _ (when (and deleted index)
                      (index/delete-document index id))]
              (log/log-info (str "Document " (if deleted "deleted successfully" "deletion failed")))
              (messages/send-command-complete out "DELETE" (if deleted 1 0)))
            (do ; Document found but belongs to a different table
              (log/log-warn (str "Attempt to DELETE document " id " rejected: Document belongs to table " (:_table doc-to-delete) ", not " table-name))
              (messages/send-error-response out (str "Document not found in table " table-name))
              (messages/send-command-complete out "DELETE" 0)))
          (do ; Document ID not found
            (log/log-info (str "Document with ID: " id " not found for deletion."))
            (messages/send-command-complete out "DELETE" 0))) ; Report 0 rows deleted
        (do
          (log/log-warn "Invalid DELETE query - requires table name and simple 'WHERE id = ...' clause.")
          (let [error-msg (cond
                            (not (seq table-name)) "DELETE failed: Table name is missing."
                            (not id) "DELETE requires a 'WHERE id = ...' clause."
                            :else "Invalid DELETE statement.")]
            (log/log-error error-msg)
            (messages/send-error-response out error-msg)
            (messages/send-command-complete out "DELETE" 0)))))
    (catch Exception e
      (let [sw (java.io.StringWriter.)
            pw (java.io.PrintWriter. sw)]
        (.printStackTrace e pw)
        (log/log-error (str "Error processing DELETE: " (.getMessage e) "\n" (.toString sw))))
      (messages/send-error-response out (str "Error processing DELETE: " (.getMessage e)))
      (messages/send-command-complete out "DELETE" 0))))

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
    (case (:type parsed)
      :select (handle-select-case storage out parsed)
      :insert (handle-insert-case storage index out parsed)
      :update (handle-update-case storage index out parsed)
      :delete (handle-delete-case storage index out parsed)
      (do
        (messages/send-error-response out (str "Unknown command: " sql))
        (messages/send-command-complete out "UNKNOWN" 0)))))
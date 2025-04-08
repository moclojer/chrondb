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

;; Funções não utilizadas comentadas para resolver warnings de lint
#_(defn- get-documents-by-id
    "Retrieves a document by ID"
    [storage where-condition & [branch]]
    (let [id (str/replace (get-in where-condition [0 :value]) #"['\"]" "")]
      (if-let [doc (storage/get-document storage id branch)]
        [doc]
        [])))

#_(defn- get-all-documents
    "Retrieves all documents"
    [storage & [branch]]
    (let [docs (storage/get-documents-by-prefix storage "" branch)]
      (or (seq docs) [])))

#_(defn- get-all-documents-for-table
    "Retrieves all documents for a specific table"
    [storage table-name & [branch]]
    (log/log-info (str "Starting document search for table: " table-name))
    (let [documents (storage/get-documents-by-table storage table-name branch)]
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

(defn- normalize-schema-to-branch
  "Converts a SQL schema name to a Git branch name.
   Parameters:
   - schema: The schema name (or nil)
   Returns: The corresponding Git branch name"
  [schema]
  (cond
    (nil? schema) "main"                 ;; Default branch for no schema specified
    (= schema "public") "main"           ;; Map public schema to main branch
    :else schema))                       ;; Otherwise use schema name as branch name

;; Definição da função fts-condition? para verificar se uma condição é de busca full-text
(defn fts-condition?
  "Checks if a WHERE condition is a Full-Text Search (FTS) condition.
   Parameters:
   - condition: A WHERE condition map
   Returns: true if it's an FTS condition, false otherwise"
  [condition]
  (and (map? condition)
       (= (:type condition) :fts-match)))

(defn handle-select
  "Handles a SELECT query, incorporating FTS via the index.
   Parameters:
   - storage: The storage implementation
   - index: The index implementation (optional for FTS)
   - query: The parsed SELECT query map
   Returns: A sequence of matching documents"
  [storage index query]
  (try
    (let [branch-name (normalize-schema-to-branch (:schema query))
          table-name (when (:table query) (str/trim (:table query)))  ;; Trim extra spaces
          std-conditions (remove fts-condition? (:where query)) ;; Handle standard conditions
          fts-conditions (filter fts-condition? (:where query)) ;; Handle full-text search
          order-by (:order-by query)
          group-by (:group-by query)
          limit (:limit query)

          ;; --- Initial Document Retrieval ---
          ;; Retrieve documents: optimization for ID-based lookup vs. full table scan
          initial-docs (if (and (seq fts-conditions) (map? (first fts-conditions)))
                        ;; If using full-text search, handle through the index
                         (let [first-fts (first fts-conditions)
                               _ (log/log-info (str "Using index for FTS search in table '" table-name "'"))
                               fts-field (:field first-fts)
                               ;; Modificar o campo para usar o campo _fts para busca de texto completo
                               search-field (if (str/ends-with? fts-field "_fts")
                                              fts-field
                                              (str fts-field "_fts"))
                               _ (log/log-info (str "Using FTS field: " search-field " (original: " fts-field ")"))
                               fts-value (:value first-fts)
                               _ (log/log-info (str "Raw FTS value: " fts-value))
                               ;; Extract search term from the to_tsquery function and add wildcards
                               clean-value (cond
                                             ;; For to_tsquery('term')
                                             (re-find #"to_tsquery" fts-value)
                                             (let [matches (re-find #"to_tsquery\s*\(\s*['\"]([^'\"]+)['\"]" fts-value)]
                                               (log/log-info (str "Regex matches: " (pr-str matches)))
                                               (if (and matches (> (count matches) 1))
                                                 ;; Normalize term - remove accents, extra spaces and convert to lowercase
                                                 (let [term (-> (second matches)
                                                                (str/trim)
                                                                (str/lower-case)
                                                                (java.text.Normalizer/normalize java.text.Normalizer$Form/NFD)
                                                                (str/replace #"[\p{InCombiningDiacriticalMarks}]" ""))]
                                                   (log/log-info (str "Normalized term without accents: '" term "'"))
                                                   ;; If term is very short, consider it as part of a word
                                                   (if (< (count term) 4)
                                                     (str "*" term "*")
                                                     ;; For normal terms, search for exact term or prefix
                                                     (str term "*")))
                                                 fts-value))

                                             ;; For other cases
                                             :else fts-value)
                               _ (log/log-info (str "Modified search term with wildcards: '" clean-value "'"))
                               doc-ids (if index
                                         (do
                                           (log/log-info (str "Using " (type index) " for fulltext search on field: " search-field ", term: '" clean-value "'"))
                                           (index/search index search-field clean-value branch-name))
                                         [])
                               _ (log/log-info (str "FTS search found " (count doc-ids) " document IDs: " (pr-str doc-ids)))

                               ;; Recuperar os documentos completos a partir dos IDs
                               docs (if (seq doc-ids)
                                      (do
                                        (log/log-info "Retrieving full documents from storage using IDs")
                                        (filter some? (map #(storage/get-document storage % branch-name) doc-ids)))
                                      [])
                               _ (log/log-info (str "Retrieved " (count docs) " full documents from IDs"))]

                           ;; Filter by table if table is specified
                           (if (seq table-name)
                             (do
                               (log/log-info (str "Filtering " (count docs) " documents by table: '" table-name "'"))
                               (let [filtered (filter #(= (:_table %) table-name) docs)]
                                 (log/log-info (str "After table filtering: " (count filtered) " documents"))
                                 filtered))
                             docs))

                        ;; Standard query processing path - check if ID condition exists for optimization
                         (let [first-std (first std-conditions)
                               condition-by-id? (and (= (count std-conditions) 1)
                                                     (map? first-std)
                                                     (= (:field first-std) "id")
                                                     (= (:op first-std) "="))]
                           (if condition-by-id?
                             (let [id (str/replace (:value first-std) #"['\"]" "")]
                               (log/log-info (str "Retrieving document by specific ID: " id))
                               (if-let [doc (storage/get-document storage id branch-name)]
                                 [doc]
                                 []))
                             (do
                               (log/log-info (str "Retrieving all documents for table '" table-name "' in branch '" branch-name "' (pre-filtering)"))
                               (if (seq table-name)
                                 (storage/get-documents-by-table storage table-name branch-name)
                                 (storage/get-documents-by-prefix storage "" branch-name))))))

          _ (log/log-info (str "Initial documents count before standard filtering: " (count initial-docs)))

         ;; Apply standard WHERE filters (if any) to the potentially pre-filtered set
          filtered-docs (if (seq std-conditions)
                          (let [;; If we retrieved by ID already, std-conditions might be redundant
                                ;; but applying again is safer unless explicitly handled.
                                result (operators/apply-where-conditions initial-docs std-conditions)]
                            (log/log-info (str "After standard WHERE filtering: " (count result) " documents, IDs: " (mapv :id result)))
                            result)
                          initial-docs)

         ;; --- Grouping, Processing, Sorting, Limiting (Same as before) ---
          processed-docs (if (seq group-by)
                           (let [grouped-docs (operators/group-docs-by filtered-docs group-by)
                                 _ (log/log-info (str "After grouping: " (count grouped-docs) " groups"))]
                             (process-groups grouped-docs query))
                           (do
                             (log/log-info "No grouping: processing documents directly")
                             (mapv (fn [doc]
                                     (let [columns (:columns query)]
                                       (if (= 1 (count columns))
                                         (let [col (first columns)]
                                           (if (= :all (:type col))
                                             doc
                                             {(keyword (:column col)) (get doc (keyword (:column col)))}))
                                         (reduce (fn [acc col]
                                                   (if (= :all (:type col))
                                                     (merge acc doc)
                                                     (assoc acc (keyword (:column col))
                                                            (get doc (keyword (:column col))))))
                                                 {}
                                                 columns))))
                                   filtered-docs)))

          _ (log/log-info (str "After processing/projection: " (count processed-docs) " documents"))

          sorted-results (operators/sort-docs-by processed-docs order-by)
          _ (log/log-info (str "After sorting: " (count sorted-results) " documents"))

          limited-results (operators/apply-limit sorted-results limit)
          _ (log/log-info (str "After applying limit: " (count limited-results) " documents"))
          _ (log/log-info (str "Final documents returned: " (mapv :id limited-results)))]

      (or limited-results []))
    (catch Exception e
      (let [sw (java.io.StringWriter.)
            pw (java.io.PrintWriter. sw)]
        (.printStackTrace e pw)
        (log/log-error (str "Error in handle-select: " (.getMessage e) "\n" (.toString sw))))
      [])))

(defn handle-insert
  "Handles a document insert operation"
  [storage doc & [branch]]
  (storage/save-document storage doc branch))

(defn handle-update
  "Handles an UPDATE query.
   Parameters:
   - storage: The storage implementation
   - id: The ID of the document to update
   - updates: Map of fields to update
   - branch: Optional branch name to update on
   Returns: The updated document"
  [storage id updates & [branch]]
  (log/log-info (str "Updating document: " id " with changes: " updates))
  (if-let [doc (storage/get-document storage id branch)]
    (let [updated-doc (merge doc updates)
          result (storage/save-document storage updated-doc branch)]
      (log/log-info (str "Document updated successfully: " result))
      result)
    (do
      (log/log-warn (str "Document not found: " id))
      nil)))

(defn handle-delete
  "Handles a document delete operation"
  [storage id & [branch]]
  (storage/delete-document storage id branch))

(defn handle-select-case
  "Handles the SELECT case of an SQL query"
  [storage index out parsed]
  (log/log-info (str "Starting handle-select-case with parsed: " parsed))
  (let [results (handle-select storage index parsed)
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
                            results
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
                             (mapv #(if (keyword? %) (name %) (str %))))
                        (catch Exception e
                          (log/log-error (str "Erro ao extrair nomes de colunas: " (.getMessage e)))
                          (when-let [first-res (first results)] ; Tenta pegar chaves do primeiro resultado se houver
                            (mapv name (keys first-res)))
                          []))] ; Fallback para vazio se não houver resultados ou erro
          (log/log-info (str "Sending column description: " columns))
          (messages/send-row-description out columns)
          (doseq [row results]
            (log/log-info (str "Sending row: " (:id row "N/A")))
            ;; Map values according to the filtered columns
            (let [values (map #(let [val (get row (if (string? %) (keyword %) %))]
                                 (if (nil? val) "" (str val))) ; Trata nil como string vazia
                              columns)] ; Use the filtered columns here
              (log/log-info (str "Values sent: " values))
              (messages/send-data-row out values)))
          (messages/send-command-complete out "SELECT" (count results)))))

    (log/log-info "SELECT query processing completed")))

(defn handle-insert-case
  "Handles the INSERT case of an SQL query"
  [storage index out parsed]
  (try
    (log/log-info (str "Processing INSERT in table: " (:table parsed)
                       (when (:schema parsed) (str " in schema: " (:schema parsed)))))

    (let [values (:values parsed)
          columns (:columns parsed)
          table-name (:table parsed)
          schema (:schema parsed)
          branch-name (normalize-schema-to-branch schema)

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
          clean-values (when (seq values)
                         (map (fn [val]
                                ;; Only remove quotes from strings, leave other values as-is
                                (if (and (string? val)
                                         (or (str/starts-with? val "'")
                                             (str/starts-with? val "\"")))
                                  (str/replace val #"^['\"]|['\"]$" "")
                                  val))
                              values))

          ;; Make sure columns and values have the same count
          _ (when (and (seq clean-columns) (seq clean-values) (not= (count clean-columns) (count clean-values)))
              (throw (Exception. "Column count doesn't match value count")))

          ;; Create a document map from columns and values
          doc-data (if (and (seq clean-columns) (seq clean-values))
                     (zipmap (map keyword clean-columns) clean-values)
                     {})

          ;; Check if ID is provided, otherwise generate one
          id-provided (if-let [id (:id doc-data)]
                        ;; Use provided ID as is, without adding table prefix
                        id
                        ;; Generate a UUID if no ID provided, without table prefix
                        (str (java.util.UUID/randomUUID)))

          ;; Create final document with table information
          doc (merge
               doc-data
               {:id id-provided
                :_table table-name})

          _ (log/log-info (str "Document to insert: " doc))
          saved (storage/save-document storage doc branch-name)]

      (when index
        (log/log-info (str "Indexing document in Lucene: " (:id saved) ", index type: " (type index)))
        (try
          (index/index-document index saved)
          (log/log-info "Document indexed successfully")
          (catch Exception e
            (log/log-error (str "Error indexing document: " (.getMessage e))))))

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
    (log/log-info (str "Processing UPDATE for table: " (:table parsed)
                       (when (:schema parsed) (str " in schema: " (:schema parsed)))))

    (let [table-name (:table parsed)
          schema (:schema parsed)
          branch-name (normalize-schema-to-branch schema)
          updates (:updates parsed)
          where-condition (:where parsed)

      ;; Get documents with branch parameter
          table-docs (storage/get-documents-by-table storage table-name branch-name)
          matching-docs (operators/apply-where-conditions table-docs where-condition)
          update-count (atom 0)]

      (if (seq matching-docs)
        (do
          (log/log-info (str "Found " (count matching-docs) " documents to update"))
          (doseq [doc matching-docs]
            (let [updated-doc (merge doc updates)
                  saved (storage/save-document storage updated-doc branch-name)]

              (when (and saved index)
                (index/index-document index saved))

              (swap! update-count inc)))

          (log/log-info (str "Updated " @update-count " documents successfully"))
          (messages/send-command-complete out "UPDATE" @update-count))

        (do
          (log/log-warn "No documents found matching WHERE conditions")
          (messages/send-error-response out "No documents found matching WHERE conditions")
          (messages/send-command-complete out "UPDATE" 0))))
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
    (log/log-info (str "Processing DELETE from table: " (:table parsed)
                       (when (:schema parsed) (str " in schema: " (:schema parsed)))))

    (let [table-name (:table parsed)
          schema (:schema parsed)
          branch-name (normalize-schema-to-branch schema)
          where-condition (:where parsed)

      ;; For ID-based deletion
          id (when (and (seq where-condition)
                        (= (count where-condition) 1)
                        (= (get-in where-condition [0 :field]) "id")
                        (= (get-in where-condition [0 :op]) "="))
               (str/replace (get-in where-condition [0 :value]) #"[\'\\\"]" ""))]

      (if (and id (seq table-name))
        (if-let [doc-to-delete (storage/get-document storage id branch-name)]
          (if (= (:_table doc-to-delete) table-name)
            (let [_ (log/log-info (str "Deleting document with ID: " id " from table " table-name))
                  deleted (storage/delete-document storage id branch-name)
                  _ (when (and deleted index)
                      (index/delete-document index id))]
              (if deleted
                (do
                  (log/log-info "Document deleted successfully")
                  (messages/send-command-complete out "DELETE" 1))
                (do
                  (log/log-error "Failed to delete document")
                  (messages/send-error-response out "Failed to delete document")
                  (messages/send-command-complete out "DELETE" 0))))
            (do
              (log/log-error (str "Document ID " id " does not belong to table " table-name))
              (messages/send-error-response out (str "Document ID " id " does not belong to table " table-name))
              (messages/send-command-complete out "DELETE" 0)))
          (do
            (log/log-error (str "Document with ID " id " not found"))
            (messages/send-error-response out (str "Document with ID " id " not found"))
            (messages/send-command-complete out "DELETE" 0)))

        (let [error-msg (cond
                          (not (seq table-name)) "DELETE failed: Table name is missing."
                          (not id) "DELETE requires a 'WHERE id = ...' clause."
                          :else "Invalid DELETE statement.")]
          (log/log-error error-msg)
          (messages/send-error-response out error-msg)
          (messages/send-command-complete out "DELETE" 0))))
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
  (try
    (let [parsed (statements/parse-sql-query sql)
          query-type (:type parsed)]
      (log/log-info (str "Parsed query type: " query-type ", Details: " parsed))
      (case query-type
        :select (handle-select-case storage index out parsed)
        :insert (handle-insert-case storage index out parsed)
        :update (handle-update-case storage index out parsed)
        :delete (handle-delete-case storage index out parsed)
        ;; Default case for unknown or invalid query types
        (do (log/log-error (str "Unknown or invalid query type: " query-type " for SQL: " sql))
            (messages/send-error-response out (str "Unknown or invalid command: " sql))
            (messages/send-command-complete out "UNKNOWN" 0))))
    (catch Exception e
      (let [sw (java.io.StringWriter.)
            pw (java.io.PrintWriter. sw)]
        (.printStackTrace e pw)
        (log/log-error (str "Error handling query: " sql " - Error: " (.getMessage e) "\n" (.toString sw))))
      (messages/send-error-response out (str "Error processing query: " (.getMessage e))))))
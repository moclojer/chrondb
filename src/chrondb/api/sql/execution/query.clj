(ns chrondb.api.sql.execution.query
  "SQL query execution"
  (:require [clojure.string :as str]
            [chrondb.util.logging :as log]
            [chrondb.api.sql.parser.statements :as statements]
            [chrondb.api.sql.protocol.messages :as messages]
            [chrondb.api.sql.execution.operators :as operators]
            [chrondb.api.sql.execution.functions :as functions]
            [chrondb.api.sql.execution.join :as join]
            [chrondb.storage.protocol :as storage]
            [chrondb.index.protocol :as index]))

(declare handle-chrondb-function)

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

(defn- fts-get-matching-docs
  "Get documents that match a full-text search condition.
   Parameters:
   - index: The index implementation
   - storage: The storage implementation
   - fts-condition: The FTS condition map
   - branch-name: The branch to search in
   Returns: A sequence of matching documents"
  [index storage fts-condition branch-name]
  (try
    (let [fts-field (:field fts-condition)
          ;; Modify field to use _fts suffix for full-text search
          search-field (if (str/ends-with? fts-field "_fts")
                         fts-field
                         (str fts-field "_fts"))
          _ (log/log-info (str "Using FTS field: " search-field " (original: " fts-field ")"))
          fts-value (:value fts-condition)
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

          ;; Search for document IDs using the index
          doc-ids (index/search index search-field clean-value branch-name)
          _ (log/log-info (str "FTS search found " (count doc-ids) " document IDs"))

          ;; Retrieve full documents from storage
          docs (filter some? (map #(storage/get-document storage % branch-name) doc-ids))
          _ (log/log-info (str "Retrieved " (count docs) " full documents from IDs"))]

      docs)
    (catch Exception e
      (log/log-error (str "Error in FTS matching: " (.getMessage e)))
      [])))

(defn handle-chrondb-function
  "Handles ChronDB specific functions.
   Parameters:
   - storage: The storage implementation
   - index: The index implementation (not used in history functions)
   - out: Output stream to write results to
   - parsed: The parsed query details
   Returns: None, writes directly to the output stream"
  [storage _index out parsed]
  (log/log-info (str "Handling ChronDB function: " (:function parsed)))

  (try
    (let [function (:function parsed)
          table (:table parsed)
          id (:id parsed)]

      (case function
        :history
        (let [history (functions/execute-chrondb-history storage table id)
              columns ["commit_id" "timestamp" "committer" "data"]]
          (log/log-info (str "Got history with " (count history) " entries"))
          (messages/send-row-description out columns)
          (doseq [entry history]
            (messages/send-data-row out [(or (:commit_id entry) "")
                                         (or (:timestamp entry) "")
                                         (or (:committer entry) "")
                                         (or (:data entry) "")]))
          (messages/send-command-complete out "SELECT" (count history)))

        :at
        (let [commit (:commit parsed)
              document (functions/execute-chrondb-at storage table id commit)]
          (if document
            (let [; Exclude internal fields
                  columns (remove #(or (= % :_table) (str/starts-with? (name %) "_")) (keys document))]
              (messages/send-row-description out (map name columns))
              (messages/send-data-row out (map #(or (str (get document %)) "") columns))
              (messages/send-command-complete out "SELECT" 1))
            (do
              (messages/send-row-description out [])
              (messages/send-command-complete out "SELECT" 0))))

        :diff
        (let [commit1 (:commit1 parsed)
              commit2 (:commit2 parsed)
              diff-result (functions/execute-chrondb-diff storage table id commit1 commit2)
              columns ["id" "commit1" "commit2" "added" "removed" "changed"]]
          (messages/send-row-description out columns)
          (doseq [diff diff-result]
            (messages/send-data-row out [(or (:id diff) "")
                                         (or (:commit1 diff) "")
                                         (or (:commit2 diff) "")
                                         (or (:added diff) "{}")
                                         (or (:removed diff) "{}")
                                         (or (:changed diff) "{}")]))
          (messages/send-command-complete out "SELECT" (count diff-result)))

        ;; Default case for unknown functions
        (do
          (log/log-warn (str "Unknown ChronDB function: " function))
          (messages/send-error-response out (str "Unknown ChronDB function: " function)))))

    (catch Exception e
      (let [sw (java.io.StringWriter.)
            pw (java.io.PrintWriter. sw)]
        (.printStackTrace e pw)
        (log/log-error (str "Error in handle-chrondb-function: " (.getMessage e) "\n" (.toString sw)))
        (messages/send-error-response out (str "Error executing function: " (.getMessage e)))))

    (finally
      ;; Always send ready for query
      (messages/send-ready-for-query out \I))))

(defn process-columns
  "Process columns for a query, handling column projection and aliases"
  [docs columns]
  (if (and (seq columns) (not (some #(= :all (:type %)) columns)))
    ;; Column projection needed
    (mapv (fn [doc]
            (into {}
                  (keep (fn [col-def]
                          (when (and (= :column (:type col-def))
                                     (not= "," (:column col-def)))
                            (let [col-name (keyword (:column col-def))]
                              [col-name (get doc col-name)])))
                        columns)))
          docs)
    ;; No column projection, return original docs
    docs))

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
          join-info (:join query)
          columns (:columns query) ;; Get the requested columns

          ;; Verificar se é uma consulta direta por ID
          id-condition (when (and (seq std-conditions)
                                  (= (:field (first std-conditions)) "id")
                                  (= (:op (first std-conditions)) "="))
                         (first std-conditions))

          id-value (when id-condition
                     (str/replace (:value id-condition) #"['\"]" ""))

          ;; --- Get initial documents or perform JOIN ---
          initial-docs (if join-info
                         ;; Handle JOIN case - use join/perform-join
                         (do
                           (log/log-info (str "Performing JOIN between " table-name " and " (:table join-info)))
                           (join/perform-join storage table-name join-info))
                         ;; Normal document retrieval
                         (cond
                           ;; Case 1: Single ID query - efficient direct lookup
                           id-value
                           (do
                             (log/log-info (str "Performing direct ID lookup for: " id-value))
                             (let [doc (storage/get-document storage id-value branch-name)]
                               (if doc
                                 ;; Verify table match if both table and ID specified
                                 (if (and table-name (not= table-name "")
                                          (not= (:_table doc) table-name)
                                          (not (.startsWith id-value (str table-name ":"))))
                                   []  ;; Table mismatch
                                   [doc])

                                 ;; Try with table prefix if not found and table specified
                                 (if (and table-name (not= table-name "")
                                          (not (.startsWith id-value (str table-name ":"))))
                                   (let [prefixed-id (str table-name ":" id-value)
                                         doc (storage/get-document storage prefixed-id branch-name)]
                                     (if doc [doc] []))
                                   []))))

                           ;; Case 2: Table specified - get documents from that table
                           (and table-name (not= table-name ""))
                           (do
                             (log/log-info (str "Loading documents from table: " table-name))
                             (let [table-docs (storage/get-documents-by-table storage table-name branch-name)]
                               (log/log-info (str "Loaded documents from " table-name ": " (count table-docs)
                                                  " documents with IDs: " (pr-str (map :id table-docs))))

                               ;; Se a tabela for "doc" e não houver documentos, tente buscar todos
                               (if (and (= table-name "doc") (empty? table-docs))
                                 (let [all-docs (storage/get-documents-by-prefix storage "" branch-name)]
                                   (log/log-info (str "No documents found in 'doc' table, loading all: "
                                                      (count all-docs) " documents"))
                                   all-docs)
                                 table-docs)))

                           ;; Case 3: No table specified - get all documents
                              :else
                              (do
                             (log/log-info "No table specified, loading all documents")
                             (storage/get-documents-by-prefix storage "" branch-name))))

          ;; --- Apply FTS conditions if any and we have an index ---
          docs-with-fts (if (and index (seq fts-conditions))
                          (let [fts-docs (mapcat #(fts-get-matching-docs index storage % branch-name) fts-conditions)
                                fts-doc-ids (set (map :id fts-docs))]
                            (log/log-info (str "FTS search returned " (count fts-docs) " documents"))
                            ;; Combine FTS results with standard docs - Intersection
                            (filter #(contains? fts-doc-ids (:id %)) initial-docs))
                          initial-docs)

          ;; --- Apply WHERE conditions ---
          _ (log/log-info (str "Applying WHERE conditions to " (count docs-with-fts) " documents"))
          docs-filtered (operators/apply-where-conditions docs-with-fts std-conditions)
          _ (log/log-info (str "After WHERE filtering: " (count docs-filtered) " documents"))

          ;; --- Apply GROUP BY if specified ---
          grouped-docs (operators/group-docs-by docs-filtered group-by)

          ;; --- Process aggregate functions or column projections ---
          processed-docs (if group-by
                           (process-groups grouped-docs query)
                           ;; Do column projection if needed (but only for non-JOIN cases)
                           (if join-info
                             ;; For JOIN queries, don't do additional column projection here
                             ;; since we've already included column prefixes in the join results
                             docs-filtered
                             (process-columns docs-filtered columns)))

          ;; Single flat result list
          flat-results (if (and (seq processed-docs) (vector? (first processed-docs)))
                         (first processed-docs)
                         processed-docs)

          ;; --- Sort results if ORDER BY is specified ---
          sorted-docs (operators/sort-docs-by flat-results order-by)

          ;; --- Apply LIMIT if specified ---
          limited-docs (operators/apply-limit sorted-docs limit)]

      ;; For debugging
      (log/log-info (str "FINAL RESULT COUNT: " (count limited-docs)))
      (log/log-info (str "After processing/projection: " (count limited-docs) " documents"))

      limited-docs)
    (catch Exception e
      (log/log-error (str "Error in handle-select: " (.getMessage e)))
      (.printStackTrace e)
      [])))

(defn handle-insert
  "Handles a document insert operation"
  [storage doc & [branch]]
  (let [doc-with-table (if (:_table doc)
                         doc
                         (assoc doc :_table (first (str/split (:id doc) #":"))))]
    (storage/save-document storage doc-with-table branch)))

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
  "Handles an SQL SELECT query execution.
   Parameters:
   - storage: The storage implementation
   - index: The index implementation (can be nil)
   - out: The output stream to write results to
   - parsed: The parsed query details
   Returns: nil"
  [storage index ^java.io.OutputStream out parsed]
  (log/log-info (str "Starting handle-select-case with parsed: " parsed))
  (try
    (let [table-name (:table parsed)
          where-conditions (:where parsed)
          limit (:limit parsed)
          group-by (:group-by parsed)
          columns (:columns parsed)
          order-by (:order-by parsed)
          join (:join parsed)
          branch-name (normalize-schema-to-branch (:schema parsed))

          ;; Logging adicional para diagnóstico
          _ (log/log-info (str "Loading documents from table: " table-name))

          ;; Get documents from storage
          docs (if join
                 (join/perform-join storage table-name join)
                 (let [docs (storage/get-documents-by-table storage table-name branch-name)]
                     (log/log-info (str "Loaded documents from " table-name ": " (count docs)
                                        " documents with IDs: " (pr-str (map :id docs))))
                   docs))

          _ (when join
              (log/log-info (str "Join produced " (count docs) " documents")))

          ;; Apply WHERE conditions
          _ (log/log-info (str "Applying WHERE conditions: " (pr-str where-conditions)))
          filtered-docs (operators/apply-where-conditions docs where-conditions)
          _ (log/log-info (str "After WHERE filtering: " (count filtered-docs) " documents"))

          ;; --- Apply GROUP BY if specified ---
          grouped-docs (operators/group-docs-by filtered-docs group-by)
          _ (log/log-info (str "After grouping: " (count grouped-docs) " groups"))

          ;; --- Process aggregate functions or column projections ---
          processed-docs (cond
                           ;; Group by with aggregate functions
                           (seq group-by)
                           (do
                             (log/log-info "Processing groups with columns")
                             (process-groups grouped-docs parsed))

                           ;; For JOINs, we need special column handling to process all prefixed columns
                           join
                           (do
                             (log/log-info "Processing JOIN results with column projection")
                             (if (some #(= :all (:type %)) columns)
                               ;; For SELECT *, return all columns
                               filtered-docs
                               ;; For specific columns in JOIN, project only requested columns
                               (mapv (fn [doc]
                                       (into {}
                                             (keep (fn [col]
                                                     (when (= :column (:type col))
                                                       (let [col-name (keyword (:column col))]
                                                         [col-name (get doc col-name)])))
                                                   columns)))
                                     filtered-docs)))

                           ;; No grouping - just process columns
                           :else
                           (do
                             (log/log-info "No grouping: processing documents directly")
                             (let [result (mapv (fn [doc]
                                                  (let [columns (:columns parsed)]
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
                                                filtered-docs)]
                               (log/log-info (str "FINAL RESULT COUNT: " (count result)))
                               result)))

          _ (log/log-info (str "After processing/projection: " (count processed-docs) " documents"))

          sorted-results (operators/sort-docs-by processed-docs order-by)
          _ (log/log-info (str "After sorting: " (count sorted-results) " documents"))

          limited-results (operators/apply-limit sorted-results limit)
          _ (log/log-info (str "After applying limit: " (count limited-results) " documents"))]

      (if (empty? limited-results)
        ;; For empty results - special format
        (do
          (log/log-info "Sending empty row description for 0 results")
          (messages/send-row-description out [])
          (messages/send-command-complete out "SELECT" 0))

        ;; For results with data
        (cond
          ;; For COUNT(*) queries
          (and (= 1 (count columns))
               (= :aggregate-function (:type (first columns)))
               (= :count (:function (first columns)))
               (= "*" (first (:args (first columns)))))
          (do
            (log/log-info "Processing count(*) query")
            (messages/send-row-description out ["count"])
            (messages/send-data-row out [(str (count limited-results))])
            (messages/send-command-complete out "SELECT" 1))

          ;; For other aggregate functions (sum, avg, min, max)
          (and (= 1 (count columns))
               (= :aggregate-function (:type (first columns))))
          (do
            (log/log-info "Processing aggregate function query")
            (let [agg-function (:function (first columns))
                  agg-field (first (:args (first columns)))
                  column-name (name agg-function)
                  _ (log/log-info (str "Aggregate function: " agg-function ", Field: " agg-field))
                  agg-result (functions/execute-aggregate-function
                              agg-function
                              limited-results
                              agg-field)
                  _ (log/log-info (str "Aggregate result: " agg-result))]
              (messages/send-row-description out [column-name])
              (messages/send-data-row out [agg-result])
              (messages/send-command-complete out "SELECT" 1)))

          ;; For regular SELECT queries (non-aggregate)
          :else
          (let [;; Get columns to display - either requested ones or all from the results
                columns (try
                          (if (some #(= :all (:type %)) columns)
                            ;; If * was used, return all columns
                            (->> limited-results
                                              (mapcat keys)
                                              (filter #(not= % :_table)) ; Exclude :_table
                                              (distinct)
                                              (sort)
                                 (mapv name))
                              ;; Otherwise, filter to only requested columns
                            (mapv (fn [col]
                                    (when (= :column (:type col))
                                      (:column col)))
                                  (remove #(nil? (:column %)) columns)))
                          (catch Exception e
                            (log/log-error (str "Error extracting column names: " (.getMessage e)))
                            (when-let [first-res (first limited-results)] ; Try to get keys from first result if any
                              (mapv name (keys first-res)))
                            []))] ; Empty fallback if no results or error
            (log/log-info (str "Sending column description: " columns))
            (messages/send-row-description out columns)
            (doseq [row limited-results]
              (when row  ;; Guard against nil rows
              (log/log-info (str "Sending row: " (:id row "N/A")))
              ;; Map values according to the filtered columns
              (let [values (mapv #(str (get row (keyword %) ""))
                                 columns)] ; Use the filtered columns here
                  (messages/send-data-row out values))))
            (messages/send-command-complete out "SELECT" (count limited-results))))))

    (catch Exception e
      (let [sw (java.io.StringWriter.)
            pw (java.io.PrintWriter. sw)]
        (.printStackTrace e pw)
        (log/log-error (str "Error in handle-select-case: " (.getMessage e) "\n" (.toString sw))))
      (messages/send-error-response out (str "Error processing query: " (.getMessage e)))
      (messages/send-ready-for-query out \E)))

  ;; Add ReadyForQuery message at the end of all SELECT operations
  (messages/send-ready-for-query out \I)
  (log/log-info "SELECT query processing completed"))

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
                                                     (str/replace #"[^\w\d_]" "_"))]
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

      (messages/send-command-complete out "INSERT" 1)

      ;; Send ReadyForQuery to finalize the response
      (messages/send-ready-for-query out \I)

      ;; Return the saved document to support the test cases
      saved)

    (catch Exception e
      (let [sw (java.io.StringWriter.)
            pw (java.io.PrintWriter. sw)]
        (.printStackTrace e pw)
        (log/log-error (str "Error processing INSERT: " (.getMessage e) "\n" (.toString sw))))
      (messages/send-error-response out (str "Error processing INSERT: " (.getMessage e)))
      (messages/send-command-complete out "INSERT" 0)

      ;; Send ReadyForQuery in error case
      (messages/send-ready-for-query out \E)
      nil)))

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
          (messages/send-command-complete out "UPDATE" @update-count)
          (messages/send-ready-for-query out \I))

        (do
          (log/log-warn "No documents found matching WHERE conditions")
          (messages/send-error-response out "No documents found matching WHERE conditions")
          (messages/send-command-complete out "UPDATE" 0)
          (messages/send-ready-for-query out \I))))
    (catch Exception e
      (let [sw (java.io.StringWriter.)
            pw (java.io.PrintWriter. sw)]
        (.printStackTrace e pw)
        (log/log-error (str "Error processing UPDATE: " (.getMessage e) "\n" (.toString sw))))
      (messages/send-error-response out (str "Error processing UPDATE: " (.getMessage e)))
      (messages/send-command-complete out "UPDATE" 0)
      (messages/send-ready-for-query out \E))))

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
                  (messages/send-command-complete out "DELETE" 1)
                  (messages/send-ready-for-query out \I))
                (do
                  (log/log-error "Failed to delete document")
                  (messages/send-error-response out "Failed to delete document")
                  (messages/send-command-complete out "DELETE" 0)
                  (messages/send-ready-for-query out \E))))
            (do
              (log/log-error (str "Document ID " id " does not belong to table " table-name))
              (messages/send-error-response out (str "Document ID " id " does not belong to table " table-name))
              (messages/send-command-complete out "DELETE" 0)
              (messages/send-ready-for-query out \E)))
          (do
            (log/log-error (str "Document with ID " id " not found"))
            (messages/send-error-response out (str "Document with ID " id " not found"))
            (messages/send-command-complete out "DELETE" 0)
            (messages/send-ready-for-query out \E)))

        (let [error-msg (cond
                          (not (seq table-name)) "DELETE failed: Table name is missing."
                          (not id) "DELETE requires a 'WHERE id = ...' clause."
                          :else "Invalid DELETE statement.")]
          (log/log-error error-msg)
          (messages/send-error-response out error-msg)
          (messages/send-command-complete out "DELETE" 0)
          (messages/send-ready-for-query out \E))))
    (catch Exception e
      (let [sw (java.io.StringWriter.)
            pw (java.io.PrintWriter. sw)]
        (.printStackTrace e pw)
        (log/log-error (str "Error processing DELETE: " (.getMessage e) "\n" (.toString sw))))
      (messages/send-error-response out (str "Error processing DELETE: " (.getMessage e)))
      (messages/send-command-complete out "DELETE" 0)
      (messages/send-ready-for-query out \E))))

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
      (cond
        (= query-type :chrondb-function)
        (handle-chrondb-function storage index out parsed)

        (= query-type :select)
        (handle-select-case storage index out parsed)

        (= query-type :insert)
        (handle-insert-case storage index out parsed)

        (= query-type :update)
        (handle-update-case storage index out parsed)

        (= query-type :delete)
        (handle-delete-case storage index out parsed)

        :else
        (do (log/log-error (str "Unknown or invalid query type: " query-type " for SQL: " sql))
            (messages/send-error-response out (str "Unknown or invalid command: " sql))
            (messages/send-command-complete out "UNKNOWN" 0)
            (messages/send-ready-for-query out \I))))
    (catch Exception e
      (let [sw (java.io.StringWriter.)
            pw (java.io.PrintWriter. sw)]
        (.printStackTrace e pw)
        (log/log-error (str "Error handling query: " sql " - Error: " (.getMessage e) "\n" (.toString sw))))
      (messages/send-error-response out (str "Error processing query: " (.getMessage e)))
      (messages/send-ready-for-query out \E))))
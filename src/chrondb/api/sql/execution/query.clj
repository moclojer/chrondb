(ns chrondb.api.sql.execution.query
  "SQL query execution"
  (:require [clojure.string :as str]
            [chrondb.util.logging :as log]
            [chrondb.api.sql.parser.statements :as statements]
            [chrondb.api.sql.protocol.messages :as messages]
            [chrondb.api.sql.execution.operators :as operators]
            [chrondb.api.sql.execution.functions :as functions]
            [chrondb.api.sql.execution.join :as join]
            [chrondb.api.sql.execution.ast-converter :as ast-converter]
            [chrondb.api.sql.execution.ddl :as ddl]
            [chrondb.query.ast :as ast]
            [chrondb.storage.protocol :as storage]
            [chrondb.index.protocol :as index]
            [chrondb.transaction.core :as tx]))

(declare handle-chrondb-function)

(defn- sql-normalize-flags [flags]
  (letfn [(spread [v]
            (cond
              (nil? v) []
              (and (coll? v) (not (string? v))) (mapcat spread v)
              :else [v]))]
    (->> (spread flags)
         (keep identity)
         (map str)
         (remove str/blank?)
         distinct
         vec
         not-empty)))

(defn- sql-tx-options [{:keys [operation id branch table flags metadata]}]
  (let [base-meta (cond-> {:operation operation}
                    id (assoc :document-id id)
                    table (assoc :table table)
                    branch (assoc :branch branch))
        metadata (merge base-meta (or metadata {}))
        normalized (sql-normalize-flags flags)]
    (cond-> {:origin "sql"
             :metadata metadata}
      normalized (assoc :flags normalized))))

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

#_(defn- fts-get-matching-docs
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
  "Handles a SELECT query, using Lucene AST for efficient filtering when index is available.
   Parameters:
   - storage: The storage implementation
   - index: The index implementation (optional, but required for Lucene filtering)
   - query: The parsed SELECT query map
   Returns: A sequence of matching documents"
  [storage index query]
  (try
    (let [branch-name (normalize-schema-to-branch (:schema query))
          table-name (when (:table query) (str/trim (:table query)))  ;; Trim extra spaces
          where-conditions (:where query)
          order-by (:order-by query)
          group-by (:group-by query)
          limit (:limit query)
          join-info (:join query)
          columns (:columns query) ;; Get the requested columns

          ;; Verificar se é uma consulta direta por ID
          std-conditions (remove fts-condition? where-conditions)
          id-condition (when (and (seq std-conditions)
                                  (= (:field (first std-conditions)) "id")
                                  (= (:op (first std-conditions)) "="))
                         (first std-conditions))

          id-value (when id-condition
                     (str/replace (:value id-condition) #"['\"]" ""))

          ;; --- Use Lucene AST if index is available and we have WHERE conditions ---
          ;; Don't use Lucene for direct ID queries - they're more efficient with direct lookup
          use-lucene? (and index (seq where-conditions) (not join-info) (not id-value))

          ;; --- Build AST query if using Lucene ---
          ast-clauses (when use-lucene?
                        (let [all-clauses (keep (fn [condition]
                                                  (cond
                                                    ;; FTS conditions
                                                    (= (:type condition) :fts-match)
                                                    (ast/fts (:field condition) (:query condition))

                                                    ;; Standard conditions
                                                    (= (:type condition) :standard)
                                                    (ast-converter/condition->ast-clause condition)))
                                                where-conditions)]
                          (when (seq all-clauses)
                            (if (= (count all-clauses) 1)
                              (first all-clauses)
                              (apply ast/and all-clauses)))))

          ;; Add table filter to AST if table specified
          ast-clauses-with-table (if (and ast-clauses table-name (not= table-name ""))
                                   (ast/and ast-clauses (ast/term :_table table-name))
                                   ast-clauses)

          ;; Build sort descriptors from ORDER BY
          sort-descriptors (when (seq order-by)
                             (mapv (fn [o]
                                     (ast/sort-by (:column o) (:direction o)))
                                   order-by))

          ;; Execute AST query via Lucene if available
          lucene-result (when (and use-lucene? ast-clauses-with-table)
                          (try
                            (let [ast-query (ast/query [ast-clauses-with-table]
                                                       {:sort sort-descriptors
                                                        :limit limit
                                                        :offset 0
                                                        :branch branch-name})
                                  opts (cond-> {}
                                         sort-descriptors (assoc :sort sort-descriptors)
                                         limit (assoc :limit limit))]
                              (log/log-info (str "Executing AST query via Lucene: " (pr-str ast-query)))
                              (index/search-query index ast-query branch-name opts))
                            (catch Exception e
                              (log/log-error (str "Error executing AST query: " (.getMessage e)))
                              nil)))

          ;; --- Get documents: use Lucene results or fallback to storage ---
          initial-docs (if (and use-lucene? lucene-result)
                         ;; Use Lucene results
                         (let [doc-ids (:ids lucene-result)
                               ;; Deduplicate IDs before fetching documents
                               unique-doc-ids (distinct doc-ids)
                               docs (filter some? (map #(storage/get-document storage % branch-name) unique-doc-ids))]
                           (log/log-info (str "Lucene returned " (count doc-ids) " document IDs, " (count unique-doc-ids) " unique, fetched " (count docs) " documents"))
                           ;; Deduplicate by ID to ensure no duplicates
                           (->> docs
                                (clojure.core/group-by :id)
                                (vals)
                                (map first)))
                         ;; Fallback: JOIN or direct storage lookup
                         (if join-info
                           ;; Handle JOIN case - use join/perform-join
                           (do
                             (log/log-info (str "Performing JOIN between " table-name " and " (:table join-info)))
                             (join/perform-join storage table-name join-info))
                           ;; Normal document retrieval
                           (cond
                             ;; Case 1: Single ID query - efficient direct lookup with table-aware search
                             id-value
                             (do
                               (log/log-info (str "Performing direct ID lookup for: " id-value " in table: " (or table-name "any")))
                               (let [;; Try direct lookup first (ID might already have table prefix like "user:1")
                                     doc (storage/get-document storage id-value branch-name)

                                     ;; If not found and table specified, try with table prefix (for IDs like "1" when table is "user")
                                     doc (or doc
                                             (when (and table-name (not= table-name "")
                                                        (not (.startsWith id-value (str table-name ":"))))
                                               (storage/get-document storage (str table-name ":" id-value) branch-name)))

                                     ;; If still not found and table specified, try getting from table
                                     doc (or doc
                                             (when (and table-name (not= table-name "") (nil? doc))
                                               (let [table-docs (storage/get-documents-by-table storage table-name branch-name)]
                                                 (first (filter #(= (:id %) id-value) table-docs)))))

                                     ;; Verify table match: if document found and table specified, check if they match
                                     doc (if (and doc table-name (not= table-name ""))
                                           (if (= (:_table doc) table-name)
                                             doc
                                             ;; If table doesn't match but ID already starts with table prefix, still return it
                                             (if (.startsWith id-value (str table-name ":"))
                                               doc
                                               nil))
                                           doc)]

                                 (if doc [doc] [])))

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
                               (storage/get-documents-by-prefix storage "" branch-name)))))

          ;; --- Apply WHERE conditions if not using Lucene (fallback) ---
          docs-filtered (if use-lucene?
                          ;; Already filtered by Lucene
                          initial-docs
                          ;; Fallback: filter in memory
                          (do
                            (log/log-info (str "Applying WHERE conditions in memory to " (count initial-docs) " documents"))
                            (operators/apply-where-conditions initial-docs where-conditions)))

          ;; --- Apply GROUP BY if specified ---
          grouped-docs (operators/group-docs-by docs-filtered (or group-by []))

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

          ;; --- Sort results if ORDER BY is specified and not already sorted by Lucene ---
          sorted-docs (if (and use-lucene? sort-descriptors)
                        ;; Already sorted by Lucene
                        flat-results
                        ;; Fallback: sort in memory
                        (operators/sort-docs-by flat-results order-by))

          ;; --- Apply LIMIT if specified and not already applied by Lucene ---
          limited-docs (if (and use-lucene? limit)
                         ;; Already limited by Lucene
                         sorted-docs
                         ;; Fallback: limit in memory
                         (operators/apply-limit sorted-docs limit))]

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
                         (assoc doc :_table (first (str/split (:id doc) #":"))))
        tx-opts (sql-tx-options {:operation "insert"
                                 :id (:id doc-with-table)
                                 :table (:_table doc-with-table)
                                 :branch branch})]
    (tx/with-transaction [storage tx-opts]
      (storage/save-document storage doc-with-table branch))))

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
          tx-opts (sql-tx-options {:operation "update"
                                   :id id
                                   :table (:_table updated-doc)
                                   :branch branch})
          result (tx/with-transaction [storage tx-opts]
                   (storage/save-document storage updated-doc branch))]
      (log/log-info (str "Document updated successfully: " result))
      result)
    (do
      (log/log-warn (str "Document not found: " id))
      nil)))

(defn handle-delete
  "Handles a document delete operation"
  [storage id & [branch]]
  (tx/with-transaction [storage (sql-tx-options {:operation "delete"
                                                 :id id
                                                 :branch branch
                                                 :flags ["delete"]})]
    (storage/delete-document storage id branch)))

(defn handle-select-case
  "Handles an SQL SELECT query execution.
   Uses handle-select which leverages Lucene AST for efficient filtering.
   Parameters:
   - storage: The storage implementation
   - index: The index implementation (for Lucene filtering)
   - out: The output stream to write results to
   - parsed: The parsed query details
   Returns: nil"
  [storage index ^java.io.OutputStream out parsed]
  (log/log-info (str "Starting handle-select-case with parsed: " parsed))
  (try
    ;; Use handle-select which now uses Lucene AST for efficient filtering
    (let [docs (handle-select storage index parsed)
          columns (:columns parsed)]

      (if (empty? docs)
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
            (messages/send-data-row out [(str (count docs))])
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
                              docs
                              agg-field)
                  _ (log/log-info (str "Aggregate result: " agg-result))]
              (messages/send-row-description out [column-name])
              (messages/send-data-row out [agg-result])
              (messages/send-command-complete out "SELECT" 1)))

          ;; For regular SELECT queries (non-aggregate)
          :else
          (let [;; Get columns to display - either requested ones or all from the results
                column-names (try
                               (if (some #(= :all (:type %)) columns)
                                 ;; If * was used, return all columns
                                 (->> docs
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
                                 (when-let [first-res (first docs)] ; Try to get keys from first result if any
                                   (mapv name (keys first-res)))
                                 []))] ; Empty fallback if no results or error
            (log/log-info (str "Sending column description: " column-names))
            (messages/send-row-description out column-names)
            (doseq [row docs]
              (when row  ;; Guard against nil rows
                (log/log-info (str "Sending row: " (:id row "N/A")))
                ;; Map values according to the filtered columns
                (let [values (mapv #(str (get row (keyword %) ""))
                                   column-names)] ; Use the filtered columns here
                  (messages/send-data-row out values))))
            (messages/send-command-complete out "SELECT" (count docs))))))

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
  "Handles the INSERT case of an SQL query.
   Supports multiple VALUES rows: INSERT INTO table (cols) VALUES (...), (...), ..."
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

          ;; Check if values is a list of lists (multiple rows) or a single list
          values-rows (if (and (sequential? values)
                               (seq values)
                               (every? sequential? values))
                        ;; Multiple rows: values is [[val1 val2], [val3 val4], ...]
                        values
                        ;; Single row: values is [val1 val2 val3]
                        [values])

          ;; Build documents for each row
          documents (mapv (fn [row-values]
                            (let [clean-values (when (seq row-values)
                                                 (map (fn [val]
                                                        ;; Only remove quotes from strings, leave other values as-is
                                                        (if (and (string? val)
                                                                 (or (str/starts-with? val "'")
                                                                     (str/starts-with? val "\"")))
                                                          (str/replace val #"^['\"]|['\"]$" "")
                                                          val))
                                                      row-values))

                                  ;; Make sure columns and values have the same count
                                  _ (when (and (seq clean-columns) (seq clean-values)
                                               (not= (count clean-columns) (count clean-values)))
                                      (throw (Exception. (str "Column count doesn't match value count: "
                                                              (count clean-columns) " columns vs "
                                                              (count clean-values) " values"))))

                                  ;; Create a document map from columns and values
                                  doc-data (if (and (seq clean-columns) (seq clean-values))
                                             (zipmap (map keyword clean-columns) clean-values)
                                             {})

                                  ;; Check if ID is provided, otherwise generate one
                                  id-provided (if-let [id (:id doc-data)]
                                                ;; Use provided ID as is, without adding table prefix
                                                id
                                                ;; Generate a UUID if no ID provided, without table prefix
                                                (str (java.util.UUID/randomUUID)))]

                              ;; Create final document with table information
                              (merge doc-data
                                     {:id id-provided
                                      :_table table-name})))
                          values-rows)

          doc-ids (map :id documents)
          tx-opts (sql-tx-options {:operation "insert"
                                   :table table-name
                                   :branch branch-name
                                   :metadata (cond-> {:document-count (count documents)}
                                               (seq doc-ids) (assoc :document-ids (vec doc-ids)))})

          saved-docs (tx/with-transaction [storage tx-opts]
                       (mapv (fn [doc]
                               (log/log-info (str "Document to insert: " doc))
                               (let [saved (storage/save-document storage doc branch-name)]
                                 ;; Index document when index is provided
                                 (when index
                                   (try
                                     (index/index-document index saved)
                                     (log/log-info (str "Document indexed successfully: " (:id saved)))
                                     (catch Exception e
                                       (log/log-error (str "Error indexing document: " (:id saved)
                                                           " - " (.getMessage e))))))
                                 saved))
                             documents))
          inserted-count (count saved-docs)]

      (log/log-info (str "Inserted " inserted-count " document(s)"))

      (messages/send-command-complete out "INSERT" inserted-count)

      ;; Send ReadyForQuery to finalize the response
      (messages/send-ready-for-query out \I)

      ;; Return the saved documents to support the test cases
      saved-docs)

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
          matching-docs (operators/apply-where-conditions table-docs where-condition)]

      (if (seq matching-docs)
        (let [doc-ids (map :id matching-docs)
              tx-opts (sql-tx-options {:operation "update"
                                       :table table-name
                                       :branch branch-name
                                       :flags ["update"]
                                       :metadata (cond-> {:document-count (count matching-docs)}
                                                   (seq doc-ids) (assoc :document-ids (vec doc-ids)))})
              updated-docs (tx/with-transaction [storage tx-opts]
                             (mapv (fn [doc]
                                     (let [updated-doc (merge doc updates)
                                           saved (storage/save-document storage updated-doc branch-name)]
                                       (when (and saved index)
                                         (index/index-document index saved))
                                       saved))
                                   matching-docs))]
          (log/log-info (str "Updated " (count updated-docs) " documents successfully"))
          (messages/send-command-complete out "UPDATE" (count updated-docs))
          (messages/send-ready-for-query out \I)
          updated-docs)

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
            (let [tx-opts (sql-tx-options {:operation "delete"
                                           :id id
                                           :table table-name
                                           :branch branch-name
                                           :flags ["delete"]})
                  deleted (tx/with-transaction [storage tx-opts]
                            (let [result (storage/delete-document storage id branch-name)]
                              (when (and result index)
                                (index/delete-document index id))
                              result))]
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
   - session-context: Optional atom with session state (e.g., current branch)
   Returns: nil"
  ([storage index ^java.io.OutputStream out sql]
   (handle-query storage index out sql nil))
  ([storage index ^java.io.OutputStream out sql session-context]
   (log/log-info (str "Executing query: " sql))
   (try
     (let [parsed (statements/parse-sql-query sql)
           query-type (:type parsed)
           query-function (:function parsed)]
       (log/log-info (str "Parsed query type: " query-type ", Details: " parsed))
       (cond
         ;; DDL: CREATE TABLE
         (= query-type :create-table)
         (ddl/handle-create-table storage out parsed)

         ;; DDL: DROP TABLE
         (= query-type :drop-table)
         (ddl/handle-drop-table storage out parsed)

         ;; DDL: SHOW TABLES
         (= query-type :show-tables)
         (ddl/handle-show-tables storage out parsed)

         ;; DDL: SHOW SCHEMAS
         (= query-type :show-schemas)
         (ddl/handle-show-schemas storage out parsed)

         ;; DDL: DESCRIBE
         (= query-type :describe)
         (ddl/handle-describe storage out parsed)

         ;; DDL: CREATE VALIDATION SCHEMA
         (= query-type :create-validation-schema)
         (ddl/handle-create-validation-schema storage out parsed)

         ;; DDL: DROP VALIDATION SCHEMA
         (= query-type :drop-validation-schema)
         (ddl/handle-drop-validation-schema storage out parsed)

         ;; DDL: SHOW VALIDATION SCHEMA FOR namespace
         (= query-type :show-validation-schema)
         (ddl/handle-show-validation-schema storage out parsed)

         ;; DDL: SHOW VALIDATION SCHEMAS
         (= query-type :show-validation-schemas)
         (ddl/handle-show-validation-schemas storage out parsed)

         ;; ChronDB branch functions
         (and (= query-type :chrondb-function)
              (contains? #{:branch-list :branch-create :branch-checkout :branch-merge} query-function))
         (ddl/handle-chrondb-branch-function storage out parsed session-context)

         ;; ChronDB time-travel functions (history, at, diff)
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
       (messages/send-ready-for-query out \E)))))
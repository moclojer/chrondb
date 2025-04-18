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

          ;; Verificar se é uma consulta direta por ID
          id-condition (when (and (= (count std-conditions) 1)
                                  (= (:field (first std-conditions)) "id")
                                  (= (:op (first std-conditions)) "="))
                         (first std-conditions))

          id-value (when id-condition
                     (str/replace (:value id-condition) #"['\"]" ""))

          ;; --- Get initial documents ---
          primary-docs (cond
                        ;; Specific document ID requested - check if it belongs to table
                         (and id-value (seq table-name))
                         (let [id-doc (storage/get-document storage id-value branch-name)]
                           (if (or
                                ;; Either the document belongs to the specified table
                                (and id-doc (= (:_table id-doc) table-name))
                                ;; Or we're querying the special "doc" table which should access all documents
                                (and id-doc (= table-name "doc"))
                                ;; Or the document ID has a prefix matching the table name (for backward compatibility)
                                (and id-doc (str/starts-with? id-value (str table-name ":"))))
                             [id-doc]
                             []))

                        ;; No table specified - invalid
                         (not table-name)
                         (do
                           (log/log-error "No table specified in query")
                           [])

                        ;; Full-text search - index required
                         (and (seq fts-conditions) index)
                         (let [docs (mapcat #(fts-get-matching-docs index storage % branch-name) fts-conditions)]
                           docs)

                         :else
                         (let [docs (if (= table-name "doc")
                                      ;; Special case for "doc" table - get all documents
                                      (storage/get-documents-by-prefix storage "" branch-name)
                                      ;; Normal case - get documents for specific table
                                      (storage/get-documents-by-table storage table-name branch-name))]
                           docs))

          ;; --- Handle JOIN if present ---
          joined-docs (if join-info
                        (let [join-table (when join-info (:table join-info))
                              join-branch (normalize-schema-to-branch (:schema join-info))
                              on-condition (:on join-info)]

                          (if (and join-table on-condition)
                            (let [;; Get documents from join table
                                  second-table-docs (storage/get-documents-by-table storage join-table join-branch)

                                  ;; Extract join condition details
                                  left-table (:left-table on-condition)
                                  left-field (:left-field on-condition)
                                  right-table (:right-table on-condition)
                                  right-field (:right-field on-condition)

                                  ;; Determine which collection is which (primary vs secondary)
                                  [primary-key _ secondary-key]
                                  (if (= left-table table-name)
                                    [left-field right-table right-field]
                                    [right-field left-table left-field])

                                  ;; Determine join type
                                  join-type (:type join-info)
                                  is-left-join (= join-type :left)

                                  ;; Create joined documents based on join type
                                  docs (if is-left-join
                                        ;; LEFT JOIN: Keep all records from primary table
                                         (let [_ (log/log-info (str "PERFORMING LEFT JOIN with " (count primary-docs) " primary docs and " (count second-table-docs) " secondary docs"))
                                               ;; First, get all possible field names from secondary docs
                                               secondary-fields (if (seq second-table-docs)
                                                                  (distinct (mapcat (fn [doc]
                                                                                      (keep (fn [[k _]]
                                                                                              (when (not= k :_table)
                                                                                                (keyword (str join-table "." (name k)))))
                                                                                            doc))
                                                                                    second-table-docs))
                                                                  [])
                                               _ (log/log-info (str "Secondary fields: " (pr-str secondary-fields)))

                                               all-docs
                                               (for [primary-doc primary-docs]
                                                 (let [primary-key-val (get primary-doc (keyword primary-key))
                                                       ;; Usar str para garantir compatibilidade de tipos na comparação
                                                       primary-key-str (if (nil? primary-key-val) "" (str primary-key-val))

                                                       ;; First create the primary side of the result with prefixed fields
                                                       primary-map (into {}
                                                                         (keep (fn [[k v]]
                                                                              ;; Filtrar campos _table
                                                                                 (when (not= k :_table)
                                                                                   [(keyword (str table-name "." (name k))) v]))
                                                                               primary-doc))

                                                       ;; Create a map with nil values for all secondary fields
                                                       null-secondary-map (into {} (map #(vector % nil) secondary-fields))

                                                       ;; Find matching secondary docs
                                                       matching-secondary-docs
                                                       (filter #(let [secondary-key-val (get % (keyword secondary-key))
                                                                      secondary-key-str (if (nil? secondary-key-val) "" (str secondary-key-val))]
                                                                  (= primary-key-str secondary-key-str))
                                                               second-table-docs)]
                                                   (if (seq matching-secondary-docs)
                                                     ;; For matches, merge primary with secondary values
                                                     (let [secondary-doc (first matching-secondary-docs)
                                                           secondary-map (into {}
                                                                               (keep (fn [[k v]]
                                                                                       (when (not= k :_table)
                                                                                         [(keyword (str join-table "." (name k))) v]))
                                                                                     secondary-doc))]
                                                       (merge primary-map secondary-map))
                                                     ;; For non-matches, merge primary with null secondary values
                                                     (merge primary-map null-secondary-map))))]
                                           (log/log-info (str "Created " (count all-docs) " joined documents"))
                                           all-docs)

                                        ;; INNER JOIN: Only where both tables match
                                         (for [primary-doc primary-docs
                                               secondary-doc second-table-docs
                                               :when (let [primary-key-val (get primary-doc (keyword primary-key))
                                                           secondary-key-val (get secondary-doc (keyword secondary-key))
                                                           primary-key-str (if (nil? primary-key-val) "" (str primary-key-val))
                                                           secondary-key-str (if (nil? secondary-key-val) "" (str secondary-key-val))]
                                                       (= primary-key-str secondary-key-str))]
                                          ;; Merge documents and prefix keys with table name, excluindo campos _table
                                           (merge
                                            (into {} (keep (fn [[k v]]
                                                            ;; Filtrar campos _table
                                                             (when (not= k :_table)
                                                               [(keyword (str table-name "." (name k))) v]))
                                                           primary-doc))
                                            (into {} (keep (fn [[k v]]
                                                            ;; Filtrar campos _table
                                                             (when (not= k :_table)
                                                               [(keyword (str join-table "." (name k))) v]))
                                                           secondary-doc)))))]
                              docs)
                            ;; No valid join condition
                            (do
                              (log/log-error "Invalid JOIN - missing table or ON condition")
                              primary-docs)))
                        ;; No join - just use primary docs
                        primary-docs)

          ;; --- Apply WHERE conditions (skip if já aplicamos a condição de ID) ---
          filtered-docs (if id-value
                          joined-docs
                          (let [join-type (get-in join-info [:type])
                                is-left-join (= join-type :left)
                                ;; For LEFT JOIN with conditions on the right table fields, we need special handling
                                right-table-name (get-in join-info [:table])
                                right-table-conditions (when (and is-left-join right-table-name)
                                                         (filter (fn [condition]
                                                                   (and (:field condition)
                                                                        (string? (:field condition))
                                                                        (str/starts-with? (:field condition) (str right-table-name "."))))
                                                                 std-conditions))
                                left-table-conditions (when (and is-left-join right-table-name)
                                                        (remove (fn [condition]
                                                                  (and (:field condition)
                                                                       (string? (:field condition))
                                                                       (str/starts-with? (:field condition) (str right-table-name "."))))
                                                                std-conditions))]
                            (cond
                              ;; For LEFT JOIN with right table conditions:
                              ;; Need special handling to ensure we only include rows where the right table fields
                              ;; match the conditions - this means filtering out rows where:
                              ;; 1. The right side fields are NULL (no match for the left side record)
                              ;; 2. The right side fields don't satisfy the conditions
                              (and is-left-join (seq right-table-conditions))
                              (let [;; For LEFT JOIN with WHERE on right table, we need to handle it differently
                                   ;; The issue is that we need to both:
                                   ;; 1. Filter out rows that don't satisfy the conditions
                                   ;; 2. Include rows where right table fields are NULL (legitimate LEFT JOIN matches)

                                    right-prefixed-fields (map #(keyword (:field %)) right-table-conditions)

                                   ;; Identify documents without values for the secondary table (NULL values from LEFT JOIN)
                                    docs-without-right-fields (filter (fn [doc]
                                                                        (let [has-any-right-field (some #(contains? doc %) right-prefixed-fields)]
                                                                          (not has-any-right-field)))
                                                                      joined-docs)
                                    _ (log/log-info (str "Docs without right fields (NULL joined): " (count docs-without-right-fields)))

                                   ;; These docs have non-null values for all right table fields in the conditions
                                    docs-with-right-fields (filter (fn [doc]
                                                                     (every? #(contains? doc %) right-prefixed-fields))
                                                                   joined-docs)
                                    _ (log/log-info (str "Docs with right fields: " (count docs-with-right-fields)))

                                   ;; Apply conditions only to docs that have right table fields
                                    docs-matching-conditions (operators/apply-where-conditions docs-with-right-fields right-table-conditions)
                                    _ (log/log-info (str "Docs matching right conditions: " (count docs-matching-conditions)))

                                   ;; Apply left table conditions only to documents with matches in the right table
                                    docs-with-left-conditions (if (seq left-table-conditions)
                                                                (operators/apply-where-conditions docs-matching-conditions left-table-conditions)
                                                                docs-matching-conditions)
                                    _ (log/log-info (str "Docs matching left conditions: " (count docs-with-left-conditions)))

                                   ;; Apply left table conditions to documents without matches in the right table
                                    docs-without-right-filtered (if (seq left-table-conditions)
                                                                  (operators/apply-where-conditions docs-without-right-fields left-table-conditions)
                                                                  docs-without-right-fields)

                                   ;; Combine the results
                                    final-filtered (concat docs-with-left-conditions docs-without-right-filtered)]

                                final-filtered)

                              ;; Normal condition filtering - mas garantimos manter registros mesmo sem JOIN
                              (and is-left-join (seq std-conditions))
                              (do
                                (log/log-info "Applying standard conditions to LEFT JOIN result")
                                (operators/apply-where-conditions joined-docs std-conditions))

                              ;; If we have standard conditions but no LEFT JOIN, apply them
                              (seq std-conditions)
                              (do
                                (log/log-info "Applying standard conditions to result")
                                (operators/apply-where-conditions joined-docs std-conditions))

                              ;; No conditions - just return joined docs
                              :else
                              (do
                                (log/log-info "No conditions to apply, returning all joined docs")
                                joined-docs))))
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
                             (process-groups grouped-docs query))

                           ;; No grouping - just process columns
                           :else
                           (do
                             (log/log-info "No grouping: processing documents directly")
                             (let [result (mapv (fn [doc]
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
                                                filtered-docs)]
                               (log/log-info (str "FINAL RESULT COUNT: " (count result)))
                               result)))

          _ (log/log-info (str "After processing/projection: " (count processed-docs) " documents"))

          sorted-results (operators/sort-docs-by processed-docs order-by)
          _ (log/log-info (str "After sorting: " (count sorted-results) " documents"))

          limited-results (operators/apply-limit sorted-results limit)
          _ (log/log-info (str "After applying limit: " (count limited-results) " documents"))]

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
        (let [;; Get column names from all results, not just the first one
              ;; This ensures we capture all possible columns across all documents
              columns (try
                        (let [;; Extract all unique keys from all documents
                              all-keys (->> results
                                            (mapcat keys)
                                            (filter #(not= % :_table)) ; Exclude :_table
                                            (filter #(not (str/ends-with? (name %) "_table"))) ; Exclude table-prefixed _table fields
                                            (into #{})
                                            (sort))]
                          (mapv #(if (keyword? %) (name %) (str %)) all-keys))
                        (catch Exception e
                          (log/log-error (str "Error extracting column names: " (.getMessage e)))
                          (when-let [first-res (first results)] ; Try to get keys from first result if any
                            (mapv name (keys first-res)))
                          []))] ; Empty fallback if no results or error
          (log/log-info (str "Sending column description: " columns))
          (messages/send-row-description out columns)
          (doseq [row results]
            (log/log-info (str "Sending row: " (:id row "N/A")))
            ;; Map values according to the filtered columns
            (let [values (map #(let [val (get row (if (string? %) (keyword %) %))]
                                 (if (nil? val)
                                   ""
                                   (try
                                     ;; Try to check if it's binary data, but safely handle ClassNotFound
                                     (if (instance? (Class/forName "[B") val)
                                       ;; For binary data, just log a placeholder
                                       val
                                       ;; For other types, convert to string
                                       (str val))
                                     (catch ClassNotFoundException _
                                       ;; If we can't find the byte array class, we're likely in a test environment
                                       (str val)))))
                              columns)] ; Use the filtered columns here
              (messages/send-data-row out values)))
          (messages/send-command-complete out "SELECT" (count results)))))

    ;; Add ReadyForQuery message at the end of all SELECT operations
    (messages/send-ready-for-query out \I)
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
      (case query-type
        :select (handle-select-case storage index out parsed)
        :insert (handle-insert-case storage index out parsed)
        :update (handle-update-case storage index out parsed)
        :delete (handle-delete-case storage index out parsed)
        ;; Default case for unknown or invalid query types
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
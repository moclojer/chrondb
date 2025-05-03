(ns chrondb.api.sql.execution.join
  "SQL join operations"
  (:require [clojure.string :as str]
            [chrondb.util.logging :as log]
            [chrondb.storage.protocol :as storage]))

(defn perform-join
  "Performs a SQL JOIN operation between two tables.
   Parameters:
   - storage: The storage implementation
   - primary-table: The name of the primary table
   - join-info: A map containing join information
   Returns: A sequence of joined documents"
  [storage primary-table join-info]
  (log/log-info (str "Performing join between " primary-table " and " (:table join-info)))

  (let [join-table (when join-info (:table join-info))
        join-branch (or (:schema join-info) "main")
        on-condition (:on join-info)]

    (if (and join-table on-condition)
      (let [;; Get primary docs - make sure to get all documents
            primary-docs (do
                           (log/log-info (str "Retrieving all documents from primary table " primary-table))
                           (let [docs (storage/get-documents-by-table storage primary-table "main")]
                             (log/log-info (str "Retrieved " (count docs) " documents from primary table "
                                                primary-table " with IDs: " (pr-str (map :id docs))))
                             docs))

            ;; Get documents from join table
            second-table-docs (do
                                (log/log-info (str "Retrieving all documents from join table " join-table))
                                (let [docs (storage/get-documents-by-table storage join-table join-branch)]
                                  (log/log-info (str "Retrieved " (count docs) " documents from join table "
                                                     join-table " with IDs: " (pr-str (map :id docs))))
                                  docs))

            ;; Extract join condition details
            left-table (:left-table on-condition)
            left-field (:left-field on-condition)
            right-table (:right-table on-condition)
            right-field (:right-field on-condition)

            ;; Determine which field belongs to which table
            ;; Corrected to handle tables properly regardless of order
            primary-key (if (= left-table primary-table)
                          left-field
                          right-field)
            secondary-key (if (= left-table primary-table)
                            right-field
                            left-field)

            ;; Log all the join fields for debugging
            _ (log/log-info (str "Join condition: " primary-table "." primary-key " = " join-table "." secondary-key))

            ;; Log primary key values for debugging
            _ (log/log-info (str "Primary key values: "
                                 (pr-str (map #(get % (keyword primary-key)) primary-docs))))

            ;; Log secondary key values for debugging
            _ (log/log-info (str "Secondary key values: "
                                 (pr-str (map #(get % (keyword secondary-key)) second-table-docs))))

            ;; Determine join type
            join-type (:type join-info)
            is-left-join (= join-type :left)
            _ (log/log-info (str "Join type: " (if is-left-join "LEFT JOIN" "INNER JOIN")))

            ;; Create joined documents based on join type
            docs (if is-left-join
                   ;; LEFT JOIN: Keep all records from primary table
                   (let [_ (log/log-info (str "PERFORMING LEFT JOIN with " (count primary-docs) " primary docs and "
                                              (count second-table-docs) " secondary docs"))

                         ;; First, extract all possible field names from secondary documents
                         ;; This is important for LEFT JOIN to ensure all fields are present with nil values
                         ;; when there's no match
                         secondary-fields (if (seq second-table-docs)
                                            (distinct (mapcat (fn [doc]
                                                                (keep (fn [[k _]]
                                                                        (when (not= k :_table)
                                                                          (keyword (str join-table "." (name k)))))
                                                                      doc))
                                                              second-table-docs))
                                            [])
                         _ (log/log-info (str "Secondary fields for nulls: " (pr-str secondary-fields)))

                         ;; Process each primary document
                         all-docs
                         (mapv
                          (fn [primary-doc]
                            (let [primary-key-val (get primary-doc (keyword primary-key))
                                  ;; Use str to guarantee type compatibility in comparison
                                  primary-key-str (if (nil? primary-key-val) "" (str primary-key-val))

                                  _ (log/log-info (str "Processing primary doc with ID " (:id primary-doc) " key "
                                                       primary-key " = " primary-key-str))

                                  ;; First create the primary side of the result with prefixed fields
                                  primary-map (into {}
                                                    (keep (fn [[k v]]
                                                            ;; Filter _table fields
                                                            (when (not= k :_table)
                                                              [(keyword (str primary-table "." (name k))) v]))
                                                          primary-doc))

                                  ;; Create a map with nil values for all secondary fields
                                  ;; This ensures LEFT JOIN returns all required fields with nil values when no match
                                  null-secondary-map (into {} (map #(vector % nil) secondary-fields))

                                  ;; Find matching secondary docs
                                  matching-secondary-docs
                                  (filter #(let [secondary-key-val (get % (keyword secondary-key))
                                                 secondary-key-str (if (nil? secondary-key-val) "" (str secondary-key-val))]
                                             (= primary-key-str secondary-key-str))
                                          second-table-docs)]

                              (log/log-info (str "Found " (count matching-secondary-docs)
                                                 " matching secondary docs for primary key " primary-key-str))

                              (if (seq matching-secondary-docs)
                                ;; For matches, merge primary with secondary values
                                (let [secondary-doc (first matching-secondary-docs)
                                      secondary-map (into {}
                                                          (keep (fn [[k v]]
                                                                  (when (not= k :_table)
                                                                    [(keyword (str join-table "." (name k))) v]))
                                                                secondary-doc))]
                                  (log/log-info (str "Joining " (:id primary-doc) " with " (:id secondary-doc)))
                                  (merge primary-map secondary-map))

                                ;; For non-matches in LEFT JOIN, merge primary with null secondary values
                                (do
                                  (log/log-info (str "No matches found for " (:id primary-doc) " with key " primary-key-str
                                                     ", returning primary with null secondary fields"))
                                  (merge primary-map null-secondary-map)))))
                          primary-docs)]

                     (log/log-info (str "Created " (count all-docs) " joined documents with LEFT JOIN"))
                     ;; Always return the result for left join
                     all-docs)

                   ;; INNER JOIN: Only where both tables match
                   (let [joined-docs
                         (mapv
                          (fn [matching-pair]
                            (let [primary-doc (first matching-pair)
                                  secondary-doc (second matching-pair)
                                  ;; Merge documents and prefix keys with table name, exclude _table fields
                                  primary-map (into {}
                                                    (keep (fn [[k v]]
                                                            (when (not= k :_table)
                                                              [(keyword (str primary-table "." (name k))) v]))
                                                          primary-doc))
                                  secondary-map (into {}
                                                      (keep (fn [[k v]]
                                                              (when (not= k :_table)
                                                                [(keyword (str join-table "." (name k))) v]))
                                                            secondary-doc))]
                              (merge primary-map secondary-map)))
                          (for [primary-doc primary-docs
                                secondary-doc second-table-docs
                                :when (let [primary-key-val (get primary-doc (keyword primary-key))
                                            secondary-key-val (get secondary-doc (keyword secondary-key))
                                            primary-key-str (if (nil? primary-key-val) "" (str primary-key-val))
                                            secondary-key-str (if (nil? secondary-key-val) "" (str secondary-key-val))]
                                        (= primary-key-str secondary-key-str))]
                            [primary-doc secondary-doc]))]
                     (log/log-info (str "Created " (count joined-docs) " joined documents with INNER JOIN"))
                     joined-docs))]

        ;; Verify that we have the correct number of results for LEFT JOIN
        (when (and is-left-join (seq primary-docs) (not (seq docs)))
          (log/log-error "LEFT JOIN produced zero results with non-empty primary table - this should never happen!"))

        ;; For LEFT JOIN, the number of results should match the number of primary docs
        (when (and is-left-join (not= (count docs) (count primary-docs)))
          (log/log-warn (str "LEFT JOIN resulted in " (count docs) " rows, but expected "
                             (count primary-docs) " rows (one per primary record)")))

        docs)
      ;; No valid join condition
      (do
        (log/log-error "Invalid JOIN - missing table or ON condition")
        []))))
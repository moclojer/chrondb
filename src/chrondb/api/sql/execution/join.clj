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
      (let [;; Get primary docs
            primary-docs (storage/get-documents-by-table storage primary-table "main")
            ;; Get documents from join table
            second-table-docs (storage/get-documents-by-table storage join-table join-branch)

            ;; Extract join condition details
            left-table (:left-table on-condition)
            left-field (:left-field on-condition)
            right-table (:right-table on-condition)
            right-field (:right-field on-condition)

            ;; Determine which collection is which (primary vs secondary)
            [primary-key _ secondary-key]
            (if (= left-table primary-table)
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
                                 ;; Use str to guarantee type compatibility in comparison
                                 primary-key-str (if (nil? primary-key-val) "" (str primary-key-val))

                                 ;; First create the primary side of the result with prefixed fields
                                 primary-map (into {}
                                                   (keep (fn [[k v]]
                                                           ;; Filter _table fields
                                                           (when (not= k :_table)
                                                             [(keyword (str primary-table "." (name k))) v]))
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
                     ;; Merge documents and prefix keys with table name, exclude _table fields
                     (merge
                      (into {} (keep (fn [[k v]]
                                       ;; Filter _table fields
                                       (when (not= k :_table)
                                         [(keyword (str primary-table "." (name k))) v]))
                                     primary-doc))
                      (into {} (keep (fn [[k v]]
                                       ;; Filter _table fields
                                       (when (not= k :_table)
                                         [(keyword (str join-table "." (name k))) v]))
                                     secondary-doc)))))]
        docs)
      ;; No valid join condition
      (do
        (log/log-error "Invalid JOIN - missing table or ON condition")
        []))))
(ns chrondb.index.memory
  "In-memory implementation of the Index protocol.
   This implementation stores documents in memory and provides basic search capabilities."
  (:require [chrondb.index.protocol :as protocol]
            [clojure.string :as str]
            [chrondb.util.logging :as log])
  (:import [java.util.concurrent ConcurrentHashMap]))

;; This function is currently unused
#_(defn- document-matches?
    "Checks if a document matches the given query string.
   Simple implementation that checks if any field contains the query string."
    [doc query]
    (let [query-lower (str/lower-case query)]
      (some (fn [[_ v]]
              (and (string? v)
                   (str/includes? (str/lower-case v) query-lower)))
            doc)))

(defrecord MemoryIndex [^ConcurrentHashMap data]
  protocol/Index
  (index-document [_ doc]
    (let [id (:id doc)]
      (.put data id doc)
      doc))

  (delete-document [_ id]
    (.remove data id)
    nil)

  (search [_ field query-string branch]
    (log/log-warn (str "MemoryIndex search called (field: " field ", query: " query-string ", branch: " branch "). Basic string matching performed, not full FTS."))
    (let [all-docs (seq (.values data))
          query-lower (str/lower-case query-string)]
      (filter
       (fn [doc]
         (some (fn [[k v]]
                 (when (string? v)
                   (cond
                     (= (name k) field) (str/includes? (str/lower-case v) query-lower)
                     (= field "content") (str/includes? (str/lower-case v) query-lower)
                     :else false)))
               doc))
       all-docs)))

  (search-query [_ query-map _branch {:keys [limit offset] :or {limit 100 offset 0}}]
    (let [;; Support legacy format: {:field "name" :value "John"}
          ;; Save original field and value before converting
          original-field (:field query-map)
          original-value (:value query-map)
          ;; Convert to AST format with :clauses
          query-map (if (and original-field original-value (not (:clauses query-map)))
                     (let [field original-field
                           value original-value
                           ;; For "content" field, search in all fields (match-all with wildcard)
                           clause (if (= field "content")
                                   {:type :match-all}
                                   {:type :wildcard
                                    :field field
                                    :value (str value "*")})]
                       {:clauses [clause]})
                     query-map)
          {:keys [clauses]} query-map
          limit (max 1 limit)
          offset (max 0 offset)
          matcher (fn matcher [doc clause]
                    (let [{:keys [type field value lower upper include-lower? include-upper? must should must-not filter]} clause]
                      (case type
                        :match-all true
                        :term (= (get doc (keyword field)) value)
                        :wildcard (let [pattern (-> value str/lower-case (str/replace "*" ".*") (str/replace "?" "."))
                                        regex (re-pattern pattern)
                                        v (some-> (get doc (keyword field)) str str/lower-case)]
                                    (boolean (and v (re-matches regex v))))
                        :range (let [doc-val (get doc (keyword field))]
                                 (when doc-val
                                   (let [comp (compare (str doc-val) (str (or lower doc-val)))
                                         comp-upper (compare (str doc-val) (str (or upper doc-val)))]
                                     (and (if lower
                                            (if include-lower?
                                              (>= comp 0)
                                              (> comp 0))
                                            true)
                                          (if upper
                                            (if include-upper?
                                              (<= comp-upper 0)
                                              (< comp-upper 0))
                                            true)))))
                        :boolean (let [must (every? #(matcher doc %) must)
                                       should (if (seq should)
                                                (some #(matcher doc %) should)
                                                true)
                                       must-not (every? #(not (matcher doc %)) must-not)
                                       filter-ok (every? #(matcher doc %) filter)]
                                   (and must should must-not filter-ok))
                        false)))
          ;; Special handling for content field search
          content-value (when (and original-field (= "content" original-field))
                         original-value)

          matches? (if content-value
                    ;; For content field, search in all fields of the document
                    ;; Similar to the search function - check if value appears in any field
                    (fn [doc]
                      (let [query-lower (str/lower-case (str content-value))]
                        (some (fn [[k v]]
                                (when (and (not= k :_table)
                                         (string? v))
                                  (str/includes? (str/lower-case v) query-lower)))
                              doc)))
                    ;; Normal clause-based matching
                    (fn [doc]
                      (if (seq clauses)
                        (every? #(matcher doc %) clauses)
                        true)))
          docs (->> (.values data)
                    (filter matches?)
                    (map :id)
                    (drop offset)
                    (take limit)
                    vec)
          total (count (filter matches? (.values data)))]
      {:ids docs
       :total total
       :limit limit
       :offset offset}))

  (close [_]
    (log/log-info "Closing MemoryIndex...")
    (.clear data)
    (log/log-info "MemoryIndex closed.")))

(defn create-memory-index
  "Creates a new in-memory index.
   Returns: A new MemoryIndex instance."
  []
  (->MemoryIndex (ConcurrentHashMap.)))
(ns chrondb.query.ast
  "Helpers for building query AST structures that the Lucene engine can
   execute. The AST is intentionally lightweight: leaf clauses describe
   predicates and boolean nodes combine them. A complete query may also
   include pagination, sorting hints and branch selection."
  (:refer-clojure :exclude [range boolean and or not sort-by]))

(defn match-all
  "Clause that matches all documents."
  []
  {:type :match-all})

(defn term
  "Exact match clause."
  [field value]
  (when (clojure.core/and field (some? value))
    {:type :term
     :field (name field)
     :value (str value)}))

(defn wildcard
  "Wildcard match using ?/* patterns."
  [field value]
  (when (clojure.core/and field (some? value))
    {:type :wildcard
     :field (name field)
     :value (str value)}))

(defn prefix
  "Convenience for prefix searches (converted to wildcard)."
  [field value]
  (when value
    (wildcard field (str value "*"))))

(defn range
  "Creates a range clause. Provide nil for open bounds.
   For inclusive/exclusive bounds use the options map.
   Supports value-type option: :long, :double, :string (default)."
  ([field lower upper]
   (range field lower upper {:include-lower? true :include-upper? true}))
  ([field lower upper {:keys [include-lower? include-upper? type]
                       :or {include-lower? true
                            include-upper? true}}]
   (when field
     {:type :range
      :field (name field)
      :lower (when (some? lower) (str lower))
      :upper (when (some? upper) (str upper))
      :include-lower? (clojure.core/boolean include-lower?)
      :include-upper? (clojure.core/boolean include-upper?)
      :value-type type})))

(defn range-long
  "Creates a range clause for Long values."
  ([field lower upper]
   (range-long field lower upper {:include-lower? true :include-upper? true}))
  ([field lower upper opts]
   (range field lower upper (assoc opts :type :long))))

(defn range-double
  "Creates a range clause for Double values."
  ([field lower upper]
   (range-double field lower upper {:include-lower? true :include-upper? true}))
  ([field lower upper opts]
   (range field lower upper (assoc opts :type :double))))

(defn fts
  "Full-text search clause (uses dedicated analyzer)."
  [field value]
  (when (clojure.core/and field (some? value))
    {:type :fts
     :field (name field)
     :value (str value)
     :analyzer :fts}))

(defn exists
  "Clause that asserts the existence of a field."
  [field]
  (when field
    {:type :exists
     :field (name field)}))

(defn missing
  "Clause that asserts the absence of a field."
  [field]
  (when field
    {:type :missing
     :field (name field)}))

(defn- vectorize [xs]
  (->> xs (remove nil?) vec))

(defn boolean
  "Generic boolean node."
  [{:keys [must should must-not filter]}]
  (let [must (vectorize must)
        should (vectorize should)
        must-not (vectorize must-not)
        filter (vectorize filter)]
    (cond
      (every? empty? [must should must-not filter]) (match-all)
      (clojure.core/and (empty? must)
                        (= 1 (count should))
                        (empty? must-not)
                        (empty? filter)) (first should)
      (clojure.core/and (empty? must)
                        (empty? should)
                        (= 1 (count must-not))
                        (empty? filter))
      {:type :boolean
       :must [(match-all)]
       :should []
       :must-not must-not
       :filter []}
      :else
      {:type :boolean
       :must must
       :should should
       :must-not must-not
       :filter filter})))

(defn and
  "Combines clauses using logical AND."
  [& clauses]
  (let [clauses (vectorize clauses)]
    (case (count clauses)
      0 (match-all)
      1 (first clauses)
      (boolean {:must clauses}))))

(defn or
  "Combines clauses using logical OR."
  [& clauses]
  (let [clauses (vectorize clauses)]
    (case (count clauses)
      0 (match-all)
      1 (first clauses)
      (boolean {:should clauses}))))

(defn not
  "Negates a clause."
  [clause]
  (boolean {:must [(match-all)]
            :must-not [(clojure.core/or clause (match-all))]}))

(defn sort-by
  "Creates a sort descriptor. Optionally specify :direction and :type (:string, :long, :double, :doc, :score)."
  ([field]
   (sort-by field :asc))
  ([field direction]
   (sort-by field direction :string))
  ([field direction type]
   {:field (name field)
    :direction (keyword direction)
    :type (keyword type)}))

(defn query
  "Wraps clauses with optional metadata (sorting, pagination, branch, hints).
   Supports searchAfter for cursor-based pagination."
  ([clauses]
   (query clauses nil))
  ([clauses {:keys [sort limit offset branch hints after]}]
   {:clauses (vectorize clauses)
    :sort (some-> sort vectorize)
    :limit limit
    :offset offset
    :branch branch
    :hints hints
    :after after}))

(defn with-branch [query branch]
  (assoc query :branch branch))

(defn with-hints [query hints]
  (update query :hints merge hints))

(defn with-search-after
  "Adds searchAfter cursor to a query for cursor-based pagination.
   The cursor should be a ScoreDoc from a previous query result."
  [query cursor]
  (assoc query :after cursor))

(defn with-pagination
  "Adds pagination parameters to a query.
   Options:
   - :limit - maximum number of results
   - :offset - number of results to skip
   - :after - ScoreDoc cursor for searchAfter (alternative to offset)"
  [query {:keys [limit offset after]}]
  (cond-> query
    limit (assoc :limit limit)
    offset (assoc :offset offset)
    after (assoc :after after)))

(defn with-sort
  "Adds sort descriptors to a query.
   Accepts a vector of sort descriptors (from sort-by) or a single descriptor."
  [query sort-descriptors]
  (assoc query :sort (vectorize (if (sequential? sort-descriptors)
                                  sort-descriptors
                                  [sort-descriptors]))))

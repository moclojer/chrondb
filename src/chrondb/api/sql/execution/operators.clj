(ns chrondb.api.sql.execution.operators
  "SQL operators for query execution"
  (:require [clojure.string :as str]
            [chrondb.util.logging :as log]))

(defn- try-parse-number
  "Attempts to parse a value as a number.
   Returns the number if successful, nil otherwise."
  [v]
  (cond
    (number? v) v
    (string? v) (try
                  (let [clean (str/replace v #"['\"]" "")]
                    (if (str/includes? clean ".")
                      (Double/parseDouble clean)
                      (Long/parseLong clean)))
                  (catch Exception _
                    nil))
    :else nil))

(defn evaluate-condition
  "Evaluates a condition against a document.
   Parameters:
   - doc: The document to be evaluated
   - condition: The condition to be evaluated
   Returns: true if the condition is met, false otherwise"
  [doc condition]
  (let [field-val (get doc (keyword (:field condition)))
        cond-val (str/replace (:value condition) #"['\"]" "")
        operator (str/lower-case (:op condition))
        ;; Try to parse as numbers for numeric comparisons
        field-num (try-parse-number field-val)
        cond-num (try-parse-number cond-val)]
    ;; If field value is nil (e.g., from LEFT JOIN), return false for comparison operators
    (if (nil? field-val)
      (case operator
        "=" false
        "!=" true
        "<>" true
        ">" false
        "<" false
        ">=" false
        "<=" false
        "is null" true
        "is not null" false
        false)
      (case operator
        "=" (if (and field-num cond-num)
              (= field-num cond-num)
              (= (str field-val) cond-val))
        "!=" (if (and field-num cond-num)
               (not= field-num cond-num)
               (not= (str field-val) cond-val))
        "<>" (if (and field-num cond-num)
               (not= field-num cond-num)
               (not= (str field-val) cond-val))
        ">" (if (and field-num cond-num)
              (> field-num cond-num)
              (> (compare (str field-val) cond-val) 0))
        "<" (if (and field-num cond-num)
              (< field-num cond-num)
              (< (compare (str field-val) cond-val) 0))
        ">=" (if (and field-num cond-num)
               (>= field-num cond-num)
               (>= (compare (str field-val) cond-val) 0))
        "<=" (if (and field-num cond-num)
               (<= field-num cond-num)
               (<= (compare (str field-val) cond-val) 0))
        "like" (re-find (re-pattern (str "(?i)" (str/replace cond-val #"%" ".*"))) (str field-val))
        "is null" false
        "is not null" true
        ;; Default case
        (do
          (log/log-warn (str "Unsupported operator: " operator))
          false)))))

(defn apply-where-conditions
  "Applies WHERE conditions to filter documents.
   Parameters:
   - docs: The documents to filter
   - conditions: A sequence of condition maps with :field, :op, and :value
   Returns: Filtered documents that match all conditions (AND logic)"
  [docs conditions]
  (if (empty? conditions)
    docs
    (filter (fn [doc]
              ;; All conditions must match (AND logic)
              (every? #(evaluate-condition doc %) conditions))
            docs)))

(defn group-docs-by
  "Groups documents by the specified fields.
   Parameters:
   - docs: The documents to be grouped
   - group-fields: The fields to group by
   Returns: Grouped documents"
  [docs group-fields]
  (if (empty? group-fields)
    [docs]
    (let [group-fn (fn [doc]
                     (mapv #(get doc (keyword (:column %))) group-fields))]
      (vals (group-by group-fn docs)))))

(defn sort-docs-by
  "Sorts documents by the specified order clauses.
   Parameters:
   - docs: The documents to be sorted
   - order-clauses: The order clauses to be applied
   Returns: Sorted documents"
  [docs order-clauses]
  (if (empty? order-clauses)
    docs
    (let [comparators (for [{:keys [column direction]} order-clauses]
                        (fn [a b]
                          (let [a-val (get a (keyword column))
                                b-val (get b (keyword column))
                                result (compare a-val b-val)]
                            (if (= direction :desc)
                              (- result)
                              result))))]
      (sort (fn [a b]
              (loop [comps comparators]
                (if (empty? comps)
                  0
                  (let [result ((first comps) a b)]
                    (if (zero? result)
                      (recur (rest comps))
                      result)))))
            docs))))

(defn apply-limit
  "Applies a LIMIT to a sequence of docs
   Parameters:
   - docs: The docs to limit
   - limit: The maximum number of docs to return, or nil for no limit
   Returns: A sequence of docs, limited if limit is provided"
  [docs limit]
  (if limit
    (take limit docs)
    docs))
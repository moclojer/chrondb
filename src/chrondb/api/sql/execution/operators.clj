(ns chrondb.api.sql.execution.operators
  "SQL operators for query execution"
  (:require [clojure.string :as str]
            [chrondb.util.logging :as log]))

(defn evaluate-condition
  "Evaluates a condition against a document.
   Parameters:
   - doc: The document to be evaluated
   - condition: The condition to be evaluated
   Returns: true if the condition is met, false otherwise"
  [doc condition]
  (let [field-val (get doc (keyword (:field condition)))
        cond-val (str/replace (:value condition) #"['\"]" "")
        operator (str/lower-case (:op condition))]
    (case operator
      "=" (= (str field-val) cond-val)
      "!=" (not= (str field-val) cond-val)
      "<>" (not= (str field-val) cond-val)
      ">" (> (compare (str field-val) cond-val) 0)
      "<" (< (compare (str field-val) cond-val) 0)
      ">=" (>= (compare (str field-val) cond-val) 0)
      "<=" (<= (compare (str field-val) cond-val) 0)
      "like" (re-find (re-pattern (str "(?i)" (str/replace cond-val #"%" ".*"))) (str field-val))
      ;; Default case
      (do
        (log/log-warn (str "Unsupported operator: " operator))
        false))))

(defn apply-where-conditions
  "Applies WHERE conditions to filter a collection of documents.
   Parameters:
   - docs: The documents to be filtered
   - conditions: The conditions to be applied
   Returns: Filtered documents"
  [docs conditions]
  (if (empty? conditions)
    docs
    (filter
     (fn [document]
       (every?
        (fn [condition]
          (let [field (keyword (:field condition))
                operator (str/lower-case (:op condition))
                value (str/replace (:value condition) #"['\"]" "")
                doc-value (str (get document field ""))]

            (case operator
              "=" (= doc-value value)
              "!=" (not= doc-value value)
              "<>" (not= doc-value value)
              ">" (> (compare doc-value value) 0)
              ">=" (>= (compare doc-value value) 0)
              "<" (< (compare doc-value value) 0)
              "<=" (<= (compare doc-value value) 0)
              "like" (let [pattern (str/replace value "%" ".*")]
                       (boolean (re-find (re-pattern (str "(?i)" pattern)) doc-value)))
              ;; Default: not matching
              (do
                (log/log-warn (str "Unsupported operator in where condition: " operator))
                false))))
        conditions))
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
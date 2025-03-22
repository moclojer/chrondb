(ns chrondb.api.sql.execution.operators
  "Implementation of SQL operators"
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
        operator (:op condition)]
    (case operator
      "=" (= (str field-val) cond-val)
      "!=" (not= (str field-val) cond-val)
      "<>" (not= (str field-val) cond-val)
      ">" (> (compare (str field-val) cond-val) 0)
      "<" (< (compare (str field-val) cond-val) 0)
      ">=" (>= (compare (str field-val) cond-val) 0)
      "<=" (<= (compare (str field-val) cond-val) 0)
      "like" (re-find (re-pattern (str/replace cond-val #"%" ".*")) (str field-val))
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
  (log/log-debug (str "Applying WHERE conditions: " conditions " to " (count docs) " documents"))
  (if (empty? conditions)
    docs
    (filter
     (fn [document]
       (log/log-debug (str "Checking document: " document))
       (every?
        (fn [condition]
          (let [field (keyword (:field condition))
                operator (:op condition)
                value (str/replace (:value condition) #"['\"]" "")
                doc-value (str (get document field ""))]

            (log/log-debug (str "Checking condition: " field " " operator " " value " against " doc-value))

            (case operator
              "=" (= doc-value value)
              "!=" (not= doc-value value)
              ">" (> (compare doc-value value) 0)
              ">=" (>= (compare doc-value value) 0)
              "<" (< (compare doc-value value) 0)
              "<=" (<= (compare doc-value value) 0)
              "LIKE" (let [pattern (str/replace value "%" ".*")]
                       (boolean (re-find (re-pattern pattern) doc-value)))
              ;; Default: not matching
              false)))
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
  "Applies a LIMIT clause to a collection of documents.
   Parameters:
   - docs: The documents to be limited
   - limit: The maximum number of documents to return
   Returns: Limited documents"
  [docs limit]
  (if limit
    (take limit docs)
    docs))
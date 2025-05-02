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
    (do
      (log/log-info "No conditions to apply, returning all docs")
      docs)
    (do
      (log/log-info (str "Applying filter conditions to " (count docs) " documents"))

      (filter
       (fn [document]
         ;; Check each document against all conditions
         (every?
          (fn [condition]
            (let [field (keyword (:field condition))
                  operator (str/lower-case (:op condition))
                  value (str/replace (:value condition) #"['\"]" "")
                  doc-value (str (get document field ""))

                  ;; Log para ID fields
                  _ (when (= (name field) "id")
                      (log/log-info (str "Comparing ID: document=" doc-value ", searched value=" value
                                         ", pure document ID=" (get document :id)
                                         ", _table=" (get document :_table))))

                  ;; ID field special handling
                  is-id-field (= (name field) "id")
                  numeric-value? (re-matches #"^\d+$" value)
                  numeric-doc-value? (re-matches #"^\d+$" doc-value)

                  ;; Get pure document ID
                  pure-doc-id (str (get document :id))

                  ;; Improved ID matching logic - more exact matches
                  id-match? (if is-id-field
                              (cond
                                ;; 1. Direct match: documento exato igual (mais importante)
                                (= doc-value value)
                                (do (log/log-info "✅ MATCH - Exact ID match") true)

                                ;; 2. Exact ID pure match: quando o ID puro é exatamente o valor
                                (= pure-doc-id value)
                                (do (log/log-info "✅ MATCH - Pure ID exact match") true)

                                ;; Se estamos buscando por um valor que parece ser um número puro
                                numeric-value?
                                (cond
                                  ;; 3. Se ambos são numéricos, comparar como números
                                  numeric-doc-value?
                                  (let [match (= (Double/parseDouble doc-value) (Double/parseDouble value))]
                                    (when match (log/log-info "✅ MATCH - Numeric ID match"))
                                    match)

                                  ;; 4. Se o valor termina com exatamente ":N" (ex: "user:1")
                                  ;; e estamos procurando pelo número puro, isso só deve ser considerado
                                  ;; se não há resultados EXATOS, e não para esta lógica individual
                                  (re-find (re-pattern (str ":" value "$")) doc-value)
                                  (do (log/log-info "❓ CONDITIONAL MATCH - ID ends with value, low priority")
                                      false)

                                  ;; 5. Não são números comparáveis ou não termina com o número
                                  :else
                                  (do (log/log-info "❌ NO MATCH - numeric mismatch") false))

                                ;; 6. Se não for nenhum dos casos acima, não há correspondência
                                :else
                                (do (log/log-info "❌ NO MATCH - default no match") false))
                              false)

                  ;; Final comparison result
                  result (case operator
                           "=" (if is-id-field
                                 id-match?
                                 (= doc-value value))
                           "!=" (not= doc-value value)
                           "<>" (not= doc-value value)
                           ">" (try
                                 ;; Try to convert both sides to numbers if they look like numbers
                                 (if (and (re-matches #"-?\d+(\.\d+)?" value)
                                          (re-matches #"-?\d+(\.\d+)?" doc-value))
                                   (> (Double/parseDouble doc-value) (Double/parseDouble value))
                                   ;; Fall back to string comparison if they're not numeric
                                   (> (compare doc-value value) 0))
                                 (catch Exception e
                                   (log/log-warn (str "Error in numeric comparison: " (.getMessage e)))
                                   (> (compare doc-value value) 0)))
                           ">=" (try
                                  ;; Try to convert both sides to numbers if they look like numbers
                                  (if (and (re-matches #"-?\d+(\.\d+)?" value)
                                           (re-matches #"-?\d+(\.\d+)?" doc-value))
                                    (>= (Double/parseDouble doc-value) (Double/parseDouble value))
                                    ;; Fall back to string comparison if they're not numeric
                                    (>= (compare doc-value value) 0))
                                  (catch Exception e
                                    (log/log-warn (str "Error in numeric comparison: " (.getMessage e)))
                                    (>= (compare doc-value value) 0)))
                           "<" (try
                                 ;; Try to convert both sides to numbers if they look like numbers
                                 (if (and (re-matches #"-?\d+(\.\d+)?" value)
                                          (re-matches #"-?\d+(\.\d+)?" doc-value))
                                   (< (Double/parseDouble doc-value) (Double/parseDouble value))
                                   ;; Fall back to string comparison if they're not numeric
                                   (< (compare doc-value value) 0))
                                 (catch Exception e
                                   (log/log-warn (str "Error in numeric comparison: " (.getMessage e)))
                                   (< (compare doc-value value) 0)))
                           "<=" (try
                                  ;; Try to convert both sides to numbers if they look like numbers
                                  (if (and (re-matches #"-?\d+(\.\d+)?" value)
                                           (re-matches #"-?\d+(\.\d+)?" doc-value))
                                    (<= (Double/parseDouble doc-value) (Double/parseDouble value))
                                    ;; Fall back to string comparison if they're not numeric
                                    (<= (compare doc-value value) 0))
                                  (catch Exception e
                                    (log/log-warn (str "Error in numeric comparison: " (.getMessage e)))
                                    (<= (compare doc-value value) 0)))
                           "like" (let [pattern (str/replace value "%" ".*")]
                                    (boolean (re-find (re-pattern (str "(?i)" pattern)) doc-value)))
                           ;; Default: not matching
                           (do
                             (log/log-warn (str "Unsupported operator in where condition: " operator))
                             false))]

              ;; Log final result
              (log/log-info (str "Condition " field " " operator " " value
                                 " for document " (:id document) ": "
                                 (if result "✅ ACCEPTED" "❌ REJECTED")))

              result))
          conditions))
       docs))))

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
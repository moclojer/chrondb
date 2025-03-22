(ns chrondb.api.sql.execution.functions
  "Implementation of SQL functions such as aggregations"
  (:require [chrondb.util.logging :as log]))

(defn execute-aggregate-function
  "Executes an aggregate function on a collection of documents.
   Parameters:
   - function: The aggregate function to execute (:count, :sum, :avg, :min, :max)
   - docs: The documents to operate on
   - field: The field to aggregate
   Returns: The result of the aggregate function"
  [function docs field]
  (case function
    :count (count docs)
    :sum (reduce + (keep #(get % (keyword field)) docs))
    :avg (let [values (keep #(get % (keyword field)) docs)]
           (if (empty? values)
             0
             (/ (reduce + values) (count values))))
    :min (apply min (keep #(get % (keyword field)) docs))
    :max (apply max (keep #(get % (keyword field)) docs))
    ;; Default case
    (do
      (log/log-warn (str "Unsupported aggregate function: " function))
      nil)))
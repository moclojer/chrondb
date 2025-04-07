(ns chrondb.api.sql.execution.functions
  "Implementation of SQL functions such as aggregations"
  (:require [chrondb.util.logging :as log]))

(defn process-aggregate-result
  "Formats an aggregate result to be displayed to the user"
  [function result field]
  (try
    (let [_ (log/log-info (str "Processing aggregate result - Function: " function
                               ", Result: " result
                               ", Field: " field))
          safe-result (if (nil? result)
                        (case function
                          :count 0
                          :sum 0
                          :avg 0
                          nil)
                        result)
          col-name (str (name function) "_" field)
          _ (log/log-info (str "Formatted aggregate result: " col-name " = " safe-result))]
      {(keyword col-name) safe-result})
    (catch Exception e
      (log/log-error (str "Erro ao processar resultado de agregação: " (.getMessage e)
                          "\n" (.printStackTrace e)))
      {:error "Erro ao processar resultado de agregação"})))

(defn execute-aggregate-function
  "Executes an aggregate function on a collection of documents.
   Parameters:
   - function: The aggregate function to execute (:count, :sum, :avg, :min, :max)
   - docs: The documents to operate on
   - field: The field to aggregate
   Returns: The result of the aggregate function"
  [function docs field]
  (try
    (log/log-info (str "Executing aggregate function: " function " on field: " field
                       ", Docs count: " (count docs)))
    (let [all-values (mapv #(get % (keyword field)) docs)
          _ (log/log-info (str "All values for field '" field "': " all-values))

          ;; Extract numeric part from field values (useful for IDs with prefixes like "user:1")
          numeric-values (keep (fn [v]
                                 (try
                                   (if (string? v)
                                     ; Handle both plain IDs and prefixed IDs
                                     (cond
                                       ; Try to extract numeric part from prefixed IDs like "user:1"
                                       (re-find #".*:(\d+)$" v)
                                       (Double/parseDouble (second (re-find #".*:(\d+)$" v)))

                                       ; Try to parse as plain numeric ID
                                       (re-matches #"^\d+(\.\d+)?$" v)
                                       (Double/parseDouble v)

                                       :else nil)
                                     (when (number? v) v))
                                   (catch Exception _
                                     nil)))
                               all-values)
          _ (log/log-info (str "Extracted numeric values: " numeric-values))]

      (case function
        :count (if (= field "*")
                 (count docs)  ;; For count(*), count all documents
                 (count (filter some? all-values)))  ;; For count(field), count only non-null values

        :sum (if (empty? numeric-values)
               0
               (reduce + numeric-values))

        :avg (if (empty? numeric-values)
               0
               (/ (reduce + numeric-values) (count numeric-values)))

        :min (if (empty? numeric-values)
               nil
               (apply min numeric-values))

        :max (if (empty? numeric-values)
               nil
               (apply max numeric-values))

        ;; Default case
        (do
          (log/log-warn (str "Unsupported aggregate function: " function))
          nil)))
    (catch Exception e
      (log/log-error (str "Erro ao executar função de agregação: " (.getMessage e)))
      (let [sw (java.io.StringWriter.)
            pw (java.io.PrintWriter. sw)]
        (.printStackTrace e pw)
        (log/log-error (.toString sw)))
      nil)))
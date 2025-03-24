(ns chrondb.api.sql.parser.clauses
  "Functions for parsing specific SQL clauses (WHERE, GROUP BY, etc.)"
  (:require [clojure.string :as str]
            [chrondb.api.sql.protocol.constants :as constants]
            [chrondb.api.sql.parser.tokenizer :as tokenizer]))

(defn parse-select-columns
  "Parses the column list of a SELECT query.
   Parameters:
   - tokens: The sequence of query tokens
   - from-index: The index of the FROM keyword
   Returns: A sequence of column specifications"
  [tokens from-index]
  (let [column-tokens (subvec tokens 1 from-index)]
    (loop [remaining column-tokens
           columns []]
      (if (empty? remaining)
        columns
        (let [token (str/lower-case (first remaining))]
          (cond
            ;; All columns
            (= token "*")
            (recur (rest remaining)
                   (conj columns {:type :all}))

            ;; Aggregate function with * as argument (e.g. count(*))
            (and (constants/AGGREGATE_FUNCTIONS token)
                 (>= (count remaining) 4)
                 (= (nth remaining 1) "(")
                 (= (nth remaining 2) "*")
                 (= (nth remaining 3) ")"))
            (recur (drop 4 remaining)
                   (conj columns {:type :aggregate-function
                                  :function (keyword token)
                                  :args ["*"]}))

            ;; Aggregate function with specific field
            (and (constants/AGGREGATE_FUNCTIONS token)
                 (>= (count remaining) 4)
                 (= (nth remaining 1) "(")
                 (not= (nth remaining 2) "*")
                 (= (nth remaining 3) ")"))
            (recur (drop 4 remaining)
                   (conj columns {:type :aggregate-function
                                  :function (keyword token)
                                  :args [(nth remaining 2)]}))

            ;; Column with alias (AS)
            (and (>= (count remaining) 3)
                 (= (str/lower-case (nth remaining 1)) "as"))
            (recur (drop 3 remaining)
                   (conj columns {:type :column
                                  :column token
                                  :alias (nth remaining 2)}))

            ;; Simple column
            :else
            (recur (rest remaining)
                   (conj columns {:type :column
                                  :column token}))))))))

(defn parse-where-condition
  "Parses a WHERE clause.
   Parameters:
   - tokens: The sequence of query tokens
   - where-index: The index of the WHERE keyword
   - end-index: The index where the parsing should end
   Returns: A sequence of condition specifications"
  [tokens where-index end-index]
  (if (or (nil? where-index) (>= where-index end-index))
    nil
    (let [where-tokens (subvec tokens (inc where-index) end-index)]
      (loop [remaining where-tokens
             conditions []]
        (if (< (count remaining) 3)
          conditions
          (let [field (first remaining)
                op (str/lower-case (second remaining))
                value (nth remaining 2)]
            (if (constants/COMPARISON_OPERATORS op)
              (let [condition {:field field, :op op, :value value}]
                (if (> (count remaining) 3)
                  (let [next-token (str/lower-case (nth remaining 3))]
                    (if (constants/LOGICAL_OPERATORS next-token)
                      (recur (drop 4 remaining) (conj conditions condition))
                      (conj conditions condition)))
                  (conj conditions condition)))
              conditions)))))))

(defn parse-group-by
  "Parses a GROUP BY clause.
   Parameters:
   - tokens: The sequence of query tokens
   - group-index: The index of the GROUP keyword
   - end-index: The index where the parsing should end
   Returns: A sequence of fields to group by"
  [tokens group-index end-index]
  (if (or (nil? group-index) (>= group-index end-index) (< end-index (+ group-index 2)))
    nil
    (let [by-index (tokenizer/find-token-index tokens "by" group-index end-index)]
      (if (and by-index (= (inc group-index) by-index))
        (let [group-tokens (subvec tokens (inc by-index) end-index)]
          (loop [remaining group-tokens
                 fields []]
            (if (empty? remaining)
              fields
              (recur (rest remaining)
                     (conj fields {:column (first remaining)})))))
        nil))))

(defn parse-order-by
  "Parses an ORDER BY clause.
   Parameters:
   - tokens: The sequence of query tokens
   - order-index: The index of the ORDER keyword
   - end-index: The index where the parsing should end
   Returns: A sequence of order specifications"
  [tokens order-index end-index]
  (if (or (nil? order-index) (>= order-index end-index) (< end-index (+ order-index 2)))
    nil
    (let [by-index (tokenizer/find-token-index tokens "by" order-index end-index)]
      (if (and by-index (= (inc order-index) by-index))
        (let [order-tokens (subvec tokens (inc by-index) end-index)]
          (loop [remaining order-tokens
                 clauses []]
            (if (empty? remaining)
              clauses
              (if (>= (count remaining) 2)
                (let [column (first remaining)
                      dir-token (str/lower-case (second remaining))
                      dir (if (= dir-token "desc") :desc :asc)]
                  (if (or (= dir-token "asc") (= dir-token "desc"))
                    (recur (drop 2 remaining)
                           (conj clauses {:column column, :direction dir}))
                    (recur (rest remaining)
                           (conj clauses {:column column, :direction :asc}))))
                (recur [] (conj clauses {:column (first remaining), :direction :asc}))))))
        nil))))
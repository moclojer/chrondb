(ns chrondb.api.sql.parser.clauses
  "Functions for parsing specific SQL clauses (WHERE, GROUP BY, etc.)"
  (:require [clojure.string :as str]
            [chrondb.api.sql.protocol.constants :as constants]
            [chrondb.api.sql.parser.tokenizer :as tokenizer]
            [chrondb.util.logging :as log]))

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
  "Parses a WHERE clause, recognizing standard operators, FTS_MATCH, and @@ to_tsquery.
   Parameters:
   - tokens: The sequence of query tokens
   - where-index: The index of the WHERE keyword
   - end-index: The index marking the END of the WHERE clause tokens (exclusive).
   Returns: A sequence of condition specifications, including :type key."
  [tokens where-index end-index]
  (if (or (nil? where-index) (>= where-index end-index))
    nil
    (let [where-tokens (subvec tokens (inc where-index) end-index)]
      (log/log-info (str "Parsing WHERE tokens: " (pr-str where-tokens)))
      (loop [remaining where-tokens
             conditions []]
        (if (empty? remaining)
          conditions
          (let [token1 (first remaining)]
            (cond
              ;; Check for logical operators (AND/OR) - should be preceded by a condition
              (and (constants/LOGICAL_OPERATORS (str/lower-case token1))
                   (seq conditions))
              (recur (rest remaining) conditions) ; Skip AND/OR for now, assuming implicit AND

              ;; Check for FTS_MATCH(field, query)
              (and (= (str/lower-case token1) "fts_match")
                   (>= (count remaining) 6) ; FTS_MATCH ( field , query )
                   (= (second remaining) "(")
                   (= (nth remaining 3) ",")
                   (= (nth remaining 5) ")"))
              (let [field-name (nth remaining 2)
                    query-val (nth remaining 4)
                    condition {:type :fts-match :field field-name :query query-val}]
                (log/log-info (str "Found FTS_MATCH condition: " (pr-str condition)))
                (recur (drop 6 remaining) (conj conditions condition)))

              ;; Check for field @@ to_tsquery('query')
              (and (>= (count remaining) 4) ; Mínimo: field @@ to_tsquery(
                   (= (second remaining) "@@"))
              (let [field-name token1
                    remaining-tokens (drop 2 remaining) ; Avançar além do @@
                    to_tsquery-prefix (first remaining-tokens)]
                (if (str/starts-with? (str/lower-case to_tsquery-prefix) "to_tsquery")
                  (let [;; Encontrar todos os tokens até fechar o parêntese
                        to_tsquery-tokens (take-while #(not= ")" %) (rest remaining-tokens))
                        end-paren-pos (+ (count to_tsquery-tokens) 1) ; +1 para o próprio to_tsquery

                        ;; Extrair a string da consulta - procurar entre aspas simples ou duplas
                        query-str (str/join " " to_tsquery-tokens)
                        _ (log/log-info (str "to_tsquery raw: " query-str))
                        quoted-match (re-find #"['\"]([^'\"]+)['\"]" query-str)
                        query-val (if (and quoted-match (> (count quoted-match) 1))
                                    (second quoted-match)
                                    ;; Fallback para o caso de não encontrar aspas
                                    query-str)

                        ;; Construir a condition com o query real e o value completo para referência
                        condition {:type :fts-match
                                   :field field-name
                                   :value (str "to_tsquery('" query-val "')")
                                   :query query-val}]
                    (log/log-info (str "Found to_tsquery condition: " (pr-str condition)))
                    (recur (drop (+ 2 end-paren-pos) remaining) (conj conditions condition)))
                  ;; Não é to_tsquery
                  (do (log/log-warn (str "Operador @@ seguido de algo diferente de to_tsquery: " to_tsquery-prefix))
                      (recur (rest remaining) conditions))))

              ;; Check for NOT IN (field NOT IN (val1, val2, ...))
              (and (>= (count remaining) 5)
                   (= (str/lower-case (second remaining)) "not")
                   (= (str/lower-case (nth remaining 2)) "in")
                   (= (nth remaining 3) "("))
              (let [field-name token1
                    ;; Find values between parentheses
                    rest-after-paren (drop 4 remaining)
                    values-and-rest (split-with #(not= ")" %) rest-after-paren)
                    value-tokens (first values-and-rest)
                    ;; Filter out commas and clean values
                    values (->> value-tokens
                                (remove #(= "," %))
                                (mapv str))
                    ;; Skip past closing paren
                    remaining-after (rest (second values-and-rest))
                    condition {:type :not-in :field field-name :values values}]
                (log/log-info (str "Found NOT IN condition: " (pr-str condition)))
                (recur remaining-after (conj conditions condition)))

              ;; Check for IN (field IN (val1, val2, ...))
              (and (>= (count remaining) 4)
                   (= (str/lower-case (second remaining)) "in")
                   (= (nth remaining 2) "("))
              (let [field-name token1
                    ;; Find values between parentheses
                    rest-after-paren (drop 3 remaining)
                    values-and-rest (split-with #(not= ")" %) rest-after-paren)
                    value-tokens (first values-and-rest)
                    ;; Filter out commas and clean values
                    values (->> value-tokens
                                (remove #(= "," %))
                                (mapv str))
                    ;; Skip past closing paren
                    remaining-after (rest (second values-and-rest))
                    condition {:type :in :field field-name :values values}]
                (log/log-info (str "Found IN condition: " (pr-str condition)))
                (recur remaining-after (conj conditions condition)))

              ;; Check for BETWEEN (field BETWEEN val1 AND val2)
              (and (>= (count remaining) 5)
                   (= (str/lower-case (second remaining)) "between"))
              (let [field-name token1
                    lower-val (nth remaining 2)
                    ;; Find AND keyword position
                    and-idx (first (keep-indexed
                                    (fn [idx tok]
                                      (when (= (str/lower-case tok) "and") idx))
                                    (drop 3 remaining)))
                    upper-val (when and-idx (nth (drop 3 remaining) (inc and-idx)))
                    ;; Calculate how many tokens to skip
                    tokens-to-skip (if and-idx (+ 5 and-idx) 5)
                    condition {:type :between :field field-name :lower lower-val :upper upper-val}]
                (log/log-info (str "Found BETWEEN condition: " (pr-str condition)))
                (recur (drop tokens-to-skip remaining) (conj conditions condition)))

              ;; Check for IS NOT NULL (field IS NOT NULL)
              (and (>= (count remaining) 4)
                   (= (str/lower-case (second remaining)) "is")
                   (= (str/lower-case (nth remaining 2)) "not")
                   (= (str/lower-case (nth remaining 3)) "null"))
              (let [field-name token1
                    condition {:type :is-not-null :field field-name}]
                (log/log-info (str "Found IS NOT NULL condition: " (pr-str condition)))
                (recur (drop 4 remaining) (conj conditions condition)))

              ;; Check for IS NULL (field IS NULL)
              (and (>= (count remaining) 3)
                   (= (str/lower-case (second remaining)) "is")
                   (= (str/lower-case (nth remaining 2)) "null"))
              (let [field-name token1
                    condition {:type :is-null :field field-name}]
                (log/log-info (str "Found IS NULL condition: " (pr-str condition)))
                (recur (drop 3 remaining) (conj conditions condition)))

              ;; Check for standard condition (field op value)
              (>= (count remaining) 3)
              (let [field token1 ; Corrected: use token1 as field
                    op (str/lower-case (second remaining))
                    value (nth remaining 2)]
                (if (constants/COMPARISON_OPERATORS op)
                  (let [condition {:type :standard :field field :op op :value value}]
                    (log/log-info (str "Found standard condition: " (pr-str condition)))
                    (recur (drop 3 remaining) (conj conditions condition)))
                  ;; Not a standard operator after field, maybe invalid syntax?
                  (do (log/log-warn (str "Sintaxe WHERE inválida perto de: " field ", op: " op))
                      (recur (rest remaining) conditions)))) ; Skip first token

              ;; Not enough tokens for any valid condition or invalid start
              :else
              (do (log/log-warn (str "Token inesperado na cláusula WHERE: " token1))
                  (recur (rest remaining) conditions)))))))))

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

(defn parse-join-condition
  "Parses a JOIN ON condition from a query.
   Parameters:
   - tokens: The sequence of query tokens
   - start-index: The index to start parsing from (after the ON keyword)
   - end-index: The index marking the end of the ON clause
   Returns: A map representing the join condition with :left-table, :left-field, :right-table, :right-field"
  [tokens start-index end-index]
  (when (and start-index (< start-index end-index))
    (let [condition-tokens (subvec tokens start-index end-index)
          condition-str (str/join " " condition-tokens)
          equals-pattern #"([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)\s*=\s*([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)"
          matcher (re-find equals-pattern condition-str)]

      (when (and matcher (= (count matcher) 5))
        (let [[_ left-table left-field right-table right-field] matcher]
          {:left-table left-table
           :left-field left-field
           :right-table right-table
           :right-field right-field})))))
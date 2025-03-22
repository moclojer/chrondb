(ns chrondb.api.sql.parser.clauses
  "Funções para parseamento de cláusulas SQL específicas (WHERE, GROUP BY, etc.)"
  (:require [clojure.string :as str]
            [chrondb.api.sql.protocol.constants :as constants]
            [chrondb.api.sql.parser.tokenizer :as tokenizer]))

(defn parse-select-columns
  "Analisa a lista de colunas de uma consulta SELECT.
   Parâmetros:
   - tokens: A sequência de tokens da consulta
   - from-index: O índice da palavra-chave FROM
   Retorna: Uma sequência de especificações de colunas"
  [tokens from-index]
  (let [column-tokens (subvec tokens 1 from-index)]
    (loop [remaining column-tokens
           columns []]
      (if (empty? remaining)
        columns
        (let [token (str/lower-case (first remaining))]
          (cond
            ;; Função de agregação (count, sum, etc.)
            (and (re-matches #"\w+\(.*\)" token)
                 (constants/AGGREGATE_FUNCTIONS (first (str/split token #"\("))))
            (let [[fn-name args] (str/split token #"\(|\)")
                  args (str/split args #",")
                  args (map str/trim args)]
              (recur (rest remaining)
                     (conj columns {:type :aggregate-function
                                    :function (keyword fn-name)
                                    :args args})))

            ;; Estrela (* - todas as colunas)
            (= token "*")
            (recur (rest remaining) (conj columns {:type :all}))

            ;; Coluna com alias (coluna as alias)
            (and (> (count remaining) 2)
                 (= (str/lower-case (second remaining)) "as"))
            (recur (drop 3 remaining)
                   (conj columns {:type :column
                                  :column (first remaining)
                                  :alias (nth remaining 2)}))

            ;; Coluna normal
            :else
            (recur (rest remaining)
                   (conj columns {:type :column
                                  :column token}))))))))

(defn parse-where-condition
  "Analisa a condição WHERE de uma consulta SQL.
   Parâmetros:
   - tokens: A sequência de tokens da consulta
   - where-index: O índice da palavra-chave WHERE
   - end-index: O índice onde a cláusula WHERE termina
   Retorna: Uma sequência de especificações de condição"
  [tokens where-index end-index]
  ;; Verifica se where-index é nil ou inválido
  (if (or (nil? where-index)
          (< where-index 0)
          (>= where-index (count tokens))
          (>= where-index end-index))
    nil  ;; Se inválido, retorna nil (sem condições)
    ;; Se válido, processa as condições
    (let [condition-tokens (subvec tokens (inc where-index) end-index)]
      (loop [remaining condition-tokens
             conditions []
             current-condition {}
             current-logical nil]
        (if (empty? remaining)
          ;; Quando terminamos de processar todos os tokens
          (if (empty? current-condition)
            conditions
            (conj conditions current-condition))
          ;; Processa o próximo token
          (let [token (str/lower-case (first remaining))]
            (cond
              ;; Operador lógico (AND, OR, NOT)
              (constants/LOGICAL_OPERATORS token)
              (recur (rest remaining)
                     (if (empty? current-condition)
                       conditions
                       (conj conditions current-condition))
                     {}
                     token)

              ;; Início de nova condição com operador lógico atual
              (and (seq current-logical)
                   (empty? current-condition))
              (recur (rest remaining)
                     conditions
                     {:field token
                      :logical current-logical}
                     current-logical)

              ;; Campo da condição
              (empty? current-condition)
              (recur (rest remaining)
                     conditions
                     {:field token}
                     current-logical)

              ;; Operador para condição
              (and (contains? current-condition :field)
                   (not (contains? current-condition :op)))
              (recur (rest remaining)
                     conditions
                     (assoc current-condition :op token)
                     current-logical)

              ;; Valor para condição
              (and (contains? current-condition :field)
                   (contains? current-condition :op)
                   (not (contains? current-condition :value)))
              (recur (rest remaining)
                     conditions
                     (assoc current-condition :value token)
                     current-logical)

              ;; Caso padrão
              :else
              (recur (rest remaining)
                     conditions
                     current-condition
                     current-logical))))))))

(defn parse-group-by
  "Analisa a cláusula GROUP BY de uma consulta SQL.
   Parâmetros:
   - tokens: A sequência de tokens da consulta
   - group-index: O índice da palavra-chave GROUP
   - end-index: O índice onde a cláusula GROUP BY termina
   Retorna: Uma sequência de colunas de agrupamento ou nil se não houver GROUP BY"
  [tokens group-index end-index]
  (if (or (nil? group-index) (< group-index 0) (>= group-index end-index))
    nil
    (let [by-index (+ group-index 1)]
      (if (and (< by-index end-index)
               (= (str/lower-case (nth tokens by-index)) "by"))
        (let [columns (subvec tokens (+ by-index 1) end-index)]
          (mapv (fn [col] {:column col}) columns))
        nil))))

(defn parse-order-by
  "Analisa a cláusula ORDER BY de uma consulta SQL.
   Parâmetros:
   - tokens: A sequência de tokens da consulta
   - order-index: O índice da palavra-chave ORDER
   - end-index: O índice onde a cláusula ORDER BY termina
   Retorna: Uma sequência de especificações de ordenação ou nil se não houver ORDER BY"
  [tokens order-index end-index]
  (if (or (nil? order-index) (< order-index 0) (>= order-index end-index))
    nil
    (let [by-index (+ order-index 1)]
      (if (and (< by-index end-index)
               (= (str/lower-case (nth tokens by-index)) "by"))
        (let [order-tokens (subvec tokens (+ by-index 1) end-index)]
          (loop [remaining order-tokens
                 orders []]
            (if (empty? remaining)
              orders
              (let [col (first remaining)
                    direction (if (and (> (count remaining) 1)
                                       (or (= (str/lower-case (second remaining)) "asc")
                                           (= (str/lower-case (second remaining)) "desc")))
                                (keyword (str/lower-case (second remaining)))
                                :asc)
                    next-pos (if (= direction :asc) 1 2)]
                (recur (drop next-pos remaining)
                       (conj orders {:column col
                                     :direction direction}))))))
        nil))))
(ns chrondb.api.sql.parser.tokenizer
  "Funções para tokenização de consultas SQL"
  (:require [clojure.string :as str]))

(defn tokenize-sql
  "Tokeniza uma string de consulta SQL em tokens individuais.
   Parâmetros:
   - sql: A string de consulta SQL
   Retorna: Uma sequência de tokens"
  [sql]
  (-> sql
      (str/replace #"([(),;=<>])" " $1 ")
      (str/replace #"\s+" " ")
      (str/trim)
      (str/split #"\s+")))

(defn find-token-index
  "Encontra o índice do primeiro token que corresponde a qualquer uma das palavras-chave fornecidas.
   Parâmetros:
   - tokens: A sequência de tokens para pesquisa
   - keywords: As palavras-chave para pesquisar
   Retorna: O índice do primeiro token correspondente, ou nil se não for encontrado"
  [tokens & keywords]
  (let [keyword-set (set (map str/lower-case keywords))]
    (->> tokens
         (map-indexed (fn [idx token] [idx (str/lower-case token)]))
         (filter (fn [[_ token]] (keyword-set token)))
         (first)
         (first))))
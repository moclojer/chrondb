(ns chrondb.api.sql.parser.tokenizer
  "Functions for tokenizing SQL queries"
  (:require [clojure.string :as str]))

(defn tokenize-sql
  "Tokenizes a SQL query string into individual tokens.
   Parameters:
   - sql: The SQL query string
   Returns: A sequence of tokens"
  [sql]
  (-> sql
      (str/replace #"([(),;=<>])" " $1 ")
      (str/replace #"\s+" " ")
      (str/trim)
      (str/split #"\s+")))

(defn find-token-index
  "Finds the index of the first token that matches any of the provided keywords.
   Parameters:
   - tokens: The sequence of tokens to search
   - keywords: The keywords to search for
   Returns: The index of the first matching token, or nil if not found"
  [tokens & keywords]
  (let [keyword-set (set (map str/lower-case keywords))]
    (->> tokens
         (map-indexed (fn [idx token] [idx (str/lower-case token)]))
         (filter (fn [[_ token]] (keyword-set token)))
         (first)
         (first))))
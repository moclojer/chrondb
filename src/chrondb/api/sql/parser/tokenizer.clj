(ns chrondb.api.sql.parser.tokenizer
  "Functions for tokenizing SQL queries"
  (:require [clojure.string :as str]))

(defn tokenize-sql
  "Tokenizes a SQL query string into individual tokens.
   Handles quoted strings (both single and double quotes) as single tokens.
   Parameters:
   - sql: The SQL query string
   Returns: A sequence of tokens"
  [sql]
  (let [state (atom {:tokens []
                     :current-token ""
                     :in-single-quote false
                     :in-double-quote false
                     :escaped false})]

    ;; Process each character in the SQL string
    (doseq [ch sql]
      (let [{:keys [current-token in-single-quote in-double-quote escaped]} @state]
        (cond
          ;; Handle escape character
          (and (= ch \\) (not escaped))
          (swap! state assoc :escaped true)

          ;; Handle single quote
          (and (= ch \') (not escaped) (not in-double-quote))
          (swap! state assoc
                 :in-single-quote (not in-single-quote)
                 :current-token (str current-token ch)
                 :escaped false)

          ;; Handle double quote
          (and (= ch \") (not escaped) (not in-single-quote))
          (swap! state assoc
                 :in-double-quote (not in-double-quote)
                 :current-token (str current-token ch)
                 :escaped false)

          ;; Inside quotes - append character to current token
          (or in-single-quote in-double-quote)
          (swap! state assoc
                 :current-token (str current-token ch)
                 :escaped false)

          ;; Handle separators outside quotes
          (re-matches #"[\s(),;=<>]" (str ch))
          (do
            ;; If we have a current token, add it to tokens
            (when (not (str/blank? current-token))
              (swap! state update :tokens conj current-token))

            ;; For significant separators (not whitespace), add them as tokens
            (when (not (re-matches #"\s" (str ch)))
              (swap! state update :tokens conj (str ch)))

            ;; Reset current token
            (swap! state assoc
                   :current-token ""
                   :escaped false))

          ;; Normal character - append to current token
          :else
          (swap! state assoc
                 :current-token (str current-token ch)
                 :escaped false))))

    ;; Add the final token if there is one
    (let [{:keys [tokens current-token]} @state]
      (if (not (str/blank? current-token))
        (conj tokens current-token)
        tokens))))

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

(defn find-token-index-from
  "Finds the index of the first token that matches the provided keyword,
   starting from a specific index in the token sequence.
   Parameters:
   - tokens: The sequence of tokens to search
   - keyword: The keyword to search for
   - start-index: The index to start the search from
   Returns: The index of the first matching token, or nil if not found"
  [tokens keyword start-index]
  (let [keyword-lower (str/lower-case keyword)]
    (->> tokens
         (drop start-index)
         (map-indexed (fn [idx token] [(+ idx start-index) (str/lower-case token)]))
         (filter (fn [[_ token]] (= token keyword-lower)))
         (first)
         (first))))
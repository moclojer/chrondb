(ns chrondb.api.sql.parser.statements
  "Functions for parsing complete SQL statements"
  (:require [clojure.string :as str]
            [chrondb.api.sql.parser.tokenizer :as tokenizer]
            [chrondb.api.sql.parser.clauses :as clauses]
            [chrondb.api.sql.parser.ddl :as ddl]))

(defn parse-select-query
  "Parses a SQL SELECT query.
   Parameters:
   - tokens: The sequence of query tokens
   Returns: A map representing the parsed query with fields for :type, :columns, :table etc."
  [tokens]
  (let [from-index (tokenizer/find-token-index tokens "from")
        where-index (tokenizer/find-token-index tokens "where")
        group-index (tokenizer/find-token-index tokens "group")
        order-index (tokenizer/find-token-index tokens "order")
        limit-index (tokenizer/find-token-index tokens "limit")
        join-index (tokenizer/find-token-index tokens "join")
        inner-join-index (let [inner-idx (tokenizer/find-token-index tokens "inner")]
                           (when (and inner-idx
                                      (< (inc inner-idx) (count tokens))
                                      (= (str/lower-case (nth tokens (inc inner-idx))) "join"))
                             inner-idx))
        left-join-index (let [left-idx (tokenizer/find-token-index tokens "left")]
                          (when (and left-idx
                                     (< (inc left-idx) (count tokens))
                                     (= (str/lower-case (nth tokens (inc left-idx))) "join"))
                            left-idx))

        ;; Determine where clauses end
        where-end (or group-index order-index limit-index (count tokens))
        group-end (or order-index limit-index (count tokens))
        order-end (or limit-index (count tokens))

        ;; Parse each clause
        columns (if from-index
                  (clauses/parse-select-columns tokens from-index)
                  [])

        ;; Handle base table (from clause)
        base-table-idx (when from-index (inc from-index))
        raw-base-table (when (and from-index base-table-idx (< base-table-idx (count tokens)))
                         (nth tokens base-table-idx))

        ;; Parse schema.table format for base table
        [schema base-table] (when raw-base-table
                              (let [cleaned-spec (-> raw-base-table
                                                     (str/replace #";$" "")
                                                     (str/trim))
                                    parts (str/split cleaned-spec #"\.")]
                                (if (= (count parts) 2)
                                  [(first parts) (str/trim (second parts))]
                                  [nil cleaned-spec])))

        ;; Parse join information if present
        join-start-idx (or join-index
                           (when inner-join-index (inc inner-join-index))
                           (when left-join-index (inc left-join-index)))

        join-info (when join-start-idx
                    (let [on-idx (tokenizer/find-token-index-from tokens "on" (inc join-start-idx))
                          join-table-raw (when (and join-start-idx on-idx (< (inc join-start-idx) on-idx))
                                           (nth tokens (inc join-start-idx)))
                          [join-schema join-table] (when join-table-raw
                                                     (let [cleaned-spec (-> join-table-raw
                                                                            (str/replace #";$" "")
                                                                            (str/trim))
                                                           parts (str/split cleaned-spec #"\.")]
                                                       (if (= (count parts) 2)
                                                         [(first parts) (str/trim (second parts))]
                                                         [nil cleaned-spec])))
                          on-clause-start (when on-idx (inc on-idx))
                          on-clause-end (or where-index group-index order-index limit-index (count tokens))
                          on-condition (when (and on-clause-start (< on-clause-start on-clause-end))
                                         (clauses/parse-join-condition tokens on-clause-start on-clause-end))]
                      {:type (cond
                               inner-join-index :inner-join
                               left-join-index :left
                               :else :join)
                       :schema join-schema
                       :table join-table
                       :on on-condition}))

        where-condition (clauses/parse-where-condition tokens where-index where-end)
        group-by (clauses/parse-group-by tokens group-index group-end)
        order-by (clauses/parse-order-by tokens order-index order-end)
        limit (when limit-index
                (try
                  (Integer/parseInt (nth tokens (inc limit-index)))
                  (catch Exception _ nil)))]

    {:type :select
     :columns columns
     :schema schema
     :table base-table
     :join join-info
     :where where-condition
     :group-by group-by
     :order-by order-by
     :limit limit}))

(defn parse-insert-query
  "Parses a SQL INSERT query.
   Parameters:
   - tokens: The sequence of query tokens
   Returns: A map representing the parsed query with fields :type, :table, :columns and :values"
  [tokens]
  (let [into-index (tokenizer/find-token-index tokens "into")
        values-index (tokenizer/find-token-index tokens "values")
        raw-table-spec (when (and into-index (> into-index 0))
                         (nth tokens (inc into-index)))
        ;; Parse schema.table format and remove semicolons and extra spaces
        [schema table-name] (when raw-table-spec
                              (let [cleaned-spec (-> raw-table-spec
                                                     (str/replace #";$" "")
                                                     (str/trim))
                                    parts (str/split cleaned-spec #"\.")]
                                (if (= (count parts) 2)
                                  [(first parts) (str/trim (second parts))]
                                  [nil cleaned-spec])))

        ;; Parse column names if provided
        columns-start (+ into-index 2)  ;; after INTO table (
        columns-end (- values-index 1)  ;; before )
        columns (when (and (< columns-start columns-end)
                           (= (nth tokens columns-start) "(")
                           (= (nth tokens columns-end) ")"))
                  ;; Filter out comma tokens
                  (->> (subvec tokens (inc columns-start) columns-end)
                       (remove #(= "," %))
                       (mapv str/trim)))

        ;; Parse values
        values-start (+ values-index 1)
        values-tokens (subvec tokens values-start)
        values (loop [remaining values-tokens
                      current-values []
                      in-parens false
                      current-group []]
                 (if (empty? remaining)
                   (if (empty? current-group)
                     current-values
                     (conj current-values current-group))
                   (let [token (first remaining)]
                     (cond
                       (= token "(") (recur (rest remaining) current-values true [])
                       (= token ")") (recur (rest remaining) (conj current-values current-group) false [])
                       ;; Skip commas inside parentheses
                       (and in-parens (= token ",")) (recur (rest remaining) current-values true current-group)
                       in-parens (recur (rest remaining) current-values true (conj current-group token))
                       :else (recur (rest remaining) current-values in-parens current-group)))))]

    {:type :insert
     :schema schema
     :table table-name
     :columns columns
     :values values}))

(defn parse-update-query
  "Parses a SQL UPDATE query.
   Parameters:
   - tokens: The sequence of query tokens
   Returns: A map representing the parsed query with fields :type, :table, :updates and :where"
  [tokens]
  (let [raw-table-spec (second tokens)
        ;; Parse schema.table format and remove semicolons and extra spaces
        [schema table-name] (when raw-table-spec
                              (let [cleaned-spec (-> raw-table-spec
                                                     (str/replace #";$" "")
                                                     (str/trim))
                                    parts (str/split cleaned-spec #"\.")]
                                (if (= (count parts) 2)
                                  [(first parts) (str/trim (second parts))]
                                  [nil cleaned-spec])))
        set-index (tokenizer/find-token-index tokens "set")
        where-index (tokenizer/find-token-index tokens "where")

        ;; Parse SET clause
        set-end (or where-index (count tokens))
        set-tokens (subvec tokens (inc set-index) set-end)
        updates (loop [remaining set-tokens
                       result {}
                       current-field nil]
                  (if (empty? remaining)
                    result
                    (let [token (first remaining)]
                      (cond
                        (nil? current-field)
                        (recur (rest remaining) result token)

                        (= token "=")
                        (recur (rest remaining) result current-field)

                        :else
                        (recur (rest remaining) (assoc result current-field token) nil)))))

        ;; Parse WHERE clause
        where-condition (when where-index
                          (clauses/parse-where-condition tokens where-index (count tokens)))]

    {:type :update
     :schema schema
     :table table-name
     :updates updates
     :where where-condition}))

(defn parse-delete-query
  "Parses a SQL DELETE query.
   Parameters:
   - tokens: The sequence of query tokens
   Returns: A map representing the parsed query with fields :type, :table and :where"
  [tokens]
  (let [from-index (tokenizer/find-token-index tokens "from")
        where-index (tokenizer/find-token-index tokens "where")
        raw-table-spec (when (and from-index (< from-index (count tokens)))
                         (nth tokens (inc from-index)))
        ;; Parse schema.table format and remove semicolons and extra spaces
        [schema table-name] (when raw-table-spec
                              (let [cleaned-spec (-> raw-table-spec
                                                     (str/replace #";$" "")
                                                     (str/trim))
                                    parts (str/split cleaned-spec #"\.")]
                                (if (= (count parts) 2)
                                  [(first parts) (str/trim (second parts))]
                                  [nil cleaned-spec])))
        where-condition (when where-index
                          (clauses/parse-where-condition tokens where-index (count tokens)))]

    {:type :delete
     :schema schema
     :table table-name
     :where where-condition}))

(defn parse-function-call
  "Parses a function call like chrondb_history, chrondb_at, etc.
   Parameters:
   - tokens: The sequence of query tokens starting with the function name
   Returns: A map representing the parsed function call or nil if not a recognized function"
  [tokens]
  (when (not-empty tokens)
    (let [func-name (str/lower-case (first tokens))]
      (when (and (> (count tokens) 3)  ;; Need at least 'func(' and ')' plus args
                 (= (second tokens) "(")
                 (.contains (str/lower-case func-name) "chrondb_"))
        (let [closing-paren-idx (tokenizer/find-matching-paren tokens 1)
              args-tokens (when closing-paren-idx
                            (subvec tokens 2 closing-paren-idx))
              args (when args-tokens
                     (->> args-tokens
                          (remove #(= "," %))
                          (mapv #(str/replace % #"['\"]" ""))))]
          (case func-name
            "chrondb_history"
            (when (= (count args) 2)
              {:type :chrondb-function
               :function :history
               :table (first args)
               :id (second args)})

            "chrondb_at"
            (when (= (count args) 3)
              {:type :chrondb-function
               :function :at
               :table (first args)
               :id (second args)
               :commit (nth args 2)})

            "chrondb_diff"
            (when (= (count args) 4)
              {:type :chrondb-function
               :function :diff
               :table (first args)
               :id (second args)
               :commit1 (nth args 2)
               :commit2 (nth args 3)})

            nil))))))

(defn parse-sql-query
  "Parses a SQL query string into an intermediate representation.
   Parameters:
   - query: The SQL query string
   Returns: A map representing the parsed query structure"
  [query]
  (let [tokens (tokenizer/tokenize-sql query)
        first-token (when (not-empty tokens) (str/lower-case (first tokens)))
        second-token (when (> (count tokens) 1) (str/lower-case (second tokens)))]
    (cond
      ;; DDL: CREATE TABLE
      (and (= first-token "create") (= second-token "table"))
      (ddl/parse-create-table tokens)

      ;; DDL: DROP TABLE
      (and (= first-token "drop") (= second-token "table"))
      (ddl/parse-drop-table tokens)

      ;; DDL: SHOW TABLES
      (and (= first-token "show") (= second-token "tables"))
      (ddl/parse-show-tables tokens)

      ;; DDL: SHOW SCHEMAS / SHOW DATABASES
      (and (= first-token "show")
           (or (= second-token "schemas")
               (= second-token "databases")))
      (ddl/parse-show-schemas tokens)

      ;; DDL: DESCRIBE table
      (= first-token "describe")
      (ddl/parse-describe tokens)

      ;; DDL: SHOW COLUMNS FROM table
      (and (= first-token "show") (= second-token "columns"))
      (ddl/parse-describe tokens)

      ;; ChronDB branch functions: SELECT * FROM chrondb_branch_list()
      (and (= first-token "select")
           (> (count tokens) 3)
           (= (str/lower-case (nth tokens 2)) "from")
           (let [function-name (str/lower-case (nth tokens 3))]
             (or (.startsWith function-name "chrondb_branch_list")
                 (.startsWith function-name "chrondb_branch_create")
                 (.startsWith function-name "chrondb_branch_checkout")
                 (.startsWith function-name "chrondb_branch_merge"))))
      (let [func-name (str/lower-case (nth tokens 3))]
        (cond
          (.startsWith func-name "chrondb_branch_list")
          (ddl/parse-branch-function tokens "chrondb_branch_list")

          (.startsWith func-name "chrondb_branch_create")
          (ddl/parse-branch-function tokens "chrondb_branch_create")

          (.startsWith func-name "chrondb_branch_checkout")
          (ddl/parse-branch-function tokens "chrondb_branch_checkout")

          (.startsWith func-name "chrondb_branch_merge")
          (ddl/parse-branch-function tokens "chrondb_branch_merge")

          :else
          {:type :unknown, :message (str "Unknown ChronDB branch function: " func-name)}))

      ;; Tratamento específico para as funções chrondb (history, at, diff)
      (and (= first-token "select")
           (> (count tokens) 3) ;; Precisa ter pelo menos SELECT * FROM FUNC
           (= (str/lower-case (nth tokens 2)) "from")
           (let [function-name (str/lower-case (nth tokens 3))]
             (or (.startsWith function-name "chrondb_history")
                 (.startsWith function-name "chrondb_at")
                 (.startsWith function-name "chrondb_diff"))))
      (let [func-name (str/lower-case (nth tokens 3))
            opening-paren-idx (tokenizer/find-token-index-from tokens "(" 4)
            closing-paren-idx (when opening-paren-idx
                                (tokenizer/find-matching-paren tokens opening-paren-idx))
            args (when (and opening-paren-idx closing-paren-idx)
                   (remove #(= "," %)
                           (subvec tokens (inc opening-paren-idx) closing-paren-idx)))]
        (cond
          ;; chrondb_history('table', 'id')
          (and (.startsWith func-name "chrondb_history")
               (= (count args) 2))
          {:type :chrondb-function
           :function :history
           :table (str/replace (first args) #"['\"]" "")
           :id (str/replace (second args) #"['\"]" "")}

          ;; chrondb_at('table', 'id', 'commit')
          (and (.startsWith func-name "chrondb_at")
               (= (count args) 3))
          {:type :chrondb-function
           :function :at
           :table (str/replace (first args) #"['\"]" "")
           :id (str/replace (second args) #"['\"]" "")
           :commit (str/replace (nth args 2) #"['\"]" "")}

          ;; chrondb_diff('table', 'id', 'commit1', 'commit2')
          (and (.startsWith func-name "chrondb_diff")
               (= (count args) 4))
          {:type :chrondb-function
           :function :diff
           :table (str/replace (first args) #"['\"]" "")
           :id (str/replace (second args) #"['\"]" "")
           :commit1 (str/replace (nth args 2) #"['\"]" "")
           :commit2 (str/replace (nth args 3) #"['\"]" "")}

          ;; Default - unknown function
          :else
          {:type :unknown, :message (str "Unknown ChronDB function: " func-name)}))

      (= first-token "select")
      (parse-select-query tokens)

      (= first-token "insert")
      (parse-insert-query tokens)

      (= first-token "update")
      (parse-update-query tokens)

      (= first-token "delete")
      (parse-delete-query tokens)

      :else
      {:type :unknown, :message "Unsupported SQL query type"})))
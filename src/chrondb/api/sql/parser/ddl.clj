(ns chrondb.api.sql.parser.ddl
  "Functions for parsing DDL (Data Definition Language) SQL statements"
  (:require [clojure.string :as str]
            [chrondb.api.sql.parser.tokenizer :as tokenizer]))

(defn- parse-column-type
  "Parses a column type token into a normalized type.
   Parameters:
   - token: The type token (e.g., 'TEXT', 'INTEGER', 'TIMESTAMP')
   Returns: Normalized type string"
  [token]
  (when token
    (str/upper-case (str/trim token))))

(defn- parse-column-constraints
  "Parses column constraints from a sequence of tokens.
   Parameters:
   - tokens: Tokens after the type definition
   Returns: Map of constraints {:primary-key bool :not-null bool :default value}"
  [tokens]
  (loop [remaining tokens
         constraints {}]
    (if (or (empty? remaining)
            (= "," (first remaining))
            (= ")" (first remaining)))
      constraints
      (let [token (str/lower-case (first remaining))]
        (cond
          ;; PRIMARY KEY
          (and (= token "primary")
               (>= (count remaining) 2)
               (= (str/lower-case (second remaining)) "key"))
          (recur (drop 2 remaining) (assoc constraints :primary-key true))

          ;; NOT NULL
          (and (= token "not")
               (>= (count remaining) 2)
               (= (str/lower-case (second remaining)) "null"))
          (recur (drop 2 remaining) (assoc constraints :not-null true))

          ;; DEFAULT value
          (= token "default")
          (if (>= (count remaining) 2)
            (let [default-value (second remaining)
                  ;; Remove quotes from default value if present
                  clean-value (str/replace default-value #"^['\"]|['\"]$" "")]
              (recur (drop 2 remaining) (assoc constraints :default clean-value)))
            (recur (rest remaining) constraints))

          ;; UNIQUE
          (= token "unique")
          (recur (rest remaining) (assoc constraints :unique true))

          ;; Skip other tokens
          :else
          (recur (rest remaining) constraints))))))

(defn- parse-column-definition
  "Parses a single column definition from tokens.
   Parameters:
   - tokens: Tokens for the column definition
   Returns: Map with column name, type, and constraints"
  [tokens]
  (when (>= (count tokens) 2)
    (let [column-name (first tokens)
          column-type (parse-column-type (second tokens))
          constraint-tokens (drop 2 tokens)
          constraints (parse-column-constraints constraint-tokens)]
      (merge {:name column-name
              :type column-type}
             (when (:primary-key constraints) {:primary_key true})
             (when (:not-null constraints) {:nullable false})
             (when (:default constraints) {:default (:default constraints)})
             (when (:unique constraints) {:unique true})))))

(defn- split-column-definitions
  "Splits the tokens inside CREATE TABLE parentheses into individual column definitions.
   Parameters:
   - tokens: Tokens between ( and ) in CREATE TABLE
   Returns: Sequence of token sequences, one per column"
  [tokens]
  (loop [remaining tokens
         current-col []
         columns []
         paren-depth 0]
    (if (empty? remaining)
      (if (empty? current-col)
        columns
        (conj columns current-col))
      (let [token (first remaining)]
        (cond
          ;; Increase depth for nested parens (e.g., for CHECK constraints)
          (= token "(")
          (recur (rest remaining)
                 (conj current-col token)
                 columns
                 (inc paren-depth))

          ;; Decrease depth for closing parens
          (= token ")")
          (recur (rest remaining)
                 (conj current-col token)
                 columns
                 (dec paren-depth))

          ;; Comma at depth 0 separates columns
          (and (= token ",") (zero? paren-depth))
          (recur (rest remaining)
                 []
                 (if (empty? current-col) columns (conj columns current-col))
                 paren-depth)

          ;; Regular token
          :else
          (recur (rest remaining)
                 (conj current-col token)
                 columns
                 paren-depth))))))

(defn parse-create-table
  "Parses a CREATE TABLE statement.
   Parameters:
   - tokens: The sequence of query tokens
   Returns: A map representing the parsed CREATE TABLE with :type, :table, :columns, :if-not-exists"
  [tokens]
  (let [;; Check for IF NOT EXISTS
        if-idx (tokenizer/find-token-index tokens "if")
        has-if-not-exists (and if-idx
                               (>= (count tokens) (+ if-idx 3))
                               (= (str/lower-case (nth tokens (+ if-idx 1))) "not")
                               (= (str/lower-case (nth tokens (+ if-idx 2))) "exists"))

        ;; Find table name (after TABLE or after IF NOT EXISTS)
        table-idx (if has-if-not-exists
                    (+ if-idx 3)
                    (when-let [tbl-idx (tokenizer/find-token-index tokens "table")]
                      (inc tbl-idx)))

        table-name (when (and table-idx (< table-idx (count tokens)))
                     (let [raw-name (nth tokens table-idx)]
                       (-> raw-name
                           (str/replace #"['\"]" "")
                           (str/replace #";$" "")
                           (str/trim))))

        ;; Parse schema.table format
        [schema table] (when table-name
                         (let [parts (str/split table-name #"\.")]
                           (if (= (count parts) 2)
                             [(first parts) (second parts)]
                             [nil table-name])))

        ;; Find opening parenthesis for columns
        open-paren-idx (when table-idx
                         (tokenizer/find-token-index-from tokens "(" (inc table-idx)))

        ;; Find matching closing parenthesis
        close-paren-idx (when open-paren-idx
                          (tokenizer/find-matching-paren tokens open-paren-idx))

        ;; Extract tokens between parentheses
        column-tokens (when (and open-paren-idx close-paren-idx)
                        (subvec (vec tokens) (inc open-paren-idx) close-paren-idx))

        ;; Split into individual column definitions
        column-defs (when column-tokens
                      (split-column-definitions column-tokens))

        ;; Parse each column definition
        columns (when column-defs
                  (filterv some? (map parse-column-definition column-defs)))]

    {:type :create-table
     :schema schema
     :table table
     :columns (or columns [])
     :if-not-exists (boolean has-if-not-exists)}))

(defn parse-drop-table
  "Parses a DROP TABLE statement.
   Parameters:
   - tokens: The sequence of query tokens
   Returns: A map representing the parsed DROP TABLE with :type, :table, :if-exists"
  [tokens]
  (let [;; Check for IF EXISTS
        if-idx (tokenizer/find-token-index tokens "if")
        has-if-exists (and if-idx
                           (>= (count tokens) (+ if-idx 2))
                           (= (str/lower-case (nth tokens (+ if-idx 1))) "exists"))

        ;; Find table name
        table-idx (if has-if-exists
                    (+ if-idx 2)
                    (when-let [tbl-idx (tokenizer/find-token-index tokens "table")]
                      (inc tbl-idx)))

        table-name (when (and table-idx (< table-idx (count tokens)))
                     (let [raw-name (nth tokens table-idx)]
                       (-> raw-name
                           (str/replace #"['\"]" "")
                           (str/replace #";$" "")
                           (str/trim))))

        ;; Parse schema.table format
        [schema table] (when table-name
                         (let [parts (str/split table-name #"\.")]
                           (if (= (count parts) 2)
                             [(first parts) (second parts)]
                             [nil table-name])))]

    {:type :drop-table
     :schema schema
     :table table
     :if-exists (boolean has-if-exists)}))

(defn parse-show-tables
  "Parses a SHOW TABLES statement.
   Parameters:
   - tokens: The sequence of query tokens
   Returns: A map representing the parsed SHOW TABLES with :type and optional :schema"
  [tokens]
  (let [;; Check for FROM/IN schema clause
        from-idx (or (tokenizer/find-token-index tokens "from")
                     (tokenizer/find-token-index tokens "in"))
        schema (when (and from-idx (< (inc from-idx) (count tokens)))
                 (let [raw-schema (nth tokens (inc from-idx))]
                   (-> raw-schema
                       (str/replace #"['\"]" "")
                       (str/replace #";$" "")
                       (str/trim))))]
    {:type :show-tables
     :schema schema}))

(defn parse-show-schemas
  "Parses a SHOW SCHEMAS statement.
   Parameters:
   - tokens: The sequence of query tokens
   Returns: A map representing the parsed SHOW SCHEMAS with :type"
  [_tokens]
  {:type :show-schemas})

(defn parse-describe
  "Parses a DESCRIBE table or SHOW COLUMNS FROM table statement.
   Parameters:
   - tokens: The sequence of query tokens
   Returns: A map representing the parsed DESCRIBE with :type and :table"
  [tokens]
  (let [first-token (str/lower-case (first tokens))
        ;; Handle both DESCRIBE table and SHOW COLUMNS FROM table
        table-idx (cond
                    (= first-token "describe")
                    1

                    (and (= first-token "show")
                         (>= (count tokens) 4)
                         (= (str/lower-case (second tokens)) "columns")
                         (= (str/lower-case (nth tokens 2)) "from"))
                    3

                    :else nil)

        table-name (when (and table-idx (< table-idx (count tokens)))
                     (let [raw-name (nth tokens table-idx)]
                       (-> raw-name
                           (str/replace #"['\"]" "")
                           (str/replace #";$" "")
                           (str/trim))))

        ;; Parse schema.table format
        [schema table] (when table-name
                         (let [parts (str/split table-name #"\.")]
                           (if (= (count parts) 2)
                             [(first parts) (second parts)]
                             [nil table-name])))]
    {:type :describe
     :schema schema
     :table table}))

(defn parse-branch-function
  "Parses ChronDB branch function calls.
   Parameters:
   - tokens: The sequence of query tokens
   - func-name: The function name (e.g., 'chrondb_branch_list')
   Returns: A map representing the parsed function call or nil"
  [tokens func-name]
  (let [opening-paren-idx (tokenizer/find-token-index tokens "(")
        closing-paren-idx (when opening-paren-idx
                            (tokenizer/find-matching-paren tokens opening-paren-idx))
        args (when (and opening-paren-idx closing-paren-idx)
               (->> (subvec (vec tokens) (inc opening-paren-idx) closing-paren-idx)
                    (remove #(= "," %))
                    (mapv #(str/replace % #"['\"]" ""))))]
    (case func-name
      "chrondb_branch_list"
      {:type :chrondb-function
       :function :branch-list}

      "chrondb_branch_create"
      (when (= (count args) 1)
        {:type :chrondb-function
         :function :branch-create
         :branch-name (first args)})

      "chrondb_branch_checkout"
      (when (= (count args) 1)
        {:type :chrondb-function
         :function :branch-checkout
         :branch-name (first args)})

      "chrondb_branch_merge"
      (when (= (count args) 2)
        {:type :chrondb-function
         :function :branch-merge
         :source-branch (first args)
         :target-branch (second args)})

      nil)))

(defn parse-create-validation-schema
  "Parses a CREATE VALIDATION SCHEMA FOR namespace AS 'json-schema' MODE strict|warning statement.
   Parameters:
   - tokens: The sequence of query tokens
   Returns: A map with :type, :namespace, :schema-json, and :mode"
  [tokens]
  (let [;; Find FOR keyword to get namespace
        for-idx (tokenizer/find-token-index tokens "for")
        namespace (when (and for-idx (< (inc for-idx) (count tokens)))
                    (let [raw-name (nth tokens (inc for-idx))]
                      (-> raw-name
                          (str/replace #"['\"]" "")
                          (str/replace #";$" "")
                          (str/trim))))

        ;; Find AS keyword to get schema JSON
        as-idx (tokenizer/find-token-index tokens "as")
        schema-json (when (and as-idx (< (inc as-idx) (count tokens)))
                      (let [raw-schema (nth tokens (inc as-idx))]
                        (-> raw-schema
                            (str/replace #"^['\"]|['\"]$" "")
                            (str/replace #";$" "")
                            (str/trim))))

        ;; Find MODE keyword to get validation mode
        mode-idx (tokenizer/find-token-index tokens "mode")
        mode (when (and mode-idx (< (inc mode-idx) (count tokens)))
               (let [raw-mode (nth tokens (inc mode-idx))]
                 (-> raw-mode
                     (str/replace #"['\"]" "")
                     (str/replace #";$" "")
                     (str/trim)
                     (str/lower-case)
                     keyword)))]

    {:type :create-validation-schema
     :namespace namespace
     :schema-json schema-json
     :mode (or mode :strict)}))

(defn parse-drop-validation-schema
  "Parses a DROP VALIDATION SCHEMA FOR namespace statement.
   Parameters:
   - tokens: The sequence of query tokens
   Returns: A map with :type and :namespace"
  [tokens]
  (let [;; Find FOR keyword to get namespace
        for-idx (tokenizer/find-token-index tokens "for")
        namespace (when (and for-idx (< (inc for-idx) (count tokens)))
                    (let [raw-name (nth tokens (inc for-idx))]
                      (-> raw-name
                          (str/replace #"['\"]" "")
                          (str/replace #";$" "")
                          (str/trim))))]
    {:type :drop-validation-schema
     :namespace namespace}))

(defn parse-show-validation-schema
  "Parses a SHOW VALIDATION SCHEMA FOR namespace statement.
   Parameters:
   - tokens: The sequence of query tokens
   Returns: A map with :type and :namespace"
  [tokens]
  (let [;; Find FOR keyword to get namespace
        for-idx (tokenizer/find-token-index tokens "for")
        namespace (when (and for-idx (< (inc for-idx) (count tokens)))
                    (let [raw-name (nth tokens (inc for-idx))]
                      (-> raw-name
                          (str/replace #"['\"]" "")
                          (str/replace #";$" "")
                          (str/trim))))]
    {:type :show-validation-schema
     :namespace namespace}))

(defn parse-show-validation-schemas
  "Parses a SHOW VALIDATION SCHEMAS statement.
   Parameters:
   - tokens: The sequence of query tokens
   Returns: A map with :type"
  [_tokens]
  {:type :show-validation-schemas})

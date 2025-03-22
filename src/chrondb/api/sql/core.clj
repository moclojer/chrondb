(ns chrondb.api.sql.core
   "SQL server implementation for ChronDB"
   (:require [chrondb.storage.protocol :as storage]
             [chrondb.index.protocol :as index]
             [chrondb.util.logging :as log]
             [clojure.string :as str]
             [clojure.core.async :as async])
   (:import [java.net ServerSocket Socket]
            [java.io OutputStream InputStream DataOutputStream DataInputStream]
            [java.nio.charset StandardCharsets]
            [java.nio ByteBuffer]))

;; PostgreSQL Protocol Constants
 (def PG_PROTOCOL_VERSION 196608)  ;; Protocol version 3.0
 (def PG_ERROR_RESPONSE (byte (int \E)))
 (def PG_READY_FOR_QUERY (byte (int \Z)))
 (def PG_ROW_DESCRIPTION (byte (int \T)))
 (def PG_DATA_ROW (byte (int \D)))
 (def PG_COMMAND_COMPLETE (byte (int \C)))
 (def PG_AUTHENTICATION_OK (byte (int \R)))
 (def PG_PARAMETER_STATUS (byte (int \S)))
 (def PG_BACKEND_KEY_DATA (byte (int \K)))

;; SQL Parser Constants
 (def RESERVED_WORDS #{"select" "from" "where" "group" "by" "order" "having"
                       "limit" "offset" "insert" "update" "delete" "set" "values"
                       "into" "and" "or" "not" "in" "like" "between" "is" "null"
                       "as" "join" "inner" "left" "right" "outer" "on"})

 (def AGGREGATE_FUNCTIONS #{"count" "sum" "avg" "min" "max"})

 (def COMPARISON_OPERATORS #{"=" "!=" "<>" ">" "<" ">=" "<=" "like" "in"})

 (def LOGICAL_OPERATORS #{"and" "or" "not"})

;; Protocol Communication Functions
 (defn write-bytes
   "Writes a byte array to an output stream and flushes.
   Parameters:
   - out: The output stream to write to
   - data: The byte array to write
   Returns: nil"
   [^OutputStream out ^bytes data]
   (.write out data 0 (alength data))
   (.flush out))

 (defn write-message
   "Writes a PostgreSQL protocol message to an output stream.
   Parameters:
   - out: The output stream to write to
   - type: The message type (one byte)
   - data: The message content as a byte array
   Returns: nil"
   [^OutputStream out type ^bytes data]
   (try
     (let [msg-length (+ 4 (alength data))
           dos (DataOutputStream. out)]
      ;; Message type (1 byte)
       (.writeByte dos (int type))

      ;; Message length (int - 4 bytes) in Big Endian format (Java default)
       (.writeInt dos msg-length)

      ;; Write message content
       (.write dos data 0 (alength data))
       (.flush dos))
     (catch Exception e
       (log/log-error (str "Error writing message: " (.getMessage e))))))

 (defn read-startup-message
   "Reads the startup message from a PostgreSQL client.
   Parameters:
   - in: The input stream to read from
   Returns: A map containing protocol version, database name, and username"
   [^InputStream in]
   (try
     (let [dis (DataInputStream. in)
          ;; Read message size (first field - 4 bytes)
           msg-length (.readInt dis)]
       (when (> msg-length 0)
         (let [content-length (- msg-length 4)
               buffer (byte-array content-length)]
          ;; Read the rest of the message
           (.readFully dis buffer 0 content-length)

          ;; Log received bytes for debug
           (log/log-debug (str "Startup message bytes received: " (count buffer) " bytes"))

          ;; Always return default values to avoid parsing failures
           {:protocol-version PG_PROTOCOL_VERSION
            :database "chrondb"
            :user "chrondb"})))
     (catch Exception e
       (log/log-error (str "Error reading startup message: " (.getMessage e)))
      ;; Return default values to allow the process to continue
       {:protocol-version PG_PROTOCOL_VERSION
        :database "chrondb"
        :user "chrondb"})))

 (defn send-authentication-ok
   "Sends an authentication OK message to the client.
   Parameters:
   - out: The output stream to write to
   Returns: nil"
   [^OutputStream out]
   (let [buffer (ByteBuffer/allocate 4)]
     (.putInt buffer 0)  ;; Authentication success (0)
     (write-message out PG_AUTHENTICATION_OK (.array buffer))))

 (defn string-to-bytes
   "Converts a string to a null-terminated byte array.
   Parameters:
   - s: The string to convert
   Returns: A byte array containing the string bytes followed by a null terminator"
   [s]
   (let [s-bytes (.getBytes s StandardCharsets/UTF_8)
         result (byte-array (inc (alength s-bytes)))]
    ;; Copy the original string
     (System/arraycopy s-bytes 0 result 0 (alength s-bytes))
    ;; Add the null terminator
     (aset-byte result (alength s-bytes) (byte 0))
     result))

 (defn send-parameter-status
   "Sends a parameter status message to the client.
   Parameters:
   - out: The output stream to write to
   - param: The parameter name
   - value: The parameter value
   Returns: nil"
   [^OutputStream out param value]
   (let [buffer (ByteBuffer/allocate 1024)
         param-bytes (.getBytes param StandardCharsets/UTF_8)
         value-bytes (.getBytes value StandardCharsets/UTF_8)]

    ;; Add parameter name
     (.put buffer param-bytes)
     (.put buffer (byte 0))  ;; Null terminator for the name

    ;; Add parameter value
     (.put buffer value-bytes)
     (.put buffer (byte 0))  ;; Null terminator for the value

     (let [pos (.position buffer)
           final-data (byte-array pos)]
       (.flip buffer)
       (.get buffer final-data)
       (write-message out PG_PARAMETER_STATUS final-data))))

 (defn send-backend-key-data
   "Sends backend key data to the client.
   Parameters:
   - out: The output stream to write to
   Returns: nil"
   [^OutputStream out]
   (let [buffer (ByteBuffer/allocate 8)]
    ;; Process ID (4 bytes)
     (.putInt buffer 12345)
    ;; Secret Key (4 bytes)
     (.putInt buffer 67890)
     (write-message out PG_BACKEND_KEY_DATA (.array buffer))))

 (defn send-ready-for-query
   "Sends a ready-for-query message to the client.
   Parameters:
   - out: The output stream to write to
   Returns: nil"
   [^OutputStream out]
   (let [buffer (ByteBuffer/allocate 1)]
    ;; Status: 'I' = idle (ready for queries)
     (.put buffer (byte (int \I)))
     (write-message out PG_READY_FOR_QUERY (.array buffer))))

 (defn send-error-response
   "Sends an error response message to the client.
   Parameters:
   - out: The output stream to write to
   - message: The error message
   Returns: nil"
   [^OutputStream out message]
   (let [buffer (ByteBuffer/allocate 1024)]
    ;; Add error fields
     (.put buffer (byte (int \S)))   ;; Severity field identifier
     (.put buffer (.getBytes "ERROR" StandardCharsets/UTF_8))
     (.put buffer (byte 0))

     (.put buffer (byte (int \C)))  ;; Code field identifier
     (.put buffer (.getBytes "42501" StandardCharsets/UTF_8))  ;; Permission Denied (for UPDATE/DELETE without WHERE)
     (.put buffer (byte 0))

     (.put buffer (byte (int \M)))  ;; Message field identifier
     (.put buffer (.getBytes message StandardCharsets/UTF_8))
     (.put buffer (byte 0))

     (.put buffer (byte (int \H)))  ;; Hint field identifier
     (.put buffer (.getBytes "Add a WHERE clause with a specific ID" StandardCharsets/UTF_8))
     (.put buffer (byte 0))

     (.put buffer (byte (int \P)))  ;; Position field identifier
     (.put buffer (.getBytes "1" StandardCharsets/UTF_8))
     (.put buffer (byte 0))

     (.put buffer (byte 0))  ;; Final terminator

     (let [pos (.position buffer)
           final-data (byte-array pos)]
       (.flip buffer)
       (.get buffer final-data)
       (write-message out PG_ERROR_RESPONSE final-data))))

 (defn send-command-complete
   "Sends a command complete message to the client.
   Parameters:
   - out: The output stream to write to
   - command: The command that was executed (e.g., 'SELECT', 'INSERT')
   - rows: The number of rows affected
   Returns: nil"
   [^OutputStream out command rows]
   (let [buffer (ByteBuffer/allocate 128)
        ;; Format depends on the command
         command-str (if (= command "INSERT")
                      ;; For INSERT the format should be "INSERT 0 1" where 0 is the OID and 1 is the number of rows
                       "INSERT 0 1"
                       (str command " " rows))]
     (log/log-debug (str "Sending command complete: " command-str))
     (.put buffer (.getBytes command-str StandardCharsets/UTF_8))
     (.put buffer (byte 0))  ;; String null terminator

     (let [pos (.position buffer)
           final-data (byte-array pos)]
       (.flip buffer)
       (.get buffer final-data)
       (write-message out PG_COMMAND_COMPLETE final-data))))

 (defn send-row-description
   "Sends a row description message to the client.
   Parameters:
   - out: The output stream to write to
   - columns: A sequence of column names
   Returns: nil"
   [^OutputStream out columns]
   (let [buffer (ByteBuffer/allocate 1024)]
    ;; Number of fields (2 bytes)
     (.putShort buffer (short (count columns)))

    ;; Information for each column
     (doseq [col columns]
      ;; Column name (null-terminated string)
       (.put buffer (.getBytes col StandardCharsets/UTF_8))
       (.put buffer (byte 0))

      ;; Table OID (4 bytes)
       (.putInt buffer 0)

      ;; Attribute number (2 bytes)
       (.putShort buffer (short 0))

      ;; Data type OID (4 bytes) - VARCHAR
       (.putInt buffer 25)  ;; TEXT

      ;; Type size (2 bytes)
       (.putShort buffer (short -1))

      ;; Type modifier (4 bytes)
       (.putInt buffer -1)

      ;; Format code (2 bytes) - 0=text
       (.putShort buffer (short 0)))

     (let [pos (.position buffer)
           final-data (byte-array pos)]
       (.flip buffer)
       (.get buffer final-data)
       (write-message out PG_ROW_DESCRIPTION final-data))))

 (defn send-data-row
   "Sends a data row message to the client.
   Parameters:
   - out: The output stream to write to
   - values: A sequence of values for the row
   Returns: nil"
   [^OutputStream out values]
   (let [buffer (ByteBuffer/allocate (+ 2 (* (count values) 256)))]  ;; Estimated size
    ;; Number of values in the row
     (.putShort buffer (short (count values)))

    ;; Each value
     (doseq [val values]
       (if val
         (let [val-str (str val)
               val-bytes (.getBytes val-str StandardCharsets/UTF_8)]
          ;; Value length
           (.putInt buffer (alength val-bytes))
          ;; Value
           (.put buffer val-bytes))
        ;; Null value
         (.putInt buffer -1)))

     (let [pos (.position buffer)
           final-data (byte-array pos)]
       (.flip buffer)
       (.get buffer final-data)
       (write-message out PG_DATA_ROW final-data))))

;; SQL Parsing Functions
 (defn tokenize-sql
   "Tokenizes an SQL query string into individual tokens.
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
   "Finds the index of the first token that matches any of the given keywords.
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

;; SQL Clause Parsers
 (defn parse-select-columns
   "Parses the column list from a SELECT query.
   Parameters:
   - tokens: The sequence of tokens from the query
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
            ;; Aggregation function (count, sum, etc.)
             (and (re-matches #"\w+\(.*\)" token)
                  (AGGREGATE_FUNCTIONS (first (str/split token #"\("))))
             (let [[fn-name args] (str/split token #"\(|\)")
                   args (str/split args #",")
                   args (map str/trim args)]
               (recur (rest remaining)
                      (conj columns {:type :aggregate-function
                                     :function (keyword fn-name)
                                     :args args})))

            ;; Star (* - all columns)
             (= token "*")
             (recur (rest remaining) (conj columns {:type :all}))

            ;; Column with alias (column as alias)
             (and (> (count remaining) 2)
                  (= (str/lower-case (second remaining)) "as"))
             (recur (drop 3 remaining)
                    (conj columns {:type :column
                                   :column (first remaining)
                                   :alias (nth remaining 2)}))

            ;; Normal column
             :else
             (recur (rest remaining)
                    (conj columns {:type :column
                                   :column token}))))))))

 (defn parse-where-condition
   "Parses the WHERE condition from a SQL query.
   Parameters:
   - tokens: The sequence of tokens from the query
   - where-index: The index of the WHERE keyword
   - end-index: The index where the WHERE clause ends
   Returns: A sequence of condition specifications"
   [tokens where-index end-index]
    ;; Check if where-index is nil or invalid
   (if (or (nil? where-index)
           (< where-index 0)
           (>= where-index (count tokens))
           (>= where-index end-index))
     nil  ;; If invalid, return nil (no conditions)
      ;; If valid, process the conditions
     (let [condition-tokens (subvec tokens (inc where-index) end-index)]
       (loop [remaining condition-tokens
              conditions []
              current-condition {}
              current-logical nil]
         (if (empty? remaining)
            ;; When we've finished processing all tokens
           (if (empty? current-condition)
             conditions
             (conj conditions current-condition))
            ;; Process the next token
           (let [token (str/lower-case (first remaining))]
             (cond
                ;; Logical operator (AND, OR, NOT)
               (LOGICAL_OPERATORS token)
               (recur (rest remaining)
                      (if (empty? current-condition)
                        conditions
                        (conj conditions current-condition))
                      {}
                      token)

                ;; Start of new condition with current logical operator
               (and (seq current-logical)
                    (empty? current-condition))
               (recur (rest remaining)
                      conditions
                      {:field token
                       :logical current-logical}
                      current-logical)

                ;; Condition field
               (empty? current-condition)
               (recur (rest remaining)
                      conditions
                      {:field token}
                      current-logical)

                ;; Operator for condition
               (and (contains? current-condition :field)
                    (not (contains? current-condition :op)))
               (recur (rest remaining)
                      conditions
                      (assoc current-condition :op token)
                      current-logical)

                ;; Value for condition
               (and (contains? current-condition :field)
                    (contains? current-condition :op)
                    (not (contains? current-condition :value)))
               (recur (rest remaining)
                      conditions
                      (assoc current-condition :value token)
                      current-logical)

                ;; Default case
               :else
               (recur (rest remaining)
                      conditions
                      current-condition
                      current-logical))))))))

 (defn parse-group-by
   "Parses the GROUP BY clause from a SQL query.
   Parameters:
   - tokens: The sequence of tokens from the query
   - group-index: The index of the GROUP keyword
   - end-index: The index where the GROUP BY clause ends
   Returns: A sequence of grouping columns or nil if no GROUP BY"
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
   "Parses the ORDER BY clause from a SQL query.
   Parameters:
   - tokens: The sequence of tokens from the query
   - order-index: The index of the ORDER keyword
   - end-index: The index where the ORDER BY clause ends
   Returns: A sequence of ordering specifications or nil if no ORDER BY"
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

;; Complete SQL Query Parsers
 (defn parse-select-query
   "Parses a SELECT SQL query.
   Parameters:
   - tokens: The sequence of tokens from the query
   Returns: A map representing the parsed query with :type, :columns, :table, :where, :group-by, :order-by, and :limit fields"
   [tokens]
   (let [from-index (find-token-index tokens "from")
         where-index (find-token-index tokens "where")
         group-index (find-token-index tokens "group")
         order-index (find-token-index tokens "order")
         limit-index (find-token-index tokens "limit")

      ;; Determine indices for each clause
         where-end (or group-index order-index limit-index (count tokens))
         group-end (or order-index limit-index (count tokens))
         order-end (or limit-index (count tokens))

      ;; Parse each clause
         columns (if from-index
                   (parse-select-columns tokens from-index)
                   [])
         table-name (if (and from-index (> from-index 0))
                      (if (and where-index (> where-index (inc from-index)))
                        (nth tokens (inc from-index))
                        (str/join " " (subvec tokens (inc from-index)
                                              (or where-index group-index order-index limit-index (count tokens)))))
                      nil)
         where-condition (parse-where-condition tokens where-index where-end)
         group-by (parse-group-by tokens group-index group-end)
         order-by (parse-order-by tokens order-index order-end)
         limit (when limit-index
                 (try
                   (Integer/parseInt (nth tokens (inc limit-index)))
                   (catch Exception _ nil)))]

     {:type :select
      :columns columns
      :table table-name
      :where where-condition
      :group-by group-by
      :order-by order-by
      :limit limit}))

 (defn parse-insert-query
   "Parses an INSERT SQL query.
   Parameters:
   - tokens: The sequence of tokens from the query
   Returns: A map representing the parsed query with :type, :table, :columns, and :values fields"
   [tokens]
   (let [into-index (find-token-index tokens "into")
         values-index (find-token-index tokens "values")
         table-name (when (and into-index (> into-index 0))
                      (nth tokens (inc into-index)))

      ;; Parse column names if provided
         columns-start (+ into-index 2)  ;; after INTO table (
         columns-end (- values-index 1)  ;; before )
         columns (when (and (< columns-start columns-end)
                            (= (nth tokens columns-start) "(")
                            (= (nth tokens columns-end) ")"))
                   (mapv str/trim (subvec tokens (inc columns-start) columns-end)))

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
                        in-parens (recur (rest remaining) current-values true (conj current-group token))
                        :else (recur (rest remaining) current-values in-parens current-group)))))]

     {:type :insert
      :table table-name
      :columns columns
      :values (first values)}))

 (defn parse-update-query
   "Parses an UPDATE SQL query.
   Parameters:
   - tokens: The sequence of tokens from the query
   Returns: A map representing the parsed query with :type, :table, :updates, and :where fields"
   [tokens]
   (let [table-name (second tokens)
         set-index (find-token-index tokens "set")
         where-index (find-token-index tokens "where")

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
                           (parse-where-condition tokens where-index (count tokens)))]

     {:type :update
      :table table-name
      :updates updates
      :where where-condition}))

 (defn parse-delete-query
   "Parses a DELETE SQL query.
   Parameters:
   - tokens: The sequence of tokens from the query
   Returns: A map representing the parsed query with :type, :table, and :where fields"
   [tokens]
   (let [from-index (find-token-index tokens "from")
         where-index (find-token-index tokens "where")
         table-name (when (and from-index (< from-index (count tokens)))
                      (nth tokens (inc from-index)))
         where-condition (when where-index
                           (parse-where-condition tokens where-index (count tokens)))]

     {:type :delete
      :table table-name
      :where where-condition}))

 (defn parse-sql-query
   "Parses an SQL query string into a structured representation.
   Parameters:
   - sql: The SQL query string
   Returns: A map representing the parsed query"
   [sql]
   (let [tokens (tokenize-sql sql)
         command (str/lower-case (first tokens))]
     (case command
       "select" (parse-select-query tokens)
       "insert" (parse-insert-query tokens)
       "update" (parse-update-query tokens)
       "delete" (parse-delete-query tokens)
       {:type :unknown, :sql sql})))

;; Query Execution Functions
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

 (defn evaluate-condition
   "Evaluates a condition against a document.
   Parameters:
   - doc: The document to evaluate against
   - condition: The condition to evaluate
   Returns: true if the condition is met, false otherwise"
   [doc condition]
   (let [field-val (get doc (keyword (:field condition)))
         cond-val (str/replace (:value condition) #"['\"]" "")
         operator (:op condition)]
     (case operator
       "=" (= (str field-val) cond-val)
       "!=" (not= (str field-val) cond-val)
       "<>" (not= (str field-val) cond-val)
       ">" (> (compare (str field-val) cond-val) 0)
       "<" (< (compare (str field-val) cond-val) 0)
       ">=" (>= (compare (str field-val) cond-val) 0)
       "<=" (<= (compare (str field-val) cond-val) 0)
       "like" (re-find (re-pattern (str/replace cond-val #"%" ".*")) (str field-val))
    ;; Default case
       (do
         (log/log-warn (str "Unsupported operator: " operator))
         false))))

 (defn apply-where-conditions
   "Applies WHERE conditions to filter a collection of documents.
   Parameters:
   - docs: The documents to filter
   - conditions: The conditions to apply
   Returns: Filtered documents"
   [docs conditions]
   (log/log-debug (str "Applying WHERE conditions: " conditions " to " (count docs) " documents"))
   (if (empty? conditions)
     docs
     (filter
      (fn [document]
        (log/log-debug (str "Checking document: " document))
        (every?
         (fn [condition]
           (let [field (keyword (:field condition))
                 operator (:op condition)
                 value (str/replace (:value condition) #"['\"]" "")
                 doc-value (str (get document field ""))]

             (log/log-debug (str "Checking condition: " field " " operator " " value " against " doc-value))

             (case operator
               "=" (= doc-value value)
               "!=" (not= doc-value value)
               ">" (> (compare doc-value value) 0)
               ">=" (>= (compare doc-value value) 0)
               "<" (< (compare doc-value value) 0)
               "<=" (<= (compare doc-value value) 0)
               "LIKE" (let [pattern (str/replace value "%" ".*")]
                        (boolean (re-find (re-pattern pattern) doc-value)))
              ;; Default: not matching
               false)))
         conditions))
      docs)))

 (defn group-docs-by
   "Groups documents by the specified fields.
   Parameters:
   - docs: The documents to group
   - group-fields: The fields to group by
   Returns: Grouped documents"
   [docs group-fields]
   (if (empty? group-fields)
     [docs]
     (let [group-fn (fn [doc]
                      (mapv #(get doc (keyword (:column %))) group-fields))]
       (vals (group-by group-fn docs)))))

 (defn sort-docs-by
   "Sorts documents by the specified order clauses.
   Parameters:
   - docs: The documents to sort
   - order-clauses: The order clauses to apply
   Returns: Sorted documents"
   [docs order-clauses]
   (if (empty? order-clauses)
     docs
     (let [comparators (for [{:keys [column direction]} order-clauses]
                         (fn [a b]
                           (let [a-val (get a (keyword column))
                                 b-val (get b (keyword column))
                                 result (compare a-val b-val)]
                             (if (= direction :desc)
                               (- result)
                               result))))]
       (sort (fn [a b]
               (loop [comps comparators]
                 (if (empty? comps)
                   0
                   (let [result ((first comps) a b)]
                     (if (zero? result)
                       (recur (rest comps))
                       result)))))
             docs))))

 (defn apply-limit
   "Applies a LIMIT clause to a collection of documents.
   Parameters:
   - docs: The documents to limit
   - limit: The maximum number of documents to return
   Returns: Limited documents"
   [docs limit]
   (if limit
     (take limit docs)
     docs))

 (defn handle-select
   "Handles a SELECT query.
   Parameters:
   - storage: The storage implementation
   - query: The parsed query
   Returns: A sequence of result documents"
   [storage query]
   (try
     (let [where-condition (:where query)
           group-by (:group-by query)
           order-by (:order-by query)
           limit (:limit query)
           table-name (:table query)

          ;; Detailed logs for debugging
           _ (log/log-info (str "Executing SELECT on table: " table-name))
           _ (log/log-debug (str "WHERE condition: " where-condition))
           _ (log/log-debug (str "Columns: " (:columns query)))

          ;; Always retrieve all documents (full scan) unless we have an ID query
           all-docs (if (and where-condition
                             (seq where-condition)
                             (= (count where-condition) 1)
                             (get-in where-condition [0 :field])
                             (= (get-in where-condition [0 :field]) "id")
                             (= (get-in where-condition [0 :op]) "="))
                     ;; Optimization for ID lookup
                      (let [id (str/replace (get-in where-condition [0 :value]) #"['\"]" "")]
                        (log/log-debug (str "Looking up document by ID: " id))
                        (if-let [doc (storage/get-document storage id)]
                          [doc]
                          []))
                     ;; Full scan - retrieve all documents
                      (do
                        (log/log-debug "Performing full document scan")
                        (let [docs (storage/get-documents-by-prefix storage "")]
                          (log/log-debug (str "Retrieved " (count docs) " documents"))
                          (or (seq docs) []))))

           _ (log/log-debug (str "Total documents retrieved: " (count all-docs)))

          ;; Apply WHERE filters
           filtered-docs (if where-condition
                           (do
                             (log/log-debug "Applying WHERE filters")
                             (apply-where-conditions all-docs where-condition))
                           all-docs)

           _ (log/log-debug (str "After filtering: " (count filtered-docs) " documents"))

          ;; Group documents if necessary
           grouped-docs (group-docs-by filtered-docs group-by)

          ;; Process each group
           processed-groups
           (mapv
            (fn [group]
              (let [result-cols (reduce
                                 (fn [acc col-def]
                                   (case (:type col-def)
                                     :all
                                     (if (empty? group)
                                       acc
                                       (merge acc (first group)))

                                     :column
                                     (if-let [alias (:alias col-def)]
                                       (assoc acc (keyword alias) (get (first group) (keyword (:column col-def))))
                                       (assoc acc (keyword (:column col-def)) (get (first group) (keyword (:column col-def)))))

                                     :aggregate-function
                                     (let [fn-result (execute-aggregate-function
                                                      (:function col-def)
                                                      group
                                                      (first (:args col-def)))]
                                       (assoc acc (keyword (str (name (:function col-def)) "_" (first (:args col-def)))) fn-result))

                                    ;; Default
                                     acc))
                                 {}
                                 (:columns query))]
                result-cols))
            grouped-docs)

          ;; Sort results
           sorted-results (sort-docs-by processed-groups order-by)

          ;; Apply limit
           limited-results (apply-limit sorted-results limit)

           _ (log/log-info (str "Returning " (count limited-results) " results"))]

      ;; Return at least an empty list if there are no results
       (or limited-results []))
     (catch Exception e
       (let [sw (java.io.StringWriter.)
             pw (java.io.PrintWriter. sw)]
         (.printStackTrace e pw)
         (log/log-error (str "Error in handle-select: " (.getMessage e) "\n" (.toString sw))))
       [])))

 (defn handle-insert
   "Handles an INSERT query.
   Parameters:
   - storage: The storage implementation
   - doc: The document to insert
   Returns: The saved document"
   [storage doc]
   (log/log-info (str "Inserting document: " doc))
   (let [result (storage/save-document storage doc)]
     (log/log-info (str "Document inserted successfully: " result))
     result))

 (defn handle-update
   "Handles an UPDATE query.
   Parameters:
   - storage: The storage implementation
   - id: The ID of the document to update
   - updates: The updates to apply
   Returns: The updated document or nil if not found"
   [storage id updates]
   (try
     (log/log-debug (str "Attempting to update document with ID: " id))
     (when-let [doc (storage/get-document storage id)]
       (log/log-debug (str "Found document to update: " doc))
       (let [updated-doc (merge doc updates)]
         (log/log-debug (str "Merged document: " updated-doc))
         (storage/save-document storage updated-doc)))
     (catch Exception e
       (let [sw (java.io.StringWriter.)
             pw (java.io.PrintWriter. sw)]
         (.printStackTrace e pw)
         (log/log-error (str "Error in handle-update: " (.getMessage e) "\n" (.toString sw))))
       nil)))

 (defn handle-delete
   "Handles a DELETE query.
   Parameters:
   - storage: The storage implementation
   - id: The ID of the document to delete
   Returns: The deleted document or nil if not found"
   [storage id]
   (storage/delete-document storage id))

 (defn handle-query
   "Handles an SQL query.
   Parameters:
   - storage: The storage implementation
   - index: The index implementation
   - out: The output stream to write the results to
   - sql: The SQL query string
   Returns: nil"
   [storage index ^OutputStream out sql]
   (log/log-info (str "Executing query: " sql))
   (let [parsed (parse-sql-query sql)]
     (log/log-debug (str "Parsed query: " parsed))
     (case (:type parsed)
       :select
       (let [results (handle-select storage parsed)
             columns (if (empty? results)
                       ["id" "value"]
                       (mapv name (keys (first results))))]
         (send-row-description out columns)
         (doseq [row results]
           (send-data-row out (map #(get row (keyword %)) columns)))
         (send-command-complete out "SELECT" (count results)))

       :insert
       (try
         (log/log-info (str "Processing INSERT on table: " (:table parsed)))
         (log/log-debug (str "Values: " (:values parsed) ", columns: " (:columns parsed)))

         (let [values (:values parsed)
               columns (:columns parsed)

              ;; Remove quotes and other formatting from values
               clean-values (if (seq values)
                              (mapv #(str/replace % #"['\"]" "") values)
                              [])

               _ (log/log-debug (str "Clean values for INSERT: " clean-values))

              ;; Create a document with provided values
               doc (if (and (seq clean-values) (seq columns))
                    ;; If columns and values are defined, map them
                     (let [doc-map (reduce (fn [acc [col val]]
                                             (assoc acc (keyword col) val))
                                           {:id (str (java.util.UUID/randomUUID))}  ;; Always provide an ID
                                           (map vector columns clean-values))]
                       (log/log-debug (str "Document created with column mapping: " doc-map))
                       doc-map)
                    ;; Otherwise, use the standard id/value format
                     (let [doc-map (cond
                                     (>= (count clean-values) 2) {:id (first clean-values)
                                                                  :value (second clean-values)}
                                     (= (count clean-values) 1) {:id (str (java.util.UUID/randomUUID))
                                                                 :value (first clean-values)}
                                     :else {:id (str (java.util.UUID/randomUUID)) :value ""})]
                       (log/log-debug (str "Document created with standard format: " doc-map))
                       doc-map))

               _ (log/log-info (str "Document to insert: " doc))

              ;; Save the document in storage
               saved (handle-insert storage doc)]

          ;; Index the document (if we have an index)
           (when index
             (log/log-debug "Indexing document")
             (index/index-document index saved))

           (log/log-info (str "INSERT completed successfully, ID: " (:id saved)))
           (send-command-complete out "INSERT" 1)
           (send-ready-for-query out))
         (catch Exception e
           (let [sw (java.io.StringWriter.)
                 pw (java.io.PrintWriter. sw)]
             (.printStackTrace e pw)
             (log/log-error (str "Error processing INSERT: " (.getMessage e) "\n" (.toString sw))))
           (send-error-response out (str "Error processing INSERT: " (.getMessage e)))
           (send-command-complete out "INSERT" 0)
           (send-ready-for-query out)))

       :update
       (try
         (log/log-info (str "Processing UPDATE on table: " (:table parsed)))
         (let [where-conditions (:where parsed)
               updates (try
                         (reduce-kv
                          (fn [m k v]
                            (assoc m (keyword k) (str/replace v #"['\"]" "")))
                          {}
                          (:updates parsed))
                         (catch Exception e
                           (log/log-error (str "Error processing update values: " (.getMessage e)))
                           {}))]

           (log/log-debug (str "Update conditions: " where-conditions ", values: " updates))

           (if (and (seq where-conditions) (seq updates))
            ;; Update with valid conditions
             (let [;; Find documents that match the conditions
                   _ (log/log-info (str "Searching for documents matching: " where-conditions))
                   all-docs (storage/get-documents-by-prefix storage "")
                   matching-docs (apply-where-conditions all-docs where-conditions)
                   update-count (atom 0)]

               (if (seq matching-docs)
                 (do
                  ;; Update each found document
                   (log/log-info (str "Found " (count matching-docs) " documents to update"))
                   (doseq [doc matching-docs]
                     (let [updated-doc (merge doc updates)
                           _ (log/log-debug (str "Updating document: " (:id doc) " with values: " updates))
                           _ (log/log-debug (str "Merged document: " updated-doc))
                           saved (storage/save-document storage updated-doc)]

                      ;; Re-index the document if necessary
                       (when (and saved index)
                         (log/log-debug (str "Re-indexing updated document: " (:id saved)))
                         (index/index-document index saved))

                       (swap! update-count inc)))

                   (log/log-info (str "Updated " @update-count " documents successfully"))
                   (send-command-complete out "UPDATE" @update-count))

                ;; No documents found
                 (do
                   (log/log-warn "No documents found matching the WHERE conditions")
                   (send-error-response out "No documents found matching the WHERE conditions")
                   (send-command-complete out "UPDATE" 0))))

            ;; Update without WHERE or with invalid data
             (do
               (log/log-warn "Invalid UPDATE query - missing WHERE clause or invalid update values")
               (let [error-msg (if (seq where-conditions)
                                 "UPDATE failed: No valid values to update"
                                 "UPDATE without WHERE clause is not supported. Please specify conditions.")]
                 (log/log-error error-msg)
                 (send-error-response out error-msg)
                 (send-command-complete out "UPDATE" 0)))))
         (catch Exception e
           (let [sw (java.io.StringWriter.)
                 pw (java.io.PrintWriter. sw)]
             (.printStackTrace e pw)
             (log/log-error (str "Error processing UPDATE: " (.getMessage e) "\n" (.toString sw))))
           (send-error-response out (str "Error processing UPDATE: " (.getMessage e)))
           (send-command-complete out "UPDATE" 0)))

       :delete
       (let [id (when (seq (:where parsed))
                  (str/replace (get-in parsed [:where 0 :value]) #"['\"]" ""))]
         (if id
        ;; Specific DELETE by ID
           (let [_ (log/log-info (str "Deleting document with ID: " id))
                 deleted (handle-delete storage id)
                 _ (when (and deleted index)
                     (index/delete-document index id))]
             (log/log-info (str "Document " (if deleted "successfully deleted" "not found")))
             (send-command-complete out "DELETE" (if deleted 1 0)))
        ;; DELETE without WHERE - respond with error
           (do
             (log/log-warn "DELETE attempt without WHERE clause was rejected")
             (let [error-msg "DELETE without WHERE clause is not supported. Use WHERE id='value' to specify which document to delete."]
               (log/log-error error-msg)
               (send-error-response out error-msg)
               (send-ready-for-query out)))))

      ;; Unknown command
       (do
         (send-error-response out (str "Unknown command: " sql))
         (send-command-complete out "UNKNOWN" 0)))))

 (defn handle-message
   "Handles a PostgreSQL protocol message.
   Parameters:
   - storage: The storage implementation
   - index: The index implementation
   - out: The output stream to write responses to
   - message-type: The type of the message
   - buffer: The message content as a byte array
   - content-length: The length of the message content
   Returns: true to continue reading messages, false to terminate connection"
   [storage index ^OutputStream out message-type buffer content-length]
   (log/log-debug (str "Received message type: " (char message-type)))
   (try
     (case (char message-type)
       \Q (let [query-text (String. buffer 0 (dec content-length) StandardCharsets/UTF_8)]
            (log/log-debug (str "SQL Query: " query-text))
            (try
              (handle-query storage index out query-text)
              (catch Exception e
                (let [sw (java.io.StringWriter.)
                      pw (java.io.PrintWriter. sw)]
                  (.printStackTrace e pw)
                  (log/log-error (str "Error executing query: " (.getMessage e) "\n" (.toString sw))))
                (send-error-response out (str "Error executing query: " (.getMessage e)))
                (send-command-complete out "UNKNOWN" 0)))
            (send-ready-for-query out)
            true)  ;; Continue reading
       \X (do      ;; Terminate message received
            (log/log-info "Client requested termination")
            false) ;; Signal to close connection
       (do         ;; Other unsupported command
         (log/log-debug (str "Unsupported command: " (char message-type)))
         (send-error-response out (str "Unsupported command: " (char message-type)))
         (send-ready-for-query out)
         true))    ;; Continue reading
     (catch Exception e
       (let [sw (java.io.StringWriter.)
             pw (java.io.PrintWriter. sw)]
         (.printStackTrace e pw)
         (log/log-error (str "Error handling message: " (.getMessage e) "\n" (.toString sw))))
       (try
         (send-error-response out (str "Internal server error: " (.getMessage e)))
         (send-ready-for-query out)
         (catch Exception e2
           (log/log-error (str "Error sending error response: " (.getMessage e2)))))
       true)))

 (defn handle-client
   "Handles a PostgreSQL client connection.
   Parameters:
   - storage: The storage implementation
   - index: The index implementation
   - client-socket: The client socket
   Returns: nil"
   [storage index ^Socket client-socket]
   (log/log-info (str "SQL client connected: " (.getRemoteSocketAddress client-socket)))
   (let [closed-socket? (atom false)]
     (try
       (let [in (.getInputStream client-socket)
             out (.getOutputStream client-socket)]
        ;; Authentication and initial configuration
         (when-let [_startup-message (read-startup-message in)]
           (log/log-debug "Sending authentication OK")
           (send-authentication-ok out)
           (send-parameter-status out "server_version" "14.0")
           (send-parameter-status out "client_encoding" "UTF8")
           (send-parameter-status out "DateStyle" "ISO, MDY")
           (send-backend-key-data out)
           (send-ready-for-query out)

          ;; Main loop to read queries
           (let [dis (DataInputStream. in)]
             (try
               (loop []
                 (log/log-debug "Waiting for client message")
                 (let [continue?
                       (try
                         (let [message-type (.readByte dis)]
                           (log/log-debug (str "Received message type: " (char message-type)))
                           (when (pos? message-type)
                             (let [message-length (.readInt dis)
                                   content-length (- message-length 4)
                                   buffer (byte-array content-length)]
                               (.readFully dis buffer 0 content-length)
                               (handle-message storage index out message-type buffer content-length))))
                         (catch java.io.EOFException _e
                           (log/log-info (str "Client disconnected: " (.getRemoteSocketAddress client-socket)))
                           (reset! closed-socket? true)
                           false) ;; Terminate
                         (catch java.net.SocketException e
                           (log/log-info (str "Socket closed: " (.getMessage e)))
                           (reset! closed-socket? true)
                           false) ;; Terminate
                         (catch Exception e
                           (let [sw (java.io.StringWriter.)
                                 pw (java.io.PrintWriter. sw)]
                             (.printStackTrace e pw)
                             (log/log-error (str "Error reading client message: " (.getMessage e) "\n" (.toString sw))))
                           (try
                             (send-error-response out (str "Internal server error: Error reading message"))
                             (send-ready-for-query out)
                             (catch Exception e2
                               (log/log-error (str "Error sending error response: " (.getMessage e2)))))
                           true))] ;; Continue reading unless socket was closed
                   (when continue?
                     (recur))))
               (catch Exception e
                 (let [sw (java.io.StringWriter.)
                       pw (java.io.PrintWriter. sw)]
                   (.printStackTrace e pw)
                   (log/log-error (str "Error in client processing loop: " (.getMessage e) "\n" (.toString sw)))))))))
       (catch Exception e
         (let [sw (java.io.StringWriter.)
               pw (java.io.PrintWriter. sw)]
           (.printStackTrace e pw)
           (log/log-error (str "Error initializing SQL client: " (.getMessage e) "\n" (.toString sw)))))
       (finally
        ;; Always ensure socket is closed when we're done
         (when (and client-socket (not @closed-socket?) (not (.isClosed client-socket)))
           (try
             (.close client-socket)
             (log/log-info (str "Closed client socket: " (.getRemoteSocketAddress client-socket)))
             (catch Exception e
               (log/log-error (str "Error closing client socket: " (.getMessage e))))))))))

(defn start-sql-server
  "Starts a SQL server for ChronDB.
   Parameters:
   - storage: The storage implementation
   - index: The index implementation
   - port: The port number to listen on
   Returns: The server socket"
  [storage index port]
  (let [server-socket (ServerSocket. port)]
    (log/log-info (str "Starting SQL server on port " port))
    (async/go
      (try
        (while (not (.isClosed server-socket))
          (try
            (let [client-socket (.accept server-socket)]
              (async/go
                (handle-client storage index client-socket)))
            (catch Exception e
              (when-not (.isClosed server-socket)
                (log/log-error (str "Error accepting SQL connection: " (.getMessage e)))))))
        (catch Exception e
          (log/log-error (str "Error in SQL server: " (.getMessage e))))))
    server-socket))

(defn stop-sql-server
  "Stops the SQL server.
   Parameters:
   - server-socket: The server socket to close
   Returns: nil"
  [^ServerSocket server-socket]
  (log/log-info "Stopping SQL server")
  (when (and server-socket (not (.isClosed server-socket)))
    (.close server-socket)))
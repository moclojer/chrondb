(ns chrondb.api.sql.core
  "SQL server implementation for ChronDB"
  (:require [chrondb.storage.protocol :as storage]
            [chrondb.index.protocol :as index]
            [chrondb.util.logging :as log]
            [clojure.string :as str]
            [clojure.core.async :as async])
  (:import [java.net ServerSocket Socket]
           [java.io BufferedReader BufferedWriter InputStreamReader OutputStreamWriter OutputStream InputStream
            DataOutputStream DataInputStream]
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
(defn write-bytes [^OutputStream out ^bytes data]
  (.write out data 0 (alength data))
  (.flush out))

(defn write-message [^OutputStream out type ^bytes data]
  (try
    (let [msg-length (+ 4 (alength data))
          dos (DataOutputStream. out)]
      ;; Tipo da mensagem (1 byte)
      (.writeByte dos (int type))

      ;; Comprimento da mensagem (int - 4 bytes) em formato Big Endian (padrão do Java)
      (.writeInt dos msg-length)

      ;; Escrever o conteúdo da mensagem
      (.write dos data 0 (alength data))
      (.flush dos))
    (catch Exception e
      (log/log-error (str "Error writing message: " (.getMessage e))))))

(defn read-startup-message [^InputStream in]
  (try
    (let [dis (DataInputStream. in)
          ;; Ler o tamanho da mensagem (primeiro campo - 4 bytes)
          msg-length (.readInt dis)]
      (when (> msg-length 0)
        (let [content-length (- msg-length 4)
              buffer (byte-array content-length)]
          ;; Ler o resto da mensagem
          (.readFully dis buffer 0 content-length)

          ;; Log dos bytes recebidos para debug
          (log/log-debug (str "Startup message bytes received: " (count buffer) " bytes"))

          ;; Sempre retornar valores padrão para evitar falhas na análise
          {:protocol-version PG_PROTOCOL_VERSION
           :database "chrondb"
           :user "chrondb"})))
    (catch Exception e
      (log/log-error (str "Error reading startup message: " (.getMessage e)))
      ;; Retornar um valor padrão para permitir a continuação do processo
      {:protocol-version PG_PROTOCOL_VERSION
       :database "chrondb"
       :user "chrondb"})))

(defn send-authentication-ok [^OutputStream out]
  (let [buffer (ByteBuffer/allocate 4)]
    (.putInt buffer 0)  ;; Authentication success (0)
    (write-message out PG_AUTHENTICATION_OK (.array buffer))))

(defn string-to-bytes [s]
  (let [s-bytes (.getBytes s StandardCharsets/UTF_8)
        result (byte-array (inc (alength s-bytes)))]
    ;; Copiar a string original
    (System/arraycopy s-bytes 0 result 0 (alength s-bytes))
    ;; Adicionar o terminador nulo
    (aset-byte result (alength s-bytes) (byte 0))
    result))

(defn send-parameter-status [^OutputStream out param value]
  (let [buffer (ByteBuffer/allocate 1024)
        param-bytes (.getBytes param StandardCharsets/UTF_8)
        value-bytes (.getBytes value StandardCharsets/UTF_8)]

    ;; Adicionar o nome do parâmetro
    (.put buffer param-bytes)
    (.put buffer (byte 0))  ;; Terminador nulo para o nome

    ;; Adicionar o valor do parâmetro
    (.put buffer value-bytes)
    (.put buffer (byte 0))  ;; Terminador nulo para o valor

    (let [pos (.position buffer)
          final-data (byte-array pos)]
      (.flip buffer)
      (.get buffer final-data)
      (write-message out PG_PARAMETER_STATUS final-data))))

(defn send-backend-key-data [^OutputStream out]
  (let [buffer (ByteBuffer/allocate 8)]
    ;; Process ID (4 bytes)
    (.putInt buffer 12345)
    ;; Secret Key (4 bytes)
    (.putInt buffer 67890)
    (write-message out PG_BACKEND_KEY_DATA (.array buffer))))

(defn send-ready-for-query [^OutputStream out]
  (let [buffer (ByteBuffer/allocate 1)]
    ;; Status: 'I' = idle (ready for queries)
    (.put buffer (byte (int \I)))
    (write-message out PG_READY_FOR_QUERY (.array buffer))))

(defn send-error-response [^OutputStream out message]
  (let [buffer (ByteBuffer/allocate 1024)]
    ;; Adicionar os campos do erro
    (.put buffer (.getBytes "S" StandardCharsets/UTF_8))   ;; Severity
    (.put buffer (byte 0))
    (.put buffer (.getBytes "ERROR" StandardCharsets/UTF_8))
    (.put buffer (byte 0))

    (.put buffer (.getBytes "C" StandardCharsets/UTF_8))  ;; Code
    (.put buffer (byte 0))
    (.put buffer (.getBytes "42601" StandardCharsets/UTF_8))  ;; Syntax error code
    (.put buffer (byte 0))

    (.put buffer (.getBytes "M" StandardCharsets/UTF_8))  ;; Message
    (.put buffer (byte 0))
    (.put buffer (.getBytes message StandardCharsets/UTF_8))
    (.put buffer (byte 0))

    (.put buffer (byte 0))  ;; Terminador final

    (let [pos (.position buffer)
          final-data (byte-array pos)]
      (.flip buffer)
      (.get buffer final-data)
      (write-message out PG_ERROR_RESPONSE final-data))))

(defn send-command-complete [^OutputStream out command rows]
  (let [buffer (ByteBuffer/allocate 128)
        command-str (str command " " rows)]
    (.put buffer (.getBytes command-str StandardCharsets/UTF_8))
    (.put buffer (byte 0))  ;; String null terminator

    (let [pos (.position buffer)
          final-data (byte-array pos)]
      (.flip buffer)
      (.get buffer final-data)
      (write-message out PG_COMMAND_COMPLETE final-data))))

(defn send-row-description [^OutputStream out columns]
  (let [buffer (ByteBuffer/allocate 1024)]
    ;; Número de campos (2 bytes)
    (.putShort buffer (short (count columns)))

    ;; Informações de cada coluna
    (doseq [col columns]
      ;; Nome da coluna (string terminada com null)
      (.put buffer (.getBytes col StandardCharsets/UTF_8))
      (.put buffer (byte 0))

      ;; Table OID (4 bytes)
      (.putInt buffer 0)

      ;; Atributo número (2 bytes)
      (.putShort buffer (short 0))

      ;; Tipo de dados OID (4 bytes) - VARCHAR
      (.putInt buffer 25)  ;; TEXT

      ;; Tamanho do tipo (2 bytes)
      (.putShort buffer (short -1))

      ;; Modificador tipo (4 bytes)
      (.putInt buffer -1)

      ;; Formato código (2 bytes) - 0=texto
      (.putShort buffer (short 0)))

    (let [pos (.position buffer)
          final-data (byte-array pos)]
      (.flip buffer)
      (.get buffer final-data)
      (write-message out PG_ROW_DESCRIPTION final-data))))

(defn send-data-row [^OutputStream out values]
  (let [buffer (ByteBuffer/allocate (+ 2 (* (count values) 256)))]  ;; Tamanho estimado
    ;; Número de valores na linha
    (.putShort buffer (short (count values)))

    ;; Cada valor
    (doseq [val values]
      (if val
        (let [val-str (str val)
              val-bytes (.getBytes val-str StandardCharsets/UTF_8)]
          ;; Comprimento do valor
          (.putInt buffer (alength val-bytes))
          ;; Valor
          (.put buffer val-bytes))
        ;; Valor nulo
        (.putInt buffer -1)))

    (let [pos (.position buffer)
          final-data (byte-array pos)]
      (.flip buffer)
      (.get buffer final-data)
      (write-message out PG_DATA_ROW final-data))))

;; SQL Parsing Functions
(defn tokenize-sql [sql]
  (-> sql
      (str/replace #"([(),;=<>])" " $1 ")
      (str/replace #"\s+" " ")
      (str/trim)
      (str/split #"\s+")))

(defn find-token-index [tokens & keywords]
  (let [keyword-set (set (map str/lower-case keywords))]
    (->> tokens
         (map-indexed (fn [idx token] [idx (str/lower-case token)]))
         (filter (fn [[_ token]] (keyword-set token)))
         (first)
         (first))))

;; SQL Clause Parsers
(defn parse-select-columns [tokens from-index]
  (let [column-tokens (subvec tokens 1 from-index)]
    (loop [remaining column-tokens
           columns []]
      (if (empty? remaining)
        columns
        (let [token (str/lower-case (first remaining))]
          (cond
            ;; Função de agregação (count, sum, etc.)
            (and (re-matches #"\w+\(.*\)" token)
                 (AGGREGATE_FUNCTIONS (first (str/split token #"\("))))
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

(defn parse-where-condition [tokens where-index end-index]
  (if (or (< where-index 0) (>= where-index end-index))
    nil
    (let [condition-tokens (subvec tokens (inc where-index) end-index)]
      (loop [remaining condition-tokens
             conditions []
             current-condition {}
             current-logical nil]
        (if (empty? remaining)
          (if (empty? current-condition)
            conditions
            (conj conditions current-condition))
          (let [token (str/lower-case (first remaining))]
            (cond
              ;; Operador lógico
              (LOGICAL_OPERATORS token)
              (recur (rest remaining)
                     (if (empty? current-condition)
                       conditions
                       (conj conditions current-condition))
                     {}
                     token)

              ;; Início de nova condição com operador lógico atual
              (and (not (empty? current-logical))
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
              (recur (rest remaining) conditions current-condition current-logical))))))))

(defn parse-group-by [tokens group-index end-index]
  (if (or (< group-index 0) (>= group-index end-index))
    nil
    (let [by-index (+ group-index 1)]
      (if (and (< by-index end-index)
               (= (str/lower-case (nth tokens by-index)) "by"))
        (let [columns (subvec tokens (+ by-index 1) end-index)]
          (mapv (fn [col] {:column col}) columns))
        nil))))

(defn parse-order-by [tokens order-index end-index]
  (if (or (< order-index 0) (>= order-index end-index))
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
(defn parse-select-query [tokens]
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
        columns (parse-select-columns tokens from-index)
        table-name (if (> from-index 0)
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

(defn parse-insert-query [tokens]
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

(defn parse-update-query [tokens]
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

(defn parse-delete-query [tokens]
  (let [from-index (find-token-index tokens "from")
        where-index (find-token-index tokens "where")
        table-name (when (and from-index (< from-index (count tokens)))
                     (nth tokens (inc from-index)))
        where-condition (when where-index
                          (parse-where-condition tokens where-index (count tokens)))]

    {:type :delete
     :table table-name
     :where where-condition}))

(defn parse-sql-query [sql]
  (let [tokens (tokenize-sql sql)
        command (str/lower-case (first tokens))]
    (case command
      "select" (parse-select-query tokens)
      "insert" (parse-insert-query tokens)
      "update" (parse-update-query tokens)
      "delete" (parse-delete-query tokens)
      {:type :unknown, :sql sql})))

;; Query Execution Functions
(defn execute-aggregate-function [function docs field]
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

(defn evaluate-condition [doc condition]
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

(defn apply-where-conditions [docs conditions]
  (if (empty? conditions)
    docs
    (let [grouped-conditions (group-by #(:logical %) conditions)
          default-conditions (get grouped-conditions nil [])
          and-conditions (get grouped-conditions "and" [])
          or-conditions (get grouped-conditions "or" [])]

      (filter
       (fn [doc]
         (and
          ;; All default and AND conditions must be true
          (every? #(evaluate-condition doc %)
                  (concat default-conditions and-conditions))

          ;; At least one OR condition must be true (if there are any)
          (or (empty? or-conditions)
              (some #(evaluate-condition doc %) or-conditions))))
       docs))))

(defn group-docs-by [docs group-fields]
  (if (empty? group-fields)
    [docs]
    (let [group-fn (fn [doc]
                     (mapv #(get doc (keyword (:column %))) group-fields))]
      (vals (group-by group-fn docs)))))

(defn sort-docs-by [docs order-clauses]
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

(defn apply-limit [docs limit]
  (if limit
    (take limit docs)
    docs))

(defn handle-select [storage query]
  (try
    (let [where-condition (:where query)
          group-by (:group-by query)
          order-by (:order-by query)
          limit (:limit query)

          ;; Fetch documents
          all-docs (if (and where-condition
                            (= (count where-condition) 1)
                            (get-in where-condition [0 :field])
                            (= (get-in where-condition [0 :field]) "id")
                            (= (get-in where-condition [0 :op]) "="))
                     ;; Optimization for id lookup
                     (let [id (str/replace (get-in where-condition [0 :value]) #"['\"]" "")]
                       (when-let [doc (storage/get-document storage id)]
                         [doc]))
                     ;; Full scan
                     (or (seq (storage/get-documents-by-prefix storage "")) []))

          ;; Apply where conditions
          filtered-docs (apply-where-conditions all-docs where-condition)

          ;; Group documents if needed
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
          limited-results (apply-limit sorted-results limit)]

      ;; Retornar pelo menos uma lista vazia se não houver resultados
      (or limited-results []))
    (catch Exception e
      (log/log-error (str "Error in handle-select: " (.getMessage e)))
      [])))

(defn handle-insert [storage doc]
  (storage/save-document storage doc))

(defn handle-update [storage id updates]
  (when-let [doc (storage/get-document storage id)]
    (let [updated-doc (merge doc updates)]
      (storage/save-document storage updated-doc))))

(defn handle-delete [storage id]
  (storage/delete-document storage id))

(defn handle-query [storage index ^OutputStream out sql]
  (log/log-info (str "Executing query: " sql))
  (let [parsed (parse-sql-query sql)]
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
      (let [doc {:id (second (:values parsed))
                 :value (nth (:values parsed) 2)}
            saved (handle-insert storage doc)
            _ (when index (index/index-document index saved))]
        (send-command-complete out "INSERT" 1))

      :update
      (let [id (str/replace (get-in parsed [:where 0 :value]) #"['\"]" "")
            updates (reduce-kv
                     (fn [m k v]
                       (assoc m (keyword k) (str/replace v #"['\"]" "")))
                     {}
                     (:updates parsed))
            updated (handle-update storage id updates)
            _ (when (and updated index)
                (index/index-document index updated))]
        (send-command-complete out "UPDATE" (if updated 1 0)))

      :delete
      (let [id (str/replace (get-in parsed [:where 0 :value]) #"['\"]" "")
            deleted (handle-delete storage id)
            _ (when (and deleted index)
                (index/delete-document index id))]
        (send-command-complete out "DELETE" (if deleted 1 0)))

      ;; Unknown command
      (do
        (send-error-response out (str "Unknown command: " sql))
        (send-command-complete out "UNKNOWN" 0)))))

(defn handle-message [storage index ^OutputStream out message-type buffer content-length]
  (log/log-debug (str "Received message type: " (char message-type)))
  (try
    (case (char message-type)
      \Q (let [query-text (String. buffer 0 (dec content-length) StandardCharsets/UTF_8)]
           (log/log-debug (str "SQL Query: " query-text))
           (try
             (handle-query storage index out query-text)
             (catch Exception e
               (log/log-error (str "Error executing query: " (.getMessage e)))
               (send-error-response out (str "Error executing query: " (.getMessage e)))))
           (send-ready-for-query out)
           true)  ;; Continue reading
      \X false   ;; Terminate
      (do        ;; Other unsupported command
        (log/log-debug (str "Unsupported command: " (char message-type)))
        (send-error-response out (str "Unsupported command: " (char message-type)))
        (send-ready-for-query out)
        true))  ;; Continue reading
    (catch Exception e
      (log/log-error (str "Error handling message: " (.getMessage e)))
      (try
        (send-error-response out (str "Internal server error: " (.getMessage e)))
        (send-ready-for-query out)
        (catch Exception e2
          (log/log-error (str "Error sending error response: " (.getMessage e2)))))
      true)))

(defn handle-client [storage index ^Socket client-socket]
  (log/log-info (str "SQL client connected: " (.getRemoteSocketAddress client-socket)))
  (try
    (let [in (.getInputStream client-socket)
          out (.getOutputStream client-socket)]
      ;; Autenticação e configuração inicial
      (when-let [startup-message (read-startup-message in)]
        (log/log-debug "Sending authentication OK")
        (send-authentication-ok out)
        (send-parameter-status out "server_version" "14.0")
        (send-parameter-status out "client_encoding" "UTF8")
        (send-parameter-status out "DateStyle" "ISO, MDY")
        (send-backend-key-data out)
        (send-ready-for-query out)

        ;; Loop principal para ler consultas
        (let [dis (DataInputStream. in)]
          (try
            (loop []
              (let [message-type (.readByte dis)]
                (when (pos? message-type)
                  (let [message-length (.readInt dis)
                        content-length (- message-length 4)
                        buffer (byte-array content-length)]
                    (.readFully dis buffer 0 content-length)

                    ;; Processar a mensagem
                    (case (char message-type)
                      \Q (let [query-text (String. buffer 0 (dec content-length) StandardCharsets/UTF_8)]
                           (handle-query storage index out query-text)
                           (send-ready-for-query out)
                           (recur))
                      \X nil  ;; Terminar
                      (do     ;; Comando desconhecido
                        (send-error-response out (str "Unsupported command: " (char message-type)))
                        (send-ready-for-query out)
                        (recur)))))))
            (catch Exception e
              (log/log-error (str "Error reading client message: " (.getMessage e))))))))
    (catch Exception e
      (log/log-error (str "Error processing SQL client: " (.getMessage e))))))

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
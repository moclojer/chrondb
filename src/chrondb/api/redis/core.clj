(ns chrondb.api.redis.core
  "Redis protocol server implementation for ChronDB.
   Allows Redis clients to connect to ChronDB and use Redis commands."
  (:require [chrondb.storage.protocol :as storage]
            [chrondb.index.protocol :as index]
            [chrondb.query.ast :as ast]
            [chrondb.util.logging :as log]
            [chrondb.transaction.core :as tx]
            [clojure.string :as str]
            [clojure.data.json :as json]
            [clojure.core.async :as async])
  (:import [java.net ServerSocket]
           [java.io BufferedReader BufferedWriter InputStreamReader OutputStreamWriter]
           [java.nio.charset StandardCharsets]))

;; RESP Protocol Constants
(def CRLF "\r\n")
(def RESP_SIMPLE_STRING "+")
(def RESP_ERROR "-")
(def RESP_INTEGER ":")
(def RESP_BULK_STRING "$")
(def RESP_ARRAY "*")
(def RESP_NULL "$-1\r\n")
(def RESP_OK "+OK\r\n")
(def RESP_PONG "+PONG\r\n")

;; RESP3 Protocol Constants
(def RESP3_NULL "_")
(def RESP3_DOUBLE ",")
(def RESP3_BOOLEAN "#")
(def RESP3_MAP "%")
(def RESP3_SET "~")
(def RESP3_VERBATIM_STRING "=")
(def RESP3_BIG_NUMBER "(")
(def RESP3_PUSH ">")

;; Connection Context for RESP2/RESP3 protocol negotiation
(defrecord ConnectionContext [protocol-version client-name])

(defn create-connection-context
  "Creates a new connection context with default RESP2 protocol"
  []
  (->ConnectionContext 2 nil))

(defn resp3?
  "Returns true if the connection is using RESP3 protocol"
  [ctx]
  (= 3 (:protocol-version ctx)))

(defn- normalize-flags [flags]
  (letfn [(spread [value]
            (cond
              (nil? value) []
              (and (coll? value) (not (string? value))) (mapcat spread value)
              :else [value]))]
    (->> (spread flags)
         (keep identity)
         (map str)
         (remove str/blank?)
         distinct
         vec
         not-empty)))

(defn- redis-command-metadata
  [command args extra]
  (merge {:command command
          :arg-count (count args)}
         (when (seq args)
           {:args (mapv str (take 8 args))})
         (or extra {})))

(defn- redis-tx-options
  [command args {:keys [flags metadata]}]
  (let [normalized (normalize-flags flags)
        meta (redis-command-metadata command args metadata)]
    (cond-> {:origin "redis"
             :metadata meta}
      normalized (assoc :flags normalized))))

(defn execute-redis-write
  ([storage command args f]
   (execute-redis-write storage command args {} f))
  ([storage command args opts f]
   (tx/with-transaction [storage (redis-tx-options command args opts)]
     (f))))

;; RESP Protocol Serialization Functions
(defn write-simple-string [writer s]
  (.write writer (str RESP_SIMPLE_STRING s CRLF)))

(defn write-error [writer error-msg]
  (.write writer (str RESP_ERROR "ERR " error-msg CRLF)))

(defn write-integer [writer n]
  (.write writer (str RESP_INTEGER n CRLF)))

(defn write-bulk-string [writer s]
  (if (nil? s)
    (.write writer RESP_NULL)
    (let [bytes (.getBytes (str s) StandardCharsets/UTF_8)]
      (.write writer (str RESP_BULK_STRING (count bytes) CRLF))
      (.write writer (str s CRLF)))))

(defn write-array [writer elements]
  (if (nil? elements)
    (.write writer "*-1\r\n")
    (do
      (.write writer (str RESP_ARRAY (count elements) CRLF))
      (doseq [el elements]
        (cond
          (string? el) (write-bulk-string writer el)
          (number? el) (write-integer writer el)
          (nil? el) (.write writer RESP_NULL)
          :else (write-bulk-string writer (str el)))))))

;; RESP3 Protocol Serialization Functions
(defn write-null
  "Writes a RESP3 null value"
  [writer]
  (.write writer (str RESP3_NULL CRLF)))

(defn write-double
  "Writes a RESP3 double value"
  [writer d]
  (if (Double/isInfinite d)
    (if (pos? d)
      (.write writer (str RESP3_DOUBLE "inf" CRLF))
      (.write writer (str RESP3_DOUBLE "-inf" CRLF)))
    (.write writer (str RESP3_DOUBLE d CRLF))))

(defn write-boolean
  "Writes a RESP3 boolean value"
  [writer b]
  (.write writer (str RESP3_BOOLEAN (if b "t" "f") CRLF)))

(defn write-map
  "Writes a RESP3 map value"
  [writer m]
  (.write writer (str RESP3_MAP (count m) CRLF))
  (doseq [[k v] m]
    (write-bulk-string writer (if (keyword? k) (name k) (str k)))
    (cond
      (string? v) (write-bulk-string writer v)
      (integer? v) (write-integer writer v)
      (float? v) (write-double writer v)
      (boolean? v) (write-boolean writer v)
      (nil? v) (write-null writer)
      (map? v) (write-map writer v)
      (coll? v) (write-array writer (vec v))
      :else (write-bulk-string writer (str v)))))

(defn write-set
  "Writes a RESP3 set value"
  [writer s]
  (.write writer (str RESP3_SET (count s) CRLF))
  (doseq [el s]
    (cond
      (string? el) (write-bulk-string writer el)
      (number? el) (write-integer writer el)
      (nil? el) (write-null writer)
      :else (write-bulk-string writer (str el)))))

(declare write-value)

(defn write-value
  "Universal value writer that dispatches based on context and value type.
   Uses RESP3 types when in RESP3 mode, falls back to RESP2 otherwise."
  [writer value ctx]
  (let [resp3-mode (resp3? ctx)]
    (cond
      (nil? value) (if resp3-mode
                     (write-null writer)
                     (.write writer RESP_NULL))
      (boolean? value) (if resp3-mode
                         (write-boolean writer value)
                         (write-bulk-string writer (if value "true" "false")))
      (integer? value) (write-integer writer value)
      (float? value) (if resp3-mode
                       (write-double writer value)
                       (write-bulk-string writer (str value)))
      (string? value) (write-bulk-string writer value)
      (map? value) (if resp3-mode
                     (write-map writer value)
                     (write-array writer (mapcat (fn [[k v]] [(if (keyword? k) (name k) (str k)) (str v)]) value)))
      (set? value) (if resp3-mode
                     (write-set writer (vec value))
                     (write-array writer (vec value)))
      (coll? value) (write-array writer (vec value))
      :else (write-bulk-string writer (str value)))))

;; RESP Protocol Parsing Functions
(defn read-line-resp [reader]
  (let [line (.readLine reader)]
    (when line
      line)))

(defn parse-integer [s]
  (try
    (Long/parseLong s)
    (catch NumberFormatException _
      nil)))

(declare read-resp)

(defn read-bulk-string [reader]
  (let [line (read-line-resp reader)
        len (parse-integer (subs line 1))]
    (if (neg? len)
      nil
      (let [data (char-array len)
            _ (.read reader data 0 len)
            _ (.read reader 2)] ; consume CRLF
        (String. data)))))

(defn read-array [reader]
  (let [line (read-line-resp reader)
        count (parse-integer (subs line 1))]
    (if (neg? count)
      nil
      (vec (repeatedly count #(read-resp reader))))))

(defn read-resp [reader]
  (let [line (.readLine reader)]
    (when line
      (let [type (first line)
            data (subs line 1)]
        (case type
          \+ data                      ; Simple String
          \- (throw (ex-info data {})) ; Error
          \: (parse-integer data)      ; Integer
          \$ (if (= data "-1")         ; Bulk String
               nil
               (let [len (parse-integer data)
                     data (char-array len)
                     _ (dotimes [i len]
                         (aset data i (char (.read reader))))
                     _ (.read reader)  ; consume CR
                     _ (.read reader)] ; consume LF
                 (String. data)))
          \* (let [count (parse-integer data)] ; Array
               (if (neg? count)
                 nil
                 (vec (repeatedly count #(read-resp reader)))))
          (throw (ex-info (str "Unknown RESP type: " type) {})))))))

;; Command Handling
(defn handle-ping [writer args]
  (if (empty? args)
    (.write writer RESP_PONG)
    (write-bulk-string writer (first args))))

(defn handle-echo [writer args]
  (if (empty? args)
    (write-error writer "ERR wrong number of arguments for 'echo' command")
    (write-bulk-string writer (first args))))

(defn handle-get [storage writer args]
  (if (empty? args)
    (write-error writer "ERR wrong number of arguments for 'get' command")
    (let [key (first args)
          doc (storage/get-document storage key)]
      (if doc
        (write-bulk-string writer (:value doc))
        (.write writer RESP_NULL)))))

(defn handle-set [storage index writer args]
  (if (< (count args) 2)
    (write-error writer "ERR wrong number of arguments for 'set' command")
    (let [key (first args)
          value (second args)
          doc {:id key :value value}]
      (execute-redis-write storage "SET" args {:metadata {:key key}}
        (fn []
          (storage/save-document storage doc)
          (when index (index/index-document index doc))
          (.write writer RESP_OK))))))

(defn handle-del [storage writer args]
  (if (empty? args)
    (write-error writer "ERR wrong number of arguments for 'del' command")
    (let [key (first args)]
      (execute-redis-write storage "DEL" args {:metadata {:key key} :flags ["delete"]}
        (fn []
          (let [result (storage/delete-document storage key)]
            (write-integer writer (if result 1 0))))))))

(defn handle-command [_storage _index writer _args]
  (.write writer RESP_OK))

(defn handle-info [writer _args]
  (let [info-str "# Server\r\nredis_version:6.0.0\r\nchrondb_version:1.0.0\r\n"]
    (write-bulk-string writer info-str)))

(defn handle-setex [storage index writer args]
  (if (< (count args) 3)
    (write-error writer "ERR wrong number of arguments for 'setex' command")
    (let [key (first args)
          seconds (try (Integer/parseInt (second args))
                       (catch Exception _
                         (write-error writer "ERR value is not an integer or out of range")
                         nil))
          value (nth args 2)
          doc {:id key :value value :expire-at (+ (System/currentTimeMillis) (* seconds 1000))}]
      (when seconds
        (execute-redis-write storage "SETEX" args {:metadata {:key key :ttl seconds}}
          (fn []
            (storage/save-document storage doc)
            (when index (index/index-document index doc))
            (.write writer RESP_OK)))))))

(defn handle-setnx [storage index writer args]
  (if (< (count args) 2)
    (write-error writer "ERR wrong number of arguments for 'setnx' command")
    (let [key (first args)
          value (second args)
          existing-doc (storage/get-document storage key)]
      (if existing-doc
        (write-integer writer 0) ; Key exists, don't set
        (execute-redis-write storage "SETNX" args {:metadata {:key key}}
          (fn []
            (storage/save-document storage {:id key :value value})
            (when index (index/index-document index {:id key :value value}))
            (write-integer writer 1)))))))

(defn handle-exists [storage writer args]
  (if (empty? args)
    (write-error writer "ERR wrong number of arguments for 'exists' command")
    (let [key (first args)
          exists (if (storage/get-document storage key) 1 0)]
      (write-integer writer exists))))

;; Hash commands
(defn handle-hset [storage writer args]
  (if (< (count args) 3)
    (write-error writer "ERR wrong number of arguments for 'hset' command")
    (let [key (first args)
          field (second args)
          value (nth args 2)
          hash-key (str key ":" field)
          doc {:id hash-key :value value :hash-key key :hash-field field}
          existing-doc (storage/get-document storage hash-key)
          is-new (nil? existing-doc)]
      (execute-redis-write storage "HSET" args {:metadata {:key key :field field}}
        (fn []
          (storage/save-document storage doc)
          (write-integer writer (if is-new 1 0)))))))

(defn handle-hget [storage writer args]
  (if (< (count args) 2)
    (write-error writer "ERR wrong number of arguments for 'hget' command")
    (let [key (first args)
          field (second args)
          hash-key (str key ":" field)
          doc (storage/get-document storage hash-key)]
      (if doc
        (write-bulk-string writer (:value doc))
        (write-bulk-string writer nil)))))

(defn handle-hmset [storage writer args]
  (if (< (count args) 3)
    (write-error writer "ERR wrong number of arguments for 'hmset' command")
    (let [key (first args)
          field-values (rest args)]
      (if (odd? (count field-values))
        (write-error writer "ERR wrong number of arguments for 'hmset' command")
        (let [field-count (/ (count field-values) 2)]
          (execute-redis-write storage "HMSET" args {:metadata {:key key :field-count field-count}}
            (fn []
              (doseq [[field value] (partition 2 field-values)]
                (let [hash-key (str key ":" field)
                      doc {:id hash-key :value value :hash-key key :hash-field field}]
                  (storage/save-document storage doc)))
              (.write writer RESP_OK))))))))

(defn handle-hmget [storage writer args]
  (if (< (count args) 2)
    (write-error writer "ERR wrong number of arguments for 'hmget' command")
    (let [key (first args)
          ;; Todos os argumentos após o primeiro são campos individuais
          fields (rest args)
          values (mapv (fn [field]
                         (let [hash-key (str key ":" field)
                               doc (storage/get-document storage hash-key)]
                           (if doc (:value doc) nil)))
                       fields)]
      (write-array writer values))))

(defn handle-hgetall [storage writer args]
  (if (not= (count args) 1)
    (write-error writer "ERR wrong number of arguments for 'hgetall' command")
    (let [key (first args)
          ;; Fetch all documents that start with the key prefix
          prefix (str key ":")
          docs (storage/get-documents-by-prefix storage prefix)
          ;; Transform into a map of field -> value
          result (reduce (fn [acc doc]
                           (let [field (subs (:id doc) (count prefix))]
                             (conj acc field (:value doc))))
                         []
                         docs)]
      (write-array writer result))))

;; List commands
(defn handle-lpush [storage writer args]
  (if (< (count args) 2)
    (write-error writer "ERR wrong number of arguments for 'lpush' command")
    (let [key (first args)
          values (rest args)
          list-doc (or (storage/get-document storage key)
                       {:id key :type "list" :values []})
          updated-values (vec (concat values (:values list-doc)))
          updated-doc (assoc list-doc :values updated-values)]
      (execute-redis-write storage "LPUSH" args {:metadata {:key key :value-count (count values)}}
        (fn []
          (storage/save-document storage updated-doc)
          (write-integer writer (count updated-values)))))))

(defn handle-rpush [storage writer args]
  (if (< (count args) 2)
    (write-error writer "ERR wrong number of arguments for 'rpush' command")
    (let [key (first args)
          values (rest args)
          list-doc (or (storage/get-document storage key)
                       {:id key :type "list" :values []})
          updated-values (vec (concat (:values list-doc) values))
          updated-doc (assoc list-doc :values updated-values)]
      (execute-redis-write storage "RPUSH" args {:metadata {:key key :value-count (count values)}}
        (fn []
          (storage/save-document storage updated-doc)
          (write-integer writer (count updated-values)))))))

(defn handle-lrange [storage writer args]
  (if (not= (count args) 3)
    (write-error writer "ERR wrong number of arguments for 'lrange' command")
    (let [key (first args)
          start (try (Integer/parseInt (second args))
                     (catch Exception _
                       (write-error writer "ERR value is not an integer or out of range")
                       nil))
          stop (try (Integer/parseInt (nth args 2))
                    (catch Exception _
                      (write-error writer "ERR value is not an integer or out of range")
                      nil))
          list-doc (storage/get-document storage key)
          values (if list-doc (:values list-doc) [])]
      (when (and start stop)
        (let [len (count values)
              start (if (neg? start) (+ len start) start)
              stop (if (neg? stop) (+ len stop) stop)
              start (max 0 start)
              stop (min (dec len) stop)
              result (if (<= start stop)
                       (subvec values start (inc stop))
                       [])]
          (write-array writer result))))))

(defn handle-lpop [storage writer args]
  (if (empty? args)
    (write-error writer "ERR wrong number of arguments for 'lpop' command")
    (let [key (first args)
          list-doc (storage/get-document storage key)]
      (if (and list-doc (seq (:values list-doc)))
        (let [values (:values list-doc)
              first-value (first values)
              updated-values (vec (rest values))
              updated-doc (assoc list-doc :values updated-values)]
          (execute-redis-write storage "LPOP" args {:metadata {:key key}}
            (fn []
              (storage/save-document storage updated-doc)
              (write-bulk-string writer first-value))))
        (write-bulk-string writer nil)))))

(defn handle-rpop [storage writer args]
  (if (empty? args)
    (write-error writer "ERR wrong number of arguments for 'rpop' command")
    (let [key (first args)
          list-doc (storage/get-document storage key)]
      (if (and list-doc (seq (:values list-doc)))
        (let [values (:values list-doc)
              last-value (last values)
              updated-values (vec (butlast values))
              updated-doc (assoc list-doc :values updated-values)]
          (execute-redis-write storage "RPOP" args {:metadata {:key key}}
            (fn []
              (storage/save-document storage updated-doc)
              (write-bulk-string writer last-value))))
        (write-bulk-string writer nil)))))

(defn handle-llen [storage writer args]
  (if (empty? args)
    (write-error writer "ERR wrong number of arguments for 'llen' command")
    (let [key (first args)
          list-doc (storage/get-document storage key)
          len (if list-doc (count (:values list-doc)) 0)]
      (write-integer writer len))))

;; Set commands
(defn handle-sadd [storage writer args]
  (if (< (count args) 2)
    (write-error writer "ERR wrong number of arguments for 'sadd' command")
    (let [key (first args)
          members (rest args)
          set-doc (or (storage/get-document storage key)
                      {:id key :type "set" :members #{}})
          existing-members (:members set-doc)
          new-members (filter #(not (contains? existing-members %)) members)
          updated-members (apply conj existing-members members)
          updated-doc (assoc set-doc :members updated-members)]
      (execute-redis-write storage "SADD" args {:metadata {:key key :member-count (count members)}}
        (fn []
          (storage/save-document storage updated-doc)
          (write-integer writer (count new-members)))))))

(defn handle-smembers [storage writer args]
  (if (empty? args)
    (write-error writer "ERR wrong number of arguments for 'smembers' command")
    (let [key (first args)
          set-doc (storage/get-document storage key)
          members (if set-doc (vec (:members set-doc)) [])]
      (write-array writer members))))

(defn handle-sismember [storage writer args]
  (if (< (count args) 2)
    (write-error writer "ERR wrong number of arguments for 'sismember' command")
    (let [key (first args)
          member (second args)
          set-doc (storage/get-document storage key)
          is-member (if set-doc
                      (contains? (:members set-doc) member)
                      false)]
      (write-integer writer (if is-member 1 0)))))

(defn handle-srem [storage writer args]
  (if (< (count args) 2)
    (write-error writer "ERR wrong number of arguments for 'srem' command")
    (let [key (first args)
          members (rest args)
          set-doc (storage/get-document storage key)]
      (if set-doc
        (let [existing-members (:members set-doc)
              removed-members (filter #(contains? existing-members %) members)
              updated-members (apply disj existing-members members)
              updated-doc (assoc set-doc :members updated-members)]
          (execute-redis-write storage "SREM" args {:metadata {:key key :member-count (count members)}
                                                    :flags ["delete"]}
            (fn []
              (storage/save-document storage updated-doc)
              (write-integer writer (count removed-members)))))
        (write-integer writer 0)))))

;; Sorted Set commands
(defn handle-zadd [storage writer args]
  (if (< (count args) 3)
    (write-error writer "ERR wrong number of arguments for 'zadd' command")
    (let [key (first args)
          score-member-pairs (partition 2 (rest args))
          zset-doc (or (storage/get-document storage key)
                       {:id key :type "zset" :members {}})
          existing-members (:members zset-doc)
          new-members (count (filter (fn [[_ member]]
                                       (not (contains? existing-members member)))
                                     score-member-pairs))
          updated-members (reduce (fn [acc [score member]]
                                    (assoc acc member (Double/parseDouble score)))
                                  existing-members
                                  score-member-pairs)
          updated-doc (assoc zset-doc :members updated-members)]
      (execute-redis-write storage "ZADD" args {:metadata {:key key :member-count (count score-member-pairs)}}
        (fn []
          (storage/save-document storage updated-doc)
          (write-integer writer new-members))))))

(defn handle-zrange [storage writer args]
  (if (< (count args) 3)
    (write-error writer "ERR wrong number of arguments for 'zrange' command")
    (let [key (first args)
          start (try (Integer/parseInt (second args))
                     (catch Exception _
                       (write-error writer "ERR value is not an integer or out of range")
                       nil))
          stop (try (Integer/parseInt (nth args 2))
                    (catch Exception _
                      (write-error writer "ERR value is not an integer or out of range")
                      nil))
          zset-doc (storage/get-document storage key)]
      (when (and start stop)
        (if zset-doc
          (let [members (:members zset-doc)
                sorted-members (sort-by second (map (fn [[k v]] [k v]) members))
                len (count sorted-members)
                start (if (neg? start) (+ len start) start)
                stop (if (neg? stop) (+ len stop) stop)
                start (max 0 start)
                stop (min (dec len) stop)
                result (if (<= start stop)
                         (mapv first (subvec (vec sorted-members) start (inc stop)))
                         [])]
            (write-array writer result))
          (write-array writer []))))))

(defn handle-zrank [storage writer args]
  (if (< (count args) 2)
    (write-error writer "ERR wrong number of arguments for 'zrank' command")
    (let [key (first args)
          member (second args)
          zset-doc (storage/get-document storage key)]
      (if (and zset-doc (contains? (:members zset-doc) member))
        (let [members (:members zset-doc)
              sorted-members (sort-by second (map (fn [[k v]] [k v]) members))
              rank (first (keep-indexed (fn [idx [m _]] (when (= m member) idx)) sorted-members))]
          (write-integer writer rank))
        (write-bulk-string writer nil)))))

(defn handle-zscore [storage writer args]
  (if (< (count args) 2)
    (write-error writer "ERR wrong number of arguments for 'zscore' command")
    (let [key (first args)
          member (second args)
          zset-doc (storage/get-document storage key)]
      (if (and zset-doc (contains? (:members zset-doc) member))
        (let [score (get-in zset-doc [:members member])]
          (write-bulk-string writer (str score)))
        (write-bulk-string writer nil)))))

(defn handle-zrem [storage writer args]
  (if (< (count args) 2)
    (write-error writer "ERR wrong number of arguments for 'zrem' command")
    (let [key (first args)
          members (rest args)
          zset-doc (storage/get-document storage key)]
      (if zset-doc
        (let [existing-members (:members zset-doc)
              removed-count (count (filter #(contains? existing-members %) members))
              updated-members (apply dissoc existing-members members)
              updated-doc (assoc zset-doc :members updated-members)]
          (execute-redis-write storage "ZREM" args {:metadata {:key key :member-count (count members)}
                                                    :flags ["delete"]}
            (fn []
              (storage/save-document storage updated-doc)
              (write-integer writer removed-count))))
        (write-integer writer 0)))))

(defn handle-search
  "SEARCH command: search documents using AST queries.
   Usage: SEARCH query [LIMIT limit] [OFFSET offset] [SORT field:asc|desc] [BRANCH branch]
   Example: SEARCH hello LIMIT 10 SORT id:asc"
  [storage index writer args]
  (if (empty? args)
    (write-error writer "ERR wrong number of arguments for 'search' command")
    (try
      (let [query-str (first args)
            remaining (vec (rest args))
            ;; Parse options - look for keyword-value pairs
            limit (loop [idx 0 found nil]
                    (if (>= idx (count remaining))
                      found
                      (if (and (< (inc idx) (count remaining))
                               (= "limit" (str/lower-case (nth remaining idx))))
                        (recur (+ idx 2) (try (Integer/parseInt (nth remaining (inc idx)))
                                             (catch Exception _ nil)))
                        (recur (inc idx) found))))
            offset (loop [idx 0 found nil]
                     (if (>= idx (count remaining))
                       found
                       (if (and (< (inc idx) (count remaining))
                                (= "offset" (str/lower-case (nth remaining idx))))
                         (recur (+ idx 2) (try (Integer/parseInt (nth remaining (inc idx)))
                                              (catch Exception _ nil)))
                         (recur (inc idx) found))))
            sort-param (loop [idx 0 found nil]
                         (if (>= idx (count remaining))
                           found
                           (if (and (< (inc idx) (count remaining))
                                    (= "sort" (str/lower-case (nth remaining idx))))
                             (recur (+ idx 2) (nth remaining (inc idx)))
                             (recur (inc idx) found))))
            branch (loop [idx 0 found nil]
                     (if (>= idx (count remaining))
                       found
                       (if (and (< (inc idx) (count remaining))
                                (= "branch" (str/lower-case (nth remaining idx))))
                         (recur (+ idx 2) (nth remaining (inc idx)))
                         (recur (inc idx) found))))
            branch-name (or (not-empty branch) "main")

            ;; Build AST query from query string
            ;; Simple query string is converted to FTS clause
            fts-clause (ast/fts "content" query-str)
            sort-descriptors (when sort-param
                               (let [parts (str/split sort-param #":")
                                     field (some-> parts first str/trim)
                                     dir (some-> parts second str/trim)
                                     direction (if (= "desc" (str/lower-case (or dir ""))) :desc :asc)]
                                 (when field
                                   [(ast/sort-by field direction)])))
            ast-query (ast/query [fts-clause]
                                {:limit limit
                                 :offset offset
                                 :sort sort-descriptors
                                 :branch branch-name})

            ;; Execute search
            opts (cond-> {}
                   limit (assoc :limit limit)
                   offset (assoc :offset offset)
                   sort-descriptors (assoc :sort sort-descriptors))

            result (index/search-query index ast-query branch-name opts)

            ;; Get full documents from storage
            doc-ids (:ids result)
            docs (filter some? (map #(storage/get-document storage % branch-name) doc-ids))

            ;; Format results as array of JSON strings
            results (mapv #(json/write-str %) docs)]

        (write-array writer results))
      (catch Exception e
        (write-error writer (str "Search error: " (.getMessage e)))))))

(defn handle-ft-search
  "FT.SEARCH command: alias for SEARCH (compatible with RediSearch)"
  [storage index writer args]
  (handle-search storage index writer args))

;; HELLO command - RESP3 protocol negotiation
(defn handle-hello
  "HELLO command: negotiate protocol version and get server info.
   Usage: HELLO [protover [AUTH username password] [SETNAME clientname]]
   Returns server information as a map in RESP3 or array in RESP2."
  [writer args ctx-atom]
  (let [protover (when (seq args)
                   (try (Integer/parseInt (first args))
                        (catch NumberFormatException _ nil)))
        ;; Parse optional AUTH and SETNAME
        remaining (vec (rest args))
        client-name (loop [idx 0]
                      (if (>= idx (count remaining))
                        nil
                        (if (= "setname" (str/lower-case (str (nth remaining idx))))
                          (when (< (inc idx) (count remaining))
                            (nth remaining (inc idx)))
                          (recur (inc idx)))))
        ;; Update context if valid protocol version
        _ (when (and protover (<= 2 protover 3))
            (swap! ctx-atom assoc
                   :protocol-version protover
                   :client-name client-name))
        ctx @ctx-atom
        server-info {:server "chrondb"
                     :version "1.0.0"
                     :proto (:protocol-version ctx)
                     :id (str (System/currentTimeMillis))
                     :mode "standalone"
                     :role "master"
                     :modules []}]
    (if (resp3? ctx)
      (write-map writer server-info)
      ;; RESP2: return as flat array
      (write-array writer (mapcat (fn [[k v]]
                                    [(if (keyword? k) (name k) (str k))
                                     (if (coll? v)
                                       (str v)
                                       (str v))])
                                  server-info)))))

;; Pattern matching helpers for SCAN commands
(defn glob->regex
  "Converts a glob pattern to a Java regex pattern.
   Supports: * (any chars), ? (single char), [abc] (char class)"
  [pattern]
  (let [sb (StringBuilder. "^")]
    (doseq [c pattern]
      (case c
        \* (.append sb ".*")
        \? (.append sb ".")
        \[ (.append sb "[")
        \] (.append sb "]")
        \. (.append sb "\\.")
        \\ (.append sb "\\\\")
        \^ (.append sb "\\^")
        \$ (.append sb "\\$")
        \+ (.append sb "\\+")
        \{ (.append sb "\\{")
        \} (.append sb "\\}")
        \| (.append sb "\\|")
        \( (.append sb "\\(")
        \) (.append sb "\\)")
        (.append sb c)))
    (.append sb "$")
    (re-pattern (str sb))))

(defn matches-pattern?
  "Returns true if key matches the glob pattern"
  [pattern key]
  (if (or (nil? pattern) (= "*" pattern))
    true
    (boolean (re-matches (glob->regex pattern) key))))

;; Cursor encoding/decoding for SCAN
(defn encode-cursor
  "Encodes an offset as a cursor string"
  [offset]
  (if (zero? offset) "0" (str offset)))

(defn decode-cursor
  "Decodes a cursor string to an offset"
  [cursor-str]
  (if (= cursor-str "0") 0 (Long/parseLong cursor-str)))

;; Parse SCAN options from args
(defn parse-scan-options
  "Parses SCAN command options: MATCH pattern, COUNT count, TYPE type"
  [args]
  (loop [idx 0
         opts {:match nil :count 10 :type nil}]
    (if (>= idx (count args))
      opts
      (let [arg (str/lower-case (str (nth args idx)))]
        (case arg
          "match" (recur (+ idx 2)
                         (if (< (inc idx) (count args))
                           (assoc opts :match (nth args (inc idx)))
                           opts))
          "count" (recur (+ idx 2)
                         (if (< (inc idx) (count args))
                           (assoc opts :count (try (Integer/parseInt (nth args (inc idx)))
                                                   (catch Exception _ 10)))
                           opts))
          "type" (recur (+ idx 2)
                        (if (< (inc idx) (count args))
                          (assoc opts :type (str/lower-case (nth args (inc idx))))
                          opts))
          (recur (inc idx) opts))))))

;; SCAN command handler
(defn handle-scan
  "SCAN command: incrementally iterate the keys space.
   Usage: SCAN cursor [MATCH pattern] [COUNT count] [TYPE type]
   Returns: [next-cursor [keys...]]"
  [storage writer args]
  (if (empty? args)
    (write-error writer "ERR wrong number of arguments for 'scan' command")
    (try
      (let [cursor (decode-cursor (first args))
            opts (parse-scan-options (vec (rest args)))
            pattern (:match opts)
            count-limit (:count opts)
            type-filter (:type opts)
            ;; Get all documents (using empty prefix to get all)
            all-docs (storage/get-documents-by-prefix storage "")
            ;; Filter by pattern
            filtered-docs (if pattern
                            (filter #(matches-pattern? pattern (:id %)) all-docs)
                            all-docs)
            ;; Filter by type if specified
            typed-docs (if type-filter
                         (filter #(= type-filter (str/lower-case (or (:type %) "string")))
                                 filtered-docs)
                         filtered-docs)
            ;; Paginate
            doc-ids (map :id typed-docs)
            paged-ids (take count-limit (drop cursor doc-ids))
            next-cursor (if (< (+ cursor count-limit) (count doc-ids))
                          (encode-cursor (+ cursor count-limit))
                          "0")]
        (write-array writer [next-cursor (vec paged-ids)]))
      (catch Exception e
        (write-error writer (str "ERR " (.getMessage e)))))))

;; HSCAN command handler
(defn handle-hscan
  "HSCAN command: incrementally iterate hash fields and values.
   Usage: HSCAN key cursor [MATCH pattern] [COUNT count]
   Returns: [next-cursor [field1 val1 field2 val2...]]"
  [storage writer args]
  (if (< (count args) 2)
    (write-error writer "ERR wrong number of arguments for 'hscan' command")
    (try
      (let [key (first args)
            cursor (decode-cursor (second args))
            opts (parse-scan-options (vec (drop 2 args)))
            pattern (:match opts)
            count-limit (:count opts)
            ;; Get hash fields (stored as key:field)
            prefix (str key ":")
            docs (storage/get-documents-by-prefix storage prefix)
            ;; Extract field names and values
            field-values (map (fn [doc]
                                (let [field (subs (:id doc) (count prefix))]
                                  [field (:value doc)]))
                              docs)
            ;; Filter by pattern
            filtered (if pattern
                       (filter #(matches-pattern? pattern (first %)) field-values)
                       field-values)
            ;; Paginate
            paged (take count-limit (drop cursor filtered))
            flat-result (vec (mapcat identity paged))
            next-cursor (if (< (+ cursor count-limit) (count filtered))
                          (encode-cursor (+ cursor count-limit))
                          "0")]
        (write-array writer [next-cursor flat-result]))
      (catch Exception e
        (write-error writer (str "ERR " (.getMessage e)))))))

;; SSCAN command handler
(defn handle-sscan
  "SSCAN command: incrementally iterate set members.
   Usage: SSCAN key cursor [MATCH pattern] [COUNT count]
   Returns: [next-cursor [member1 member2...]]"
  [storage writer args]
  (if (< (count args) 2)
    (write-error writer "ERR wrong number of arguments for 'sscan' command")
    (try
      (let [key (first args)
            cursor (decode-cursor (second args))
            opts (parse-scan-options (vec (drop 2 args)))
            pattern (:match opts)
            count-limit (:count opts)
            ;; Get the set document
            set-doc (storage/get-document storage key)
            members (if set-doc (vec (:members set-doc)) [])
            ;; Filter by pattern
            filtered (if pattern
                       (filter #(matches-pattern? pattern %) members)
                       members)
            ;; Paginate
            paged (take count-limit (drop cursor filtered))
            next-cursor (if (< (+ cursor count-limit) (count filtered))
                          (encode-cursor (+ cursor count-limit))
                          "0")]
        (write-array writer [next-cursor (vec paged)]))
      (catch Exception e
        (write-error writer (str "ERR " (.getMessage e)))))))

;; HISTORY command - ChronDB time-travel extension
(defn encode-history-cursor
  "Encodes a history cursor as Base64 EDN"
  [cursor-data]
  (if (empty? cursor-data)
    "0"
    (let [edn-str (pr-str cursor-data)
          bytes (.getBytes edn-str StandardCharsets/UTF_8)]
      (.encodeToString (java.util.Base64/getEncoder) bytes))))

(defn decode-history-cursor
  "Decodes a history cursor from Base64 EDN"
  [cursor-str]
  (if (or (nil? cursor-str) (= cursor-str "0"))
    {:offset 0}
    (try
      (let [bytes (.decode (java.util.Base64/getDecoder) cursor-str)
            edn-str (String. bytes StandardCharsets/UTF_8)]
        (read-string edn-str))
      (catch Exception _
        {:offset 0}))))

(defn parse-history-options
  "Parses HISTORY command options: CURSOR cursor, SINCE timestamp, COUNT count"
  [args]
  (loop [idx 0
         opts {:cursor nil :since nil :count 100}]
    (if (>= idx (count args))
      opts
      (let [arg (str/lower-case (str (nth args idx)))]
        (case arg
          "cursor" (recur (+ idx 2)
                          (if (< (inc idx) (count args))
                            (assoc opts :cursor (nth args (inc idx)))
                            opts))
          "since" (recur (+ idx 2)
                         (if (< (inc idx) (count args))
                           (assoc opts :since (try (Long/parseLong (nth args (inc idx)))
                                                   (catch Exception _ nil)))
                           opts))
          "count" (recur (+ idx 2)
                         (if (< (inc idx) (count args))
                           (assoc opts :count (try (Integer/parseInt (nth args (inc idx)))
                                                   (catch Exception _ 100)))
                           opts))
          (recur (inc idx) opts))))))

(defn handle-history
  "HISTORY command: get the history of a key with pagination.
   Usage: HISTORY key [CURSOR cursor] [SINCE timestamp] [COUNT count]
   Returns: [next-cursor [[timestamp1 value1] [timestamp2 value2]...]]
   This is a ChronDB extension for time-travel queries."
  [storage writer args]
  (if (empty? args)
    (write-error writer "ERR wrong number of arguments for 'history' command")
    (try
      (let [key (first args)
            opts (parse-history-options (vec (rest args)))
            cursor-data (decode-history-cursor (:cursor opts))
            offset (:offset cursor-data 0)
            since-ts (:since opts)
            count-limit (:count opts)
            ;; Get document history from storage
            full-history (storage/get-document-history storage key)
            ;; Filter by timestamp if SINCE is provided
            filtered (if since-ts
                       (filter (fn [entry]
                                 (when-let [ts (or (:timestamp entry)
                                                   (:commit-time entry))]
                                   (>= ts since-ts)))
                               full-history)
                       full-history)
            ;; Apply pagination
            paged (take count-limit (drop offset filtered))
            ;; Format results as [[timestamp value]...]
            results (vec (map (fn [entry]
                                (let [ts (or (:timestamp entry)
                                             (:commit-time entry)
                                             0)
                                      value (or (:content entry)
                                                (:value entry)
                                                (json/write-str entry))]
                                  [(str ts)
                                   (if (map? value)
                                     (json/write-str value)
                                     (str value))]))
                              paged))
            ;; Calculate next cursor
            new-offset (+ offset count-limit)
            has-more (< new-offset (count filtered))
            next-cursor (if has-more
                          (encode-history-cursor {:offset new-offset})
                          "0")]
        (write-array writer [next-cursor results]))
      (catch Exception e
        (write-error writer (str "ERR " (.getMessage e)))))))

(declare handle-hello handle-scan handle-hscan handle-sscan handle-history)

(defn process-command
  "Process a Redis command with the given arguments.
   ctx-atom is an atom containing the ConnectionContext for this connection."
  [storage index writer command args ctx-atom]
  (let [cmd (str/lower-case command)]
    (case cmd
      "ping" (handle-ping writer args)
      "echo" (handle-echo writer args)
      "get" (handle-get storage writer args)
      "set" (handle-set storage index writer args)
      "setex" (handle-setex storage index writer args)
      "setnx" (handle-setnx storage index writer args)
      "del" (handle-del storage writer args)
      "exists" (handle-exists storage writer args)
      "hset" (handle-hset storage writer args)
      "hget" (handle-hget storage writer args)
      "hmset" (handle-hmset storage writer args)
      "hmget" (handle-hmget storage writer args)
      "hgetall" (handle-hgetall storage writer args)
      "lpush" (handle-lpush storage writer args)
      "rpush" (handle-rpush storage writer args)
      "lrange" (handle-lrange storage writer args)
      "lpop" (handle-lpop storage writer args)
      "rpop" (handle-rpop storage writer args)
      "llen" (handle-llen storage writer args)
      "sadd" (handle-sadd storage writer args)
      "smembers" (handle-smembers storage writer args)
      "sismember" (handle-sismember storage writer args)
      "srem" (handle-srem storage writer args)
      "zadd" (handle-zadd storage writer args)
      "zrange" (handle-zrange storage writer args)
      "zrank" (handle-zrank storage writer args)
      "zscore" (handle-zscore storage writer args)
      "zrem" (handle-zrem storage writer args)
      "search" (handle-search storage index writer args)
      "ft.search" (handle-ft-search storage index writer args)
      "command" (handle-command storage index writer args)
      "info" (handle-info writer args)
      "hello" (handle-hello writer args ctx-atom)
      "scan" (handle-scan storage writer args)
      "hscan" (handle-hscan storage writer args)
      "sscan" (handle-sscan storage writer args)
      "history" (handle-history storage writer args)
      (write-error writer (str "unknown command '" cmd "'")))))

;; Client Connection Handling
(defn handle-client [client-socket storage index]
  (async/go
    (try
      (let [ctx-atom (atom (create-connection-context))]
        (with-open [reader (BufferedReader. (InputStreamReader. (.getInputStream client-socket) StandardCharsets/UTF_8))
                    writer (BufferedWriter. (OutputStreamWriter. (.getOutputStream client-socket) StandardCharsets/UTF_8))]
          (loop []
            (let [command-array (read-resp reader)]
              (when command-array
                (let [command (first command-array)
                      args (vec (rest command-array))]
                  (process-command storage index writer command args ctx-atom)
                  (.flush writer)
                  (recur)))))))
      (catch Exception e
        (log/log-error (str "Error handling client: " (.getMessage e))))
      (finally
        (.close client-socket)))))

;; Server
(defn accept-connections
  "Accept connections on the server socket and handle them."
  [^ServerSocket server-socket storage index]
  (loop []
    (when-not (.isClosed server-socket)
      (try
        (let [client-socket (.accept server-socket)]
          (handle-client client-socket storage index))
        (catch Exception e
          (log/log-error (str "Error accepting connection: " (.getMessage e)))))
      (recur))))

(defn start-redis-server
  "Starts a Redis protocol server for ChronDB.
 Parameters:
 - storage: The storage implementation
 - index: The index implementation
 - port: The port number to listen on (default: 6379)
 Returns: The server socket"
  ([storage index]
   (start-redis-server storage index 6379))
  ([storage index port]
   (let [^ServerSocket server-socket (ServerSocket. port)]
     (log/log-info "Starting chrondb server via redis protocol on port" port)
     (async/thread
       (accept-connections server-socket storage index))
     server-socket)))

(defn stop-redis-server
  "Stops the Redis server.
 Parameters:
 - server-socket: The server socket to close
 Returns: nil"
  [server-socket]
  (when server-socket
    (log/log-info "Stopping chrondb via redis protocol server")
    (.close server-socket)
    nil))
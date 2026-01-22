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
      (try
        (execute-redis-write storage "SET" args {:metadata {:key key}}
                             (fn []
                               (storage/save-document storage doc)
                               (when index (index/index-document index doc))
                               (.write writer RESP_OK)))
        (catch clojure.lang.ExceptionInfo e
          (let [data (ex-data e)]
            (if (= :validation-error (:type data))
              (let [_ (require 'chrondb.validation.errors)
                    format-fn (resolve 'chrondb.validation.errors/format-redis-error)]
                (write-error writer (format-fn (:namespace data) (:violations data))))
              (write-error writer (.getMessage e)))))
        (catch Exception e
          (write-error writer (.getMessage e)))))))

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

;; =============================================================================
;; Validation Schema Commands
;; =============================================================================

(defn handle-schema-set
  "SCHEMA.SET namespace schema-json [MODE strict|warning]
   Creates or updates a validation schema for a namespace."
  [storage writer args]
  (if (< (count args) 2)
    (write-error writer "ERR wrong number of arguments for 'schema.set' command")
    (try
      (let [namespace (first args)
            schema-json (second args)
            mode-arg (when (>= (count args) 4)
                       (let [key-arg (nth args 2)]
                         (when (= "mode" (str/lower-case key-arg))
                           (keyword (str/lower-case (nth args 3))))))
            mode (or mode-arg :strict)
            repository (:repository storage)
            _ (require 'chrondb.validation.storage)
            save-fn (resolve 'chrondb.validation.storage/save-validation-schema)
            schema-def (json/read-str schema-json)
            result (save-fn repository namespace schema-def mode nil nil)]
        (write-simple-string writer "OK"))
      (catch Exception e
        (write-error writer (str "Schema error: " (.getMessage e)))))))

(defn handle-schema-get
  "SCHEMA.GET namespace
   Gets the validation schema for a namespace."
  [storage writer args]
  (if (empty? args)
    (write-error writer "ERR wrong number of arguments for 'schema.get' command")
    (try
      (let [namespace (first args)
            repository (:repository storage)
            _ (require 'chrondb.validation.storage)
            get-fn (resolve 'chrondb.validation.storage/get-validation-schema)
            result (get-fn repository namespace nil)]
        (if result
          (write-bulk-string writer (json/write-str result))
          (.write writer RESP_NULL)))
      (catch Exception e
        (write-error writer (str "Schema error: " (.getMessage e)))))))

(defn handle-schema-del
  "SCHEMA.DEL namespace
   Deletes the validation schema for a namespace."
  [storage writer args]
  (if (empty? args)
    (write-error writer "ERR wrong number of arguments for 'schema.del' command")
    (try
      (let [namespace (first args)
            repository (:repository storage)
            _ (require 'chrondb.validation.storage)
            delete-fn (resolve 'chrondb.validation.storage/delete-validation-schema)
            result (delete-fn repository namespace nil)]
        (write-integer writer (if result 1 0)))
      (catch Exception e
        (write-error writer (str "Schema error: " (.getMessage e)))))))

(defn handle-schema-list
  "SCHEMA.LIST
   Lists all validation schemas."
  [storage writer _args]
  (try
    (let [repository (:repository storage)
          _ (require 'chrondb.validation.storage)
          list-fn (resolve 'chrondb.validation.storage/list-validation-schemas)
          result (list-fn repository nil)]
      (write-array writer (mapv #(json/write-str %) result)))
    (catch Exception e
      (write-error writer (str "Schema error: " (.getMessage e))))))

(defn handle-schema-validate
  "SCHEMA.VALIDATE namespace doc-json
   Validates a document against a namespace's schema (dry-run)."
  [storage writer args]
  (if (< (count args) 2)
    (write-error writer "ERR wrong number of arguments for 'schema.validate' command")
    (try
      (let [namespace (first args)
            doc-json (second args)
            repository (:repository storage)
            _ (require 'chrondb.validation.core)
            validate-fn (resolve 'chrondb.validation.core/dry-run-validate)
            doc (json/read-str doc-json :key-fn keyword)
            result (validate-fn repository namespace doc nil)]
        (write-bulk-string writer (json/write-str result)))
      (catch Exception e
        (write-error writer (str "Validation error: " (.getMessage e)))))))

(defn process-command
  "Process a Redis command with the given arguments"
  [storage index writer command args]
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
      ;; Schema validation commands
      "schema.set" (handle-schema-set storage writer args)
      "schema.get" (handle-schema-get storage writer args)
      "schema.del" (handle-schema-del storage writer args)
      "schema.list" (handle-schema-list storage writer args)
      "schema.validate" (handle-schema-validate storage writer args)
      (write-error writer (str "unknown command '" cmd "'")))))

;; Client Connection Handling
(defn handle-client [client-socket storage index]
  (async/go
    (try
      (with-open [reader (BufferedReader. (InputStreamReader. (.getInputStream client-socket) StandardCharsets/UTF_8))
                  writer (BufferedWriter. (OutputStreamWriter. (.getOutputStream client-socket) StandardCharsets/UTF_8))]
        (loop []
          (let [command-array (read-resp reader)]
            (when command-array
              (let [command (first command-array)
                    args (vec (rest command-array))]
                (process-command storage index writer command args)
                (.flush writer)
                (recur))))))
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
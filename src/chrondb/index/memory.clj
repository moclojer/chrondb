(ns chrondb.index.memory
  "In-memory implementation of the Index protocol.
   This implementation stores documents in memory and provides basic search capabilities."
  (:require [chrondb.index.protocol :as protocol]
            [clojure.string :as str]
            [chrondb.util.logging :as log])
  (:import [java.util.concurrent ConcurrentHashMap]))

;; This function is currently unused
#_(defn- document-matches?
    "Checks if a document matches the given query string.
   Simple implementation that checks if any field contains the query string."
    [doc query]
    (let [query-lower (str/lower-case query)]
      (some (fn [[_ v]]
              (and (string? v)
                   (str/includes? (str/lower-case v) query-lower)))
            doc)))

(defrecord MemoryIndex [^ConcurrentHashMap data]
  protocol/Index
  (index-document [_ doc]
    (let [id (:id doc)]
      (.put data id doc)
      doc))

  (delete-document [_ id]
    (.remove data id)
    nil)

  (search [_ field query-string branch]
    (log/log-warn (str "MemoryIndex search called (field: " field ", query: " query-string ", branch: " branch "). Basic string matching performed, not full FTS."))
    (let [all-docs (seq (.values data))
          query-lower (str/lower-case query-string)]
      (filter
       (fn [doc]
         (some (fn [[k v]]
                 (when (string? v)
                   (cond
                     (= (name k) field) (str/includes? (str/lower-case v) query-lower)
                     (= field "content") (str/includes? (str/lower-case v) query-lower)
                     :else false)))
               doc))
       all-docs))))

(defn create-memory-index
  "Creates a new in-memory index.
   Returns: A new MemoryIndex instance."
  []
  (->MemoryIndex (ConcurrentHashMap.)))
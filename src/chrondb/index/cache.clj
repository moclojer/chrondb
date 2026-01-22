;; This file is part of ChronDB.
;;
;; ChronDB is free software: you can redistribute it and/or modify
;; it under the terms of the GNU Affero General Public License as published
;; by the Free Software Foundation, either version 3 of the License,
;; or (at your option) any later version.
(ns chrondb.index.cache
  "Query result cache for ChronDB.
   Provides LRU cache with TTL for frequently executed queries."
  (:require [chrondb.util.logging :as log])
  (:import [java.util LinkedHashMap Collections Map Map$Entry]
           [java.time Instant]))

;; Cache entry structure
(defrecord CacheEntry [value created-at accessed-at hit-count])

(defn- cache-entry [value]
  (let [now (Instant/now)]
    (->CacheEntry value now now 1)))

(defn- touch-entry [^CacheEntry entry]
  (->CacheEntry (:value entry)
                (:created-at entry)
                (Instant/now)
                (inc (:hit-count entry))))

(defn- entry-expired?
  "Check if a cache entry has expired based on TTL."
  [^CacheEntry entry ttl-ms]
  (let [age-ms (- (System/currentTimeMillis)
                  (.toEpochMilli ^Instant (:created-at entry)))]
    (> age-ms ttl-ms)))

;; LRU Cache with TTL
(defprotocol QueryCache
  "Protocol for query result caching."
  (cache-get [this key] "Get a value from the cache")
  (cache-put! [this key value] "Put a value in the cache")
  (cache-invalidate! [this key] "Invalidate a specific key")
  (cache-invalidate-all! [this] "Invalidate all entries")
  (cache-invalidate-branch! [this branch] "Invalidate all entries for a branch")
  (cache-stats [this] "Get cache statistics"))

(defn- create-lru-map
  "Create a thread-safe LRU LinkedHashMap."
  [max-size]
  (Collections/synchronizedMap
   (proxy [LinkedHashMap] [16 0.75 true]
     (removeEldestEntry [^Map$Entry eldest]
       (> (.size ^Map this) max-size)))))

(defrecord LRUQueryCache [^Map cache max-size ttl-ms
                          ^java.util.concurrent.atomic.AtomicLong hits
                          ^java.util.concurrent.atomic.AtomicLong misses]
  QueryCache
  (cache-get [_ key]
    (if-let [^CacheEntry entry (.get cache key)]
      (if (entry-expired? entry ttl-ms)
        (do
          (.remove cache key)
          (.incrementAndGet misses)
          nil)
        (do
          (.put cache key (touch-entry entry))
          (.incrementAndGet hits)
          (:value entry)))
      (do
        (.incrementAndGet misses)
        nil)))

  (cache-put! [_ key value]
    (.put cache key (cache-entry value))
    value)

  (cache-invalidate! [_ key]
    (.remove cache key)
    nil)

  (cache-invalidate-all! [_]
    (.clear cache)
    nil)

  (cache-invalidate-branch! [_ branch]
    ;; Remove all entries where key contains the branch
    (let [keys-to-remove (filter #(and (vector? %)
                                       (= branch (second %)))
                                 (.keySet cache))]
      (doseq [k keys-to-remove]
        (.remove cache k)))
    nil)

  (cache-stats [_]
    (let [h (.get hits)
          m (.get misses)
          total (+ h m)]
      {:hits h
       :misses m
       :total total
       :hit-rate (if (zero? total) 0.0 (/ (double h) total))
       :size (.size cache)
       :max-size max-size
       :ttl-ms ttl-ms})))

(defn create-query-cache
  "Create a new query cache.

   Options:
   - :max-size - Maximum number of entries (default 1000)
   - :ttl-ms   - Time-to-live in milliseconds (default 60000)"
  [& {:keys [max-size ttl-ms]
      :or {max-size 1000
           ttl-ms 60000}}]
  (->LRUQueryCache (create-lru-map max-size)
                   max-size
                   ttl-ms
                   (java.util.concurrent.atomic.AtomicLong. 0)
                   (java.util.concurrent.atomic.AtomicLong. 0)))

;; Cache key generation
(defn cache-key
  "Generate a cache key from query parameters."
  [query-map branch & [opts]]
  [(hash query-map) branch (hash opts)])

;; Cached search function
(defn cached-search
  "Execute a search with caching.

   Parameters:
   - cache: QueryCache instance
   - index: Index implementation
   - query-map: Query AST
   - branch: Git branch
   - opts: Search options
   - search-fn: Function to execute on cache miss"
  [cache index query-map branch opts search-fn]
  (let [key (cache-key query-map branch opts)]
    (if-let [cached (cache-get cache key)]
      (do
        (log/log-info "Cache hit for query")
        cached)
      (let [result (search-fn index query-map branch opts)]
        (log/log-info "Cache miss, executing query")
        (cache-put! cache key result)
        result))))

;; Decorator for index with caching
(defn wrap-index-with-cache
  "Wrap an index implementation with caching.
   Returns a map with the wrapped search-query function."
  [index cache]
  {:index index
   :cache cache
   :search-query-cached
   (fn [query-map branch opts]
     (cached-search cache index query-map branch opts
                    (fn [idx q b o]
                      (when-let [search-fn (resolve 'chrondb.index.protocol/search-query)]
                        (search-fn idx q b o)))))})

;; Cache warmup
(defn warmup-cache!
  "Warm up the cache with common queries.

   Parameters:
   - cache: QueryCache instance
   - index: Index implementation
   - queries: Sequence of {:query-map :branch :opts} maps"
  [cache index queries]
  (log/log-info (str "Warming up cache with " (count queries) " queries"))
  (doseq [{:keys [query-map branch opts]} queries]
    (cached-search cache index query-map branch opts
                   (fn [idx q b o]
                     (when-let [search-fn (resolve 'chrondb.index.protocol/search-query)]
                       (search-fn idx q b o))))))

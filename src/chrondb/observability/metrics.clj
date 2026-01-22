;; This file is part of ChronDB.
;;
;; ChronDB is free software: you can redistribute it and/or modify
;; it under the terms of the GNU Affero General Public License as published
;; by the Free Software Foundation, either version 3 of the License,
;; or (at your option) any later version.
(ns chrondb.observability.metrics
  "Metrics collection for ChronDB.
   Provides Prometheus-compatible metrics."
  (:require [clojure.string :as str])
  (:import [java.util.concurrent.atomic AtomicLong]
           [java.util.concurrent ConcurrentHashMap]))

;; Helper function - must be defined before records
(defn- labels->str [labels]
  (str/join "," (map (fn [[k v]] (str (clojure.core/name k) "=\"" v "\"")) labels)))

;; Metric types
(defprotocol Metric
  "Protocol for all metric types."
  (metric-name [this] "Get the metric name")
  (metric-type [this] "Get the metric type")
  (metric-help [this] "Get the metric help text")
  (metric-value [this] "Get current value(s)")
  (to-prometheus [this] "Format as Prometheus text"))

;; Counter - monotonically increasing value
(defrecord Counter [name help ^AtomicLong value labels]
  Metric
  (metric-name [_] name)
  (metric-type [_] "counter")
  (metric-help [_] help)
  (metric-value [_] (.get value))
  (to-prometheus [_]
    (str "# HELP " name " " help "\n"
         "# TYPE " name " counter\n"
         name (when (seq labels) (str "{" (labels->str labels) "}"))
         " " (.get value) "\n")))

(defn inc-counter!
  "Increment a counter by 1 or a specified amount."
  ([^Counter counter] (inc-counter! counter 1))
  ([^Counter counter amount]
   (.addAndGet ^AtomicLong (:value counter) amount)))

(defn create-counter
  "Create a new counter metric."
  [name help & {:keys [labels] :or {labels {}}}]
  (->Counter name help (AtomicLong. 0) labels))

;; Gauge - value that can go up or down
(defrecord Gauge [name help ^AtomicLong value labels]
  Metric
  (metric-name [_] name)
  (metric-type [_] "gauge")
  (metric-help [_] help)
  (metric-value [_] (.get value))
  (to-prometheus [_]
    (str "# HELP " name " " help "\n"
         "# TYPE " name " gauge\n"
         name (when (seq labels) (str "{" (labels->str labels) "}"))
         " " (.get value) "\n")))

(defn set-gauge!
  "Set gauge to a specific value."
  [^Gauge gauge value]
  (.set ^AtomicLong (:value gauge) (long value)))

(defn inc-gauge!
  "Increment gauge by 1 or specified amount."
  ([^Gauge gauge] (inc-gauge! gauge 1))
  ([^Gauge gauge amount]
   (.addAndGet ^AtomicLong (:value gauge) amount)))

(defn dec-gauge!
  "Decrement gauge by 1 or specified amount."
  ([^Gauge gauge] (dec-gauge! gauge 1))
  ([^Gauge gauge amount]
   (.addAndGet ^AtomicLong (:value gauge) (- amount))))

(defn create-gauge
  "Create a new gauge metric."
  [name help & {:keys [labels] :or {labels {}}}]
  (->Gauge name help (AtomicLong. 0) labels))

;; Histogram - distribution of values
(defrecord Histogram [name help buckets ^ConcurrentHashMap bucket-counts
                      ^AtomicLong sum ^AtomicLong count labels]
  Metric
  (metric-name [_] name)
  (metric-type [_] "histogram")
  (metric-help [_] help)
  (metric-value [_]
    {:buckets (into {} (map (fn [b] [b (.get (.getOrDefault bucket-counts b (AtomicLong. 0)))]) buckets))
     :sum (.get sum)
     :count (.get count)})
  (to-prometheus [_]
    (let [label-str (when (seq labels) (str "{" (labels->str labels) "}"))
          bucket-lines (map (fn [b]
                              (let [bucket-count (.get (.getOrDefault bucket-counts b (AtomicLong. 0)))]
                                (str name "_bucket{le=\"" b "\"" (when (seq labels) (str "," (labels->str labels))) "} " bucket-count)))
                            buckets)
          inf-count (.get count)]
      (str "# HELP " name " " help "\n"
           "# TYPE " name " histogram\n"
           (str/join "\n" bucket-lines) "\n"
           name "_bucket{le=\"+Inf\"" (when (seq labels) (str "," (labels->str labels))) "} " inf-count "\n"
           name "_sum" label-str " " (.get sum) "\n"
           name "_count" label-str " " (.get count) "\n"))))

(defn observe-histogram!
  "Observe a value in the histogram."
  [^Histogram histogram value]
  (let [value-double (double value)]
    ;; Update sum and count
    (.addAndGet ^AtomicLong (:sum histogram) (long (* value-double 1000000))) ; store as microseconds
    (.incrementAndGet ^AtomicLong (:count histogram))
    ;; Update buckets
    (doseq [bucket (:buckets histogram)]
      (when (<= value-double bucket)
        (let [bucket-counter (.computeIfAbsent ^ConcurrentHashMap (:bucket-counts histogram)
                                               bucket
                                               (reify java.util.function.Function
                                                 (apply [_ _] (AtomicLong. 0))))]
          (.incrementAndGet ^AtomicLong bucket-counter))))))

(defn create-histogram
  "Create a new histogram metric with specified buckets."
  [name help & {:keys [buckets labels]
                :or {buckets [0.001 0.005 0.01 0.025 0.05 0.1 0.25 0.5 1.0 2.5 5.0 10.0]
                     labels {}}}]
  (->Histogram name help (vec (sort buckets)) (ConcurrentHashMap.) (AtomicLong. 0) (AtomicLong. 0) labels))

;; Timer macro for measuring latency
(defmacro with-timer
  "Measure execution time and observe in histogram."
  [histogram & body]
  `(let [start# (System/nanoTime)
         result# (do ~@body)
         elapsed# (/ (- (System/nanoTime) start#) 1000000000.0)]
     (observe-histogram! ~histogram elapsed#)
     result#))

;; Registry for all metrics
(defonce ^:private metrics-registry (atom {}))

(defn register-metric!
  "Register a metric in the global registry."
  [metric]
  (swap! metrics-registry assoc (metric-name metric) metric)
  metric)

(defn get-metric
  "Get a metric by name from the registry."
  [name]
  (get @metrics-registry name))

(defn all-metrics
  "Get all registered metrics."
  []
  (vals @metrics-registry))

(defn metrics->prometheus
  "Format all metrics as Prometheus text."
  []
  (str/join "\n" (map to-prometheus (all-metrics))))

(defn export-all-metrics
  "Export all metrics in Prometheus format."
  []
  (metrics->prometheus))

;; Pre-defined ChronDB metrics
(defonce chrondb-metrics
  {:write-latency (register-metric!
                   (create-histogram "chrondb_write_latency_seconds"
                                     "Write operation latency in seconds"))
   :read-latency (register-metric!
                  (create-histogram "chrondb_read_latency_seconds"
                                    "Read operation latency in seconds"))
   :query-latency (register-metric!
                   (create-histogram "chrondb_query_latency_seconds"
                                     "Query execution latency in seconds"))
   :documents-saved (register-metric!
                     (create-counter "chrondb_documents_saved_total"
                                     "Total number of documents saved"))
   :documents-deleted (register-metric!
                       (create-counter "chrondb_documents_deleted_total"
                                       "Total number of documents deleted"))
   :documents-read (register-metric!
                    (create-counter "chrondb_documents_read_total"
                                    "Total number of documents read"))
   :queries-executed (register-metric!
                      (create-counter "chrondb_queries_executed_total"
                                      "Total number of queries executed"))
   :active-connections (register-metric!
                        (create-gauge "chrondb_active_connections"
                                      "Number of active connections"))
   :index-size-docs (register-metric!
                     (create-gauge "chrondb_index_documents"
                                   "Number of documents in the index"))
   :wal-pending-entries (register-metric!
                         (create-gauge "chrondb_wal_pending_entries"
                                       "Number of pending WAL entries"))
   :occ-conflicts (register-metric!
                   (create-counter "chrondb_occ_conflicts_total"
                                   "Total number of OCC conflicts"))
   :occ-retries (register-metric!
                 (create-counter "chrondb_occ_retries_total"
                                 "Total number of OCC retries"))})

;; Convenience functions for ChronDB metrics
(defn record-write! [latency-seconds]
  (observe-histogram! (:write-latency chrondb-metrics) latency-seconds)
  (inc-counter! (:documents-saved chrondb-metrics)))

(defn record-read! [latency-seconds]
  (observe-histogram! (:read-latency chrondb-metrics) latency-seconds)
  (inc-counter! (:documents-read chrondb-metrics)))

(defn record-query! [latency-seconds]
  (observe-histogram! (:query-latency chrondb-metrics) latency-seconds)
  (inc-counter! (:queries-executed chrondb-metrics)))

(defn record-delete! []
  (inc-counter! (:documents-deleted chrondb-metrics)))

(defn record-occ-conflict! []
  (inc-counter! (:occ-conflicts chrondb-metrics)))

(defn record-occ-retry! []
  (inc-counter! (:occ-retries chrondb-metrics)))

(defn set-active-connections! [n]
  (set-gauge! (:active-connections chrondb-metrics) n))

(defn set-index-size! [n]
  (set-gauge! (:index-size-docs chrondb-metrics) n))

(defn set-wal-pending! [n]
  (set-gauge! (:wal-pending-entries chrondb-metrics) n))

;; Aliases for test compatibility
(def write-latency (delay (:write-latency chrondb-metrics)))
(def read-latency (delay (:read-latency chrondb-metrics)))
(def query-latency (delay (:query-latency chrondb-metrics)))
(def active-transactions (delay (:active-connections chrondb-metrics)))
(def cache-hits (delay (get chrondb-metrics :cache-hits (create-counter "chrondb_cache_hits" "Cache hits"))))
(def cache-misses (delay (get chrondb-metrics :cache-misses (create-counter "chrondb_cache_misses" "Cache misses"))))

;; Generic metric operations
(defn get-value [metric]
  (metric-value metric))

(defn inc!
  ([metric] (cond
              (instance? Counter metric) (inc-counter! metric)
              (instance? Gauge metric) (inc-gauge! metric)))
  ([metric amount] (cond
                     (instance? Counter metric) (inc-counter! metric amount)
                     (instance? Gauge metric) (inc-gauge! metric amount))))

(defn dec!
  ([metric] (dec-gauge! metric))
  ([metric amount] (dec-gauge! metric amount)))

(defn set-value! [metric value]
  (set-gauge! metric value))

(defn observe! [metric value]
  (observe-histogram! metric value))

(defn prometheus-format [metric]
  (to-prometheus metric))

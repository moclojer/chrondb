;; This file is part of ChronDB.
;;
;; ChronDB is free software: you can redistribute it and/or modify
;; it under the terms of the GNU Affero General Public License as published
;; by the Free Software Foundation, either version 3 of the License,
;; or (at your option) any later version.
(ns chrondb.observability.health
  "Health check endpoints for ChronDB.
   Provides Kubernetes-compatible liveness and readiness probes."
  (:require [clojure.data.json :as json])
  (:import [java.io File]
           [java.time Instant]))

;; Health status types
(def ^:const STATUS-HEALTHY :healthy)
(def ^:const STATUS-UNHEALTHY :unhealthy)
(def ^:const STATUS-DEGRADED :degraded)

;; Aliases for compatibility
(def ^:const HEALTHY :healthy)
(def ^:const UNHEALTHY :unhealthy)
(def ^:const DEGRADED :degraded)

(defn- check-result
  "Create a health check result map."
  [component status & {:keys [message details latency-ms]}]
  (cond-> {:component component
           :status status
           :timestamp (.toString (Instant/now))}
    message (assoc :message message)
    details (assoc :details details)
    latency-ms (assoc :latency_ms latency-ms)))

;; Individual health checks
(defn check-storage
  "Check if storage is accessible."
  [storage]
  (let [start (System/currentTimeMillis)]
    (try
      ;; Try to read a non-existent document (should return nil, not throw)
      (when-let [get-fn (resolve 'chrondb.storage.protocol/get-document)]
        (get-fn storage "__health_check_probe__" "main"))
      (check-result :storage STATUS-HEALTHY
                    :latency-ms (- (System/currentTimeMillis) start))
      (catch Exception e
        (check-result :storage STATUS-UNHEALTHY
                      :message (.getMessage e)
                      :latency-ms (- (System/currentTimeMillis) start))))))

(defn check-index
  "Check if index is accessible."
  [index]
  (let [start (System/currentTimeMillis)]
    (try
      ;; Try a simple search
      (when-let [search-fn (resolve 'chrondb.index.protocol/search)]
        (search-fn index "id" "__health_check_probe__" "main"))
      (check-result :index STATUS-HEALTHY
                    :latency-ms (- (System/currentTimeMillis) start))
      (catch Exception e
        (check-result :index STATUS-UNHEALTHY
                      :message (.getMessage e)
                      :latency-ms (- (System/currentTimeMillis) start))))))

(defn check-wal
  "Check WAL health."
  [wal]
  (let [start (System/currentTimeMillis)]
    (try
      (if wal
        (let [pending-fn (resolve 'chrondb.wal.core/pending-entries)
              pending (when pending-fn (pending-fn wal))
              pending-count (count pending)]
          (if (< pending-count 100)
            (check-result :wal STATUS-HEALTHY
                          :details {:pending-entries pending-count}
                          :latency-ms (- (System/currentTimeMillis) start))
            (check-result :wal STATUS-DEGRADED
                          :message (str "High number of pending WAL entries: " pending-count)
                          :details {:pending-entries pending-count}
                          :latency-ms (- (System/currentTimeMillis) start))))
        (check-result :wal STATUS-HEALTHY
                      :message "WAL not configured"
                      :latency-ms (- (System/currentTimeMillis) start)))
      (catch Exception e
        (check-result :wal STATUS-UNHEALTHY
                      :message (.getMessage e)
                      :latency-ms (- (System/currentTimeMillis) start))))))

(defn check-disk-space
  "Check available disk space."
  [data-dir & {:keys [warning-threshold-gb critical-threshold-gb]
               :or {warning-threshold-gb 5
                    critical-threshold-gb 1}}]
  (let [start (System/currentTimeMillis)]
    (try
      (let [dir (File. data-dir)
            free-bytes (.getUsableSpace dir)
            free-gb (/ free-bytes 1073741824.0)
            total-bytes (.getTotalSpace dir)
            total-gb (/ total-bytes 1073741824.0)
            used-percent (* 100 (- 1 (/ free-bytes (max total-bytes 1))))]
        (cond
          (< free-gb critical-threshold-gb)
          (check-result :disk STATUS-UNHEALTHY
                        :message (format "Critical: Only %.2f GB free" free-gb)
                        :details {:free-gb free-gb
                                  :total-gb total-gb
                                  :used-percent used-percent}
                        :latency-ms (- (System/currentTimeMillis) start))

          (< free-gb warning-threshold-gb)
          (check-result :disk STATUS-DEGRADED
                        :message (format "Warning: Only %.2f GB free" free-gb)
                        :details {:free-gb free-gb
                                  :total-gb total-gb
                                  :used-percent used-percent}
                        :latency-ms (- (System/currentTimeMillis) start))

          :else
          (check-result :disk STATUS-HEALTHY
                        :details {:free-gb free-gb
                                  :total-gb total-gb
                                  :used-percent used-percent}
                        :latency-ms (- (System/currentTimeMillis) start))))
      (catch Exception e
        (check-result :disk STATUS-UNHEALTHY
                      :message (.getMessage e)
                      :latency-ms (- (System/currentTimeMillis) start))))))

(defn check-memory
  "Check JVM memory usage."
  [& {:keys [warning-threshold-percent critical-threshold-percent]
      :or {warning-threshold-percent 85
           critical-threshold-percent 95}}]
  (let [start (System/currentTimeMillis)
        runtime (Runtime/getRuntime)
        max-memory (.maxMemory runtime)
        total-memory (.totalMemory runtime)
        free-memory (.freeMemory runtime)
        used-memory (- total-memory free-memory)
        used-percent (* 100.0 (/ used-memory max-memory))]
    (cond
      (> used-percent critical-threshold-percent)
      (check-result :memory STATUS-UNHEALTHY
                    :message (format "Critical: %.1f%% memory used" used-percent)
                    :details {:used-mb (/ used-memory 1048576)
                              :max-mb (/ max-memory 1048576)
                              :used-percent used-percent}
                    :latency-ms (- (System/currentTimeMillis) start))

      (> used-percent warning-threshold-percent)
      (check-result :memory STATUS-DEGRADED
                    :message (format "Warning: %.1f%% memory used" used-percent)
                    :details {:used-mb (/ used-memory 1048576)
                              :max-mb (/ max-memory 1048576)
                              :used-percent used-percent}
                    :latency-ms (- (System/currentTimeMillis) start))

      :else
      (check-result :memory STATUS-HEALTHY
                    :details {:used-mb (/ used-memory 1048576)
                              :max-mb (/ max-memory 1048576)
                              :used-percent used-percent}
                    :latency-ms (- (System/currentTimeMillis) start)))))

;; Aggregate health check
(defn- aggregate-status* [checks]
  (cond
    (some #(= STATUS-UNHEALTHY (:status %)) checks) STATUS-UNHEALTHY
    (some #(= STATUS-DEGRADED (:status %)) checks) STATUS-DEGRADED
    :else STATUS-HEALTHY))

(defn full-health-check
  "Run all health checks and return aggregate result."
  [{:keys [storage index wal data-dir]}]
  (let [start (System/currentTimeMillis)
        checks [(check-storage storage)
                (check-index index)
                (check-wal wal)
                (check-disk-space (or data-dir "."))
                (check-memory)]
        status (aggregate-status* checks)]
    {:status status
     :timestamp (.toString (Instant/now))
     :total-latency-ms (- (System/currentTimeMillis) start)
     :checks checks}))

;; Ring handlers for health endpoints
(defn health-handler
  "Full health check endpoint handler."
  [components]
  (fn [_request]
    (let [result (full-health-check components)
          status-code (case (:status result)
                        :healthy 200
                        :degraded 200  ; Still return 200 for degraded
                        :unhealthy 503)]
      {:status status-code
       :headers {"Content-Type" "application/json"}
       :body (json/write-str result)})))

(defn liveness-handler
  "Kubernetes liveness probe - is the process alive?
   Returns 200 if the JVM is running, regardless of component health."
  [_request]
  {:status 200
   :headers {"Content-Type" "application/json"}
   :body (json/write-str {:status "alive"
                          :timestamp (.toString (Instant/now))})})

(defn readiness-handler
  "Kubernetes readiness probe - is the service ready to accept traffic?"
  [components]
  (fn [_request]
    (let [storage-check (check-storage (:storage components))
          index-check (check-index (:index components))
          ready? (and (= STATUS-HEALTHY (:status storage-check))
                      (#{STATUS-HEALTHY STATUS-DEGRADED} (:status index-check)))]
      {:status (if ready? 200 503)
       :headers {"Content-Type" "application/json"}
       :body (json/write-str {:status (if ready? "ready" "not-ready")
                              :timestamp (.toString (Instant/now))
                              :checks [storage-check index-check]})})))

(defn startup-handler
  "Kubernetes startup probe - has the service finished starting?"
  [startup-complete-atom]
  (fn [_request]
    (if @startup-complete-atom
      {:status 200
       :headers {"Content-Type" "application/json"}
       :body (json/write-str {:status "started"
                              :timestamp (.toString (Instant/now))})}
      {:status 503
       :headers {"Content-Type" "application/json"}
       :body (json/write-str {:status "starting"
                              :timestamp (.toString (Instant/now))})})))

;; Generic health check framework

(defn create-health-check
  "Create a health check definition.
   Parameters:
   - name: Keyword identifying the check
   - description: Human-readable description
   - check-fn: Function that returns {:status :healthy/:degraded/:unhealthy ...}"
  [name description check-fn]
  {:name name
   :description description
   :check-fn check-fn})

(defn run-health-check
  "Execute a single health check and return result with timing."
  [{:keys [name check-fn]}]
  (let [start (System/currentTimeMillis)]
    (try
      (let [result (check-fn)]
        (assoc result
               :component name
               :duration-ms (- (System/currentTimeMillis) start)))
      (catch Exception e
        {:component name
         :status :unhealthy
         :message (str "Exception: " (.getMessage e))
         :duration-ms (- (System/currentTimeMillis) start)}))))

(defn aggregate-status
  "Aggregate multiple check results into a single status."
  [results]
  (cond
    (empty? results) :healthy
    (some #(= :unhealthy (:status %)) results) :unhealthy
    (some #(= :degraded (:status %)) results) :degraded
    :else :healthy))

(defrecord HealthChecker [checks startup-complete-atom])

(defn create-health-checker
  "Create a health checker with multiple checks."
  [checks]
  (->HealthChecker checks (atom true)))

(defn run-all-checks
  "Run all health checks and return aggregate result."
  [^HealthChecker checker]
  (let [results (mapv run-health-check (:checks checker))
        status (aggregate-status results)]
    {:status status
     :timestamp (.toString (Instant/now))
     :checks results}))

;; Predefined check factories
(defn disk-space-check
  "Create a disk space check function."
  [data-dir]
  (fn []
    (let [dir (File. data-dir)
          free-bytes (.getUsableSpace dir)
          free-gb (/ free-bytes 1073741824.0)
          total-bytes (.getTotalSpace dir)
          total-gb (/ total-bytes 1073741824.0)
          used-percent (* 100 (- 1 (/ free-bytes (max total-bytes 1))))]
      {:status (cond
                 (< free-gb 1) :unhealthy
                 (< free-gb 5) :degraded
                 :else :healthy)
       :total-gb total-gb
       :free-gb free-gb
       :used-percent used-percent})))

(defn memory-check
  "Create a memory check function."
  []
  (fn []
    (let [runtime (Runtime/getRuntime)
          max-memory (.maxMemory runtime)
          total-memory (.totalMemory runtime)
          free-memory (.freeMemory runtime)
          used-memory (- total-memory free-memory)
          used-percent (* 100.0 (/ used-memory max-memory))]
      {:status (cond
                 (> used-percent 95) :unhealthy
                 (> used-percent 85) :degraded
                 :else :healthy)
       :max-mb (/ max-memory 1048576)
       :used-mb (/ used-memory 1048576)
       :used-percent used-percent})))

;; HTTP status code mapping
(defn status->http-code
  "Convert health status to HTTP status code."
  [status]
  (case status
    :healthy 200
    :degraded 200
    :unhealthy 503
    503))

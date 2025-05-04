(ns chrondb.benchmark.sql-benchmark-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [chrondb.storage.git.core :as git-core]
            [chrondb.index.lucene :as lucene]
            [chrondb.benchmark.fixtures :as fixtures]
            [chrondb.api.sql.execution.query :as query]
            [clojure.string :as str]
            [clojure.java.io :as io]
            [chrondb.util.logging :as log])
  (:import (java.util UUID)))

(def benchmark-repo-path "/tmp/chrondb-benchmark-repo")
(def benchmark-index-path "/tmp/chrondb-benchmark-index")
(def ^:dynamic *test-storage* nil)
(def ^:dynamic *test-index* nil)
(def ^:dynamic *test-conn* nil)
(def num-test-docs 10000) ;; Adjust to generate ~1GB of data
(def user-limit 1000)
(def order-limit 2000)

(defn- cleanup-test-env []
  (let [repo-dir (io/file benchmark-repo-path)
        index-dir (io/file benchmark-index-path)]
    (when (.exists repo-dir)
      (doseq [f (file-seq repo-dir)]
        (when (and (.isFile f) (.canWrite f))
          (.delete f)))
      (.delete repo-dir))
    (when (.exists index-dir)
      (doseq [f (file-seq index-dir)]
        (when (and (.isFile f) (.canWrite f))
          (.delete f)))
      (.delete index-dir))))

(defn- setup-benchmark-env []
  (cleanup-test-env)
  (.mkdirs (io/file benchmark-repo-path))
  (.mkdirs (io/file benchmark-index-path))
  (let [storage (git-core/create-git-storage benchmark-repo-path)
        index (lucene/create-lucene-index benchmark-index-path)]
    [storage index]))

(defn- with-benchmark-env [f]
  (let [[storage index] (setup-benchmark-env)]
    (try
      (binding [*test-storage* storage
                *test-index* index]
        (f))
      (finally
        (when index (.close index))))))

(use-fixtures :once with-benchmark-env)

(defn- generate-data []
  (log/log-info "Starting benchmark data generation...")
  (let [table-name "benchmark_items"
        branch "main"]
    (fixtures/generate-benchmark-data *test-storage* *test-index* table-name num-test-docs branch)
    (log/log-info "Benchmark data generation complete.")

    ;; Create additional tables for JOIN testing
    (log/log-info "Creating additional tables for JOIN tests...")
    (let [out (java.io.ByteArrayOutputStream.)]
      ;; Generate users table
      (doseq [idx (range user-limit)]
        (let [id (str (UUID/randomUUID))
              query (str "INSERT INTO users (id, name, email, age) VALUES ('"
                         id "', 'User " idx "', 'user" idx "@example.com', " (+ 20 (rand-int 40)) ")")]
          (query/handle-query *test-storage* *test-index* out query)))

      ;; Generate orders table with references to benchmark_items and users
      (doseq [_ (range order-limit)]
        (let [id (str (UUID/randomUUID))
              item-index (rand-int num-test-docs)
              user-index (rand-int user-limit)
              query (str "INSERT INTO orders (id, user_id, item_id, order_date, quantity) VALUES ('"
                         id "', 'user" user-index "', 'item" item-index "', '"
                         (+ 20220101 (rand-int 10000)) "', " (inc (rand-int 5)) ")")]
          (query/handle-query *test-storage* *test-index* out query)))

      (log/log-info "Created additional tables for JOIN tests"))))

(defn- calculate-tps [operations-count elapsed-ms]
  (/ (* operations-count 1000.0) elapsed-ms))

(defn- run-select-benchmark []
  (let [out (java.io.ByteArrayOutputStream.)
        operations-count 1000
        start-time (System/currentTimeMillis)]
    (query/handle-query *test-storage* *test-index* out "SELECT * FROM benchmark_items LIMIT 1000")
    (let [elapsed (- (System/currentTimeMillis) start-time)
          tps (calculate-tps operations-count elapsed)]
      (log/log-info (format "SELECT 1000 items completed in %d ms (%.2f TPS)" elapsed tps))
      {:elapsed elapsed :tps tps})))

(defn- run-search-benchmark []
  (let [out (java.io.ByteArrayOutputStream.)
        operations-count 100
        search-term (str "%" (fixtures/generate-random-string 5) "%")
        query (str "SELECT * FROM benchmark_items WHERE description LIKE '" search-term "' LIMIT 100")
        start-time (System/currentTimeMillis)]
    (query/handle-query *test-storage* *test-index* out query)
    (let [elapsed (- (System/currentTimeMillis) start-time)
          tps (calculate-tps operations-count elapsed)]
      (log/log-info (format "Search query completed in %d ms (%.2f TPS)" elapsed tps))
      {:elapsed elapsed :tps tps})))

(defn- run-inner-join-benchmark []
  (let [out (java.io.ByteArrayOutputStream.)
        operations-count 100
        start-time (System/currentTimeMillis)
        query (str "SELECT o.id, u.name, i.title FROM orders o "
                   "INNER JOIN users u ON o.user_id = u.id "
                   "INNER JOIN benchmark_items i ON o.item_id = i.id "
                   "LIMIT 100")]
    (query/handle-query *test-storage* *test-index* out query)
    (let [elapsed (- (System/currentTimeMillis) start-time)
          tps (calculate-tps operations-count elapsed)]
      (log/log-info (format "INNER JOIN query completed in %d ms (%.2f TPS)" elapsed tps))
      {:elapsed elapsed :tps tps})))

(defn- run-left-join-benchmark []
  (let [out (java.io.ByteArrayOutputStream.)
        operations-count 100
        start-time (System/currentTimeMillis)
        query (str "SELECT u.id, u.name, o.id, o.order_date FROM users u "
                   "LEFT JOIN orders o ON u.id = o.user_id "
                   "LIMIT 100")]
    (query/handle-query *test-storage* *test-index* out query)
    (let [elapsed (- (System/currentTimeMillis) start-time)
          tps (calculate-tps operations-count elapsed)]
      (log/log-info (format "LEFT JOIN query completed in %d ms (%.2f TPS)" elapsed tps))
      {:elapsed elapsed :tps tps})))

(defn- run-insert-benchmark [num-inserts]
  (let [out (java.io.ByteArrayOutputStream.)
        operations-count num-inserts
        total-start-time (System/currentTimeMillis)
        insert-times (for [_ (range num-inserts)]
                       (let [id (str (UUID/randomUUID))
                             doc (fixtures/generate-large-document id "benchmark_items")
                             fields (-> (into [] (keys doc))
                                        (conj "id")
                                        distinct)
                             values (map #(str "'" (str/replace (str (get doc %)) "'" "''") "'") fields)
                             query (str "INSERT INTO benchmark_items (" (str/join ", " fields) ") VALUES (" (str/join ", " values) ")")
                             start-time (System/currentTimeMillis)]
                         (query/handle-query *test-storage* *test-index* out query)
                         (- (System/currentTimeMillis) start-time)))
        total-time (- (System/currentTimeMillis) total-start-time)
        avg-time (double (/ (reduce + insert-times) (count insert-times)))
        tps (calculate-tps operations-count total-time)]
    (log/log-info (format "Average INSERT time: %.2f ms (%.2f TPS)" avg-time tps))
    {:avg-time avg-time :tps tps}))

(defn- run-bulk-select-benchmark [num-queries]
  (let [out (java.io.ByteArrayOutputStream.)
        operations-count num-queries
        total-start-time (System/currentTimeMillis)
        query-times (for [_ (range num-queries)]
                      (let [offset (rand-int (- num-test-docs 50))
                            query (str "SELECT * FROM benchmark_items LIMIT 50 OFFSET " offset)
                            start-time (System/currentTimeMillis)]
                        (query/handle-query *test-storage* *test-index* out query)
                        (- (System/currentTimeMillis) start-time)))
        total-time (- (System/currentTimeMillis) total-start-time)
        avg-time (double (/ (reduce + query-times) (count query-times)))
        tps (calculate-tps operations-count total-time)]
    (log/log-info (format "Bulk SELECT benchmark: %d queries, avg time: %.2f ms, total time: %d ms (%.2f TPS)"
                          num-queries avg-time total-time tps))
    {:avg-time avg-time :total-time total-time :tps tps}))

(deftest ^:benchmark test-sql-performance
  (testing "SQL Benchmark with 1GB+ dataset"
    (log/log-info "=== Starting SQL Protocol Benchmark Test ===")

    ;; Generate benchmark dataset
    (generate-data)

    ;; Run benchmarks
    (log/log-info "Running SELECT benchmark...")
    (let [select-result (run-select-benchmark)
          search-result (run-search-benchmark)
          inner-join-result (run-inner-join-benchmark)
          left-join-result (run-left-join-benchmark)
          insert-result (run-insert-benchmark 10)
          bulk-select-result (run-bulk-select-benchmark 20)]

      (log/log-info "=== Benchmark Results ===")
      (log/log-info (format "SELECT 1000 records: %d ms (%.2f TPS)" (:elapsed select-result) (:tps select-result)))
      (log/log-info (format "SEARCH records: %d ms (%.2f TPS)" (:elapsed search-result) (:tps search-result)))
      (log/log-info (format "INNER JOIN query: %d ms (%.2f TPS)" (:elapsed inner-join-result) (:tps inner-join-result)))
      (log/log-info (format "LEFT JOIN query: %d ms (%.2f TPS)" (:elapsed left-join-result) (:tps left-join-result)))
      (log/log-info (format "Average INSERT time: %.2f ms (%.2f TPS)" (:avg-time insert-result) (:tps insert-result)))
      (log/log-info (format "Bulk SELECT (20 queries): avg %.2f ms (%.2f TPS)" (:avg-time bulk-select-result) (:tps bulk-select-result)))
      (log/log-info "=== Benchmark Complete ===")

      ;; We're just reporting the results, not making assertions
      ;; This way the test reports metrics but doesn't fail on performance
      (is true))))
(ns chrondb.benchmark.sql-benchmark-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [chrondb.storage.git :as git]
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
(def num-test-docs 10000)  ;; Adjust to generate ~1GB of data

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
  (let [storage (git/create-git-storage benchmark-repo-path)
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
      (doseq [idx (range 100)]
        (let [id (str (UUID/randomUUID))
              query (str "INSERT INTO users (id, name, email, age) VALUES ('"
                         id "', 'User " idx "', 'user" idx "@example.com', " (+ 20 (rand-int 40)) ")")]
          (query/handle-query *test-storage* *test-index* out query)))

      ;; Generate orders table with references to benchmark_items and users
      (doseq [_ (range 200)]
        (let [id (str (UUID/randomUUID))
              item-index (rand-int num-test-docs)
              user-index (rand-int 100)
              query (str "INSERT INTO orders (id, user_id, item_id, order_date, quantity) VALUES ('"
                         id "', 'user" user-index "', 'item" item-index "', '"
                         (+ 20220101 (rand-int 10000)) "', " (inc (rand-int 5)) ")")]
          (query/handle-query *test-storage* *test-index* out query)))

      (log/log-info "Created additional tables for JOIN tests"))))

(defn- run-select-benchmark []
  (let [out (java.io.ByteArrayOutputStream.)
        start-time (System/currentTimeMillis)]
    (query/handle-query *test-storage* *test-index* out "SELECT * FROM benchmark_items LIMIT 1000")
    (let [elapsed (- (System/currentTimeMillis) start-time)]
      (log/log-info (format "SELECT 1000 items completed in %d ms" elapsed))
      elapsed)))

(defn- run-search-benchmark []
  (let [out (java.io.ByteArrayOutputStream.)
        search-term (str "%" (fixtures/generate-random-string 5) "%")
        query (str "SELECT * FROM benchmark_items WHERE description LIKE '" search-term "' LIMIT 100")
        start-time (System/currentTimeMillis)]
    (query/handle-query *test-storage* *test-index* out query)
    (let [elapsed (- (System/currentTimeMillis) start-time)]
      (log/log-info (format "Search query completed in %d ms" elapsed))
      elapsed)))

(defn- run-inner-join-benchmark []
  (let [out (java.io.ByteArrayOutputStream.)
        start-time (System/currentTimeMillis)
        query (str "SELECT o.id, u.name, i.title FROM orders o "
                   "INNER JOIN users u ON o.user_id = u.id "
                   "INNER JOIN benchmark_items i ON o.item_id = i.id "
                   "LIMIT 100")]
    (query/handle-query *test-storage* *test-index* out query)
    (let [elapsed (- (System/currentTimeMillis) start-time)]
      (log/log-info (format "INNER JOIN query completed in %d ms" elapsed))
      elapsed)))

(defn- run-left-join-benchmark []
  (let [out (java.io.ByteArrayOutputStream.)
        start-time (System/currentTimeMillis)
        query (str "SELECT u.id, u.name, o.id, o.order_date FROM users u "
                   "LEFT JOIN orders o ON u.id = o.user_id "
                   "LIMIT 100")]
    (query/handle-query *test-storage* *test-index* out query)
    (let [elapsed (- (System/currentTimeMillis) start-time)]
      (log/log-info (format "LEFT JOIN query completed in %d ms" elapsed))
      elapsed)))

(defn- run-insert-benchmark [num-inserts]
  (let [out (java.io.ByteArrayOutputStream.)
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
        total-time (reduce + insert-times)
        avg-time (double (/ total-time (count insert-times)))]
    (log/log-info (format "Average INSERT time: %.2f ms" avg-time))
    avg-time))

(deftest ^:benchmark test-sql-performance
  (testing "SQL Benchmark with 1GB+ dataset"
    (log/log-info "=== Starting SQL Protocol Benchmark Test ===")

    ;; Generate benchmark dataset
    (generate-data)

    ;; Run benchmarks
    (log/log-info "Running SELECT benchmark...")
    (let [select-time (run-select-benchmark)
          search-time (run-search-benchmark)
          inner-join-time (run-inner-join-benchmark)
          left-join-time (run-left-join-benchmark)
          insert-time (run-insert-benchmark 10)]

      (log/log-info "=== Benchmark Results ===")
      (log/log-info (format "SELECT 1000 records: %d ms" select-time))
      (log/log-info (format "SEARCH records: %d ms" search-time))
      (log/log-info (format "INNER JOIN query: %d ms" inner-join-time))
      (log/log-info (format "LEFT JOIN query: %d ms" left-join-time))
      (log/log-info (format "Average INSERT time: %.2f ms" insert-time))
      (log/log-info "=== Benchmark Complete ===")

      ;; We're just reporting the results, not making assertions
      ;; This way the test reports metrics but doesn't fail on performance
      (is true))))
(ns chrondb.observability.metrics-test
  (:require [clojure.test :refer [deftest testing is]]
            [clojure.string :as str]
            [chrondb.observability.metrics :as metrics]))

(deftest test-counter-operations
  (testing "Counter increment"
    (let [counter (metrics/create-counter "test_counter" "Test counter")]
      (is (= 0 (metrics/get-value counter)))
      (metrics/inc! counter)
      (is (= 1 (metrics/get-value counter)))
      (metrics/inc! counter 5)
      (is (= 6 (metrics/get-value counter))))))

(deftest test-gauge-operations
  (testing "Gauge set and adjust"
    (let [gauge (metrics/create-gauge "test_gauge" "Test gauge")]
      (is (= 0 (metrics/get-value gauge)))
      (metrics/set-value! gauge 100)
      (is (= 100 (metrics/get-value gauge)))
      (metrics/inc! gauge)
      (is (= 101 (metrics/get-value gauge)))
      (metrics/dec! gauge 50)
      (is (= 51 (metrics/get-value gauge))))))

(deftest test-histogram-operations
  (testing "Histogram observe"
    (let [histogram (metrics/create-histogram "test_histogram" "Test histogram"
                                              :buckets [0.01 0.1 1.0 10.0])]
      (metrics/observe! histogram 0.05)
      (metrics/observe! histogram 0.5)
      (metrics/observe! histogram 5.0)
      (let [data (metrics/get-value histogram)]
        (is (= 3 (:count data)))
        (is (> (:sum data) 5.0))))))

(deftest test-with-timer-macro
  (testing "Timer measures execution time"
    (let [histogram (metrics/create-histogram "timer_test" "Timer test"
                                              :buckets [0.001 0.01 0.1 1.0])]
      (metrics/with-timer histogram
        (Thread/sleep 10))
      (let [data (metrics/get-value histogram)]
        (is (= 1 (:count data)))
        (is (> (:sum data) 0.01))))))

(deftest test-prometheus-format
  (testing "Counter prometheus format"
    (let [counter (metrics/create-counter "http_requests_total" "Total HTTP requests")]
      (metrics/inc! counter 42)
      (let [output (metrics/prometheus-format counter)]
        (is (str/includes? output "http_requests_total"))
        (is (str/includes? output "42")))))

  (testing "Gauge prometheus format"
    (let [gauge (metrics/create-gauge "memory_usage_bytes" "Memory usage")]
      (metrics/set-value! gauge 1024)
      (let [output (metrics/prometheus-format gauge)]
        (is (str/includes? output "memory_usage_bytes"))
        (is (str/includes? output "1024")))))

  (testing "Histogram prometheus format"
    (let [histogram (metrics/create-histogram "request_duration_seconds" "Request duration"
                                              :buckets [0.01 0.1 1.0])]
      (metrics/observe! histogram 0.05)
      (metrics/observe! histogram 0.5)
      (let [output (metrics/prometheus-format histogram)]
        (is (str/includes? output "request_duration_seconds_bucket"))
        (is (str/includes? output "request_duration_seconds_count"))
        (is (str/includes? output "request_duration_seconds_sum"))))))

(deftest test-default-metrics
  (testing "Default ChronDB metrics exist"
    ;; These should not throw
    (is (some? @metrics/write-latency))
    (is (some? @metrics/read-latency))
    (is (some? @metrics/query-latency))
    (is (some? @metrics/active-transactions))
    (is (some? @metrics/cache-hits))
    (is (some? @metrics/cache-misses))))

(deftest test-record-metrics
  (testing "Record write metric"
    ;; Should not throw
    (metrics/record-write! 0.1)
    (is true))

  (testing "Record read metric"
    (metrics/record-read! 0.05)
    (is true))

  (testing "Record query metric"
    (metrics/record-query! 0.2)
    (is true)))

(deftest test-export-all-metrics
  (testing "Export all metrics"
    ;; Record some metrics first
    (metrics/record-write! 0.1)
    (metrics/record-read! 0.05)
    (let [output (metrics/export-all-metrics)]
      (is (string? output))
      (is (str/includes? output "chrondb_write_latency"))
      (is (str/includes? output "chrondb_read_latency")))))

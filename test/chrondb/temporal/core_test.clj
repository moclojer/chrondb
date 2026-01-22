(ns chrondb.temporal.core-test
  (:require [clojure.test :refer [deftest testing is]]
            [chrondb.temporal.core :as temporal])
  (:import [java.time Instant]))

(deftest test-parse-timestamp
  (testing "Parse ISO-8601 timestamp"
    (let [ts (temporal/parse-timestamp "2024-01-15T10:30:00Z")]
      (is (instance? Instant ts))
      (is (= "2024-01-15T10:30:00Z" (str ts)))))

  (testing "Parse date only"
    (let [ts (temporal/parse-timestamp "2024-01-15")]
      (is (instance? Instant ts))
      (is (= "2024-01-15T00:00:00Z" (str ts)))))

  (testing "Parse Unix timestamp (seconds)"
    (let [ts (temporal/parse-timestamp 1705315800)]
      (is (instance? Instant ts))))

  (testing "Parse Unix timestamp (milliseconds)"
    (let [ts (temporal/parse-timestamp 1705315800000)]
      (is (instance? Instant ts))))

  (testing "Parse Unix timestamp as string"
    (let [ts (temporal/parse-timestamp "1705315800")]
      (is (instance? Instant ts))))

  (testing "Parse nil returns nil"
    (is (nil? (temporal/parse-timestamp nil))))

  (testing "Pass-through Instant"
    (let [instant (Instant/now)]
      (is (= instant (temporal/parse-timestamp instant))))))

(deftest test-as-of-query
  (testing "Create AS OF query"
    (let [query (temporal/as-of-query "2024-01-15T10:30:00Z")]
      (is (= :as-of (:type query)))
      (is (instance? Instant (:timestamp query))))))

(deftest test-between-query
  (testing "Create BETWEEN query"
    (let [query (temporal/between-query "2024-01-01T00:00:00Z" "2024-01-31T23:59:59Z")]
      (is (= :between (:type query)))
      (is (instance? Instant (:start query)))
      (is (instance? Instant (:end query)))
      (is (true? (:include-start query)))
      (is (true? (:include-end query)))))

  (testing "Create BETWEEN query with exclusions"
    (let [query (temporal/between-query "2024-01-01T00:00:00Z" "2024-01-31T23:59:59Z"
                                        :include-start false :include-end false)]
      (is (false? (:include-start query)))
      (is (false? (:include-end query))))))

(deftest test-from-to-query
  (testing "Create FROM TO query"
    (let [query (temporal/from-to-query "2024-01-01T00:00:00Z" "2024-01-31T23:59:59Z")]
      (is (= :from-to (:type query)))
      (is (instance? Instant (:start query)))
      (is (instance? Instant (:end query))))))

(deftest test-versions-query
  (testing "Create VERSIONS query"
    (let [query (temporal/versions-query "2024-01-01T00:00:00Z" "2024-01-31T23:59:59Z")]
      (is (= :versions (:type query)))
      (is (instance? Instant (:start query)))
      (is (instance? Instant (:end query))))))

(deftest test-find-commit-at-timestamp
  (testing "Find commit at timestamp"
    (let [commits [{:commit-id "abc" :commit-time "2024-01-10T00:00:00Z"}
                   {:commit-id "def" :commit-time "2024-01-15T00:00:00Z"}
                   {:commit-id "ghi" :commit-time "2024-01-20T00:00:00Z"}]
          result (temporal/find-commit-at-timestamp commits "2024-01-17T00:00:00Z")]
      (is (= "def" (:commit-id result)))))

  (testing "Find commit exactly at timestamp"
    (let [commits [{:commit-id "abc" :commit-time "2024-01-15T00:00:00Z"}]
          result (temporal/find-commit-at-timestamp commits "2024-01-15T00:00:00Z")]
      (is (= "abc" (:commit-id result)))))

  (testing "No commit before timestamp"
    (let [commits [{:commit-id "abc" :commit-time "2024-01-15T00:00:00Z"}]
          result (temporal/find-commit-at-timestamp commits "2024-01-01T00:00:00Z")]
      (is (nil? result)))))

(deftest test-filter-commits-in-range
  (testing "Filter commits in range"
    (let [commits [{:commit-id "a" :commit-time "2024-01-05T00:00:00Z"}
                   {:commit-id "b" :commit-time "2024-01-10T00:00:00Z"}
                   {:commit-id "c" :commit-time "2024-01-15T00:00:00Z"}
                   {:commit-id "d" :commit-time "2024-01-20T00:00:00Z"}]
          result (temporal/filter-commits-in-range
                  commits
                  {:start "2024-01-08T00:00:00Z"
                   :end "2024-01-18T00:00:00Z"})]
      (is (= 2 (count result)))
      (is (= ["b" "c"] (map :commit-id result))))))

(deftest test-document-valid-at
  (testing "Document valid at timestamp"
    (let [doc {:id "1" :_valid_from "2024-01-01T00:00:00Z" :_valid_to "2024-12-31T23:59:59Z"}]
      (is (true? (temporal/document-valid-at? doc "2024-06-15T00:00:00Z")))
      (is (false? (temporal/document-valid-at? doc "2025-01-01T00:00:00Z")))
      (is (false? (temporal/document-valid-at? doc "2023-12-31T00:00:00Z")))))

  (testing "Document with no valid time bounds"
    (let [doc {:id "1"}]
      (is (true? (temporal/document-valid-at? doc "2024-06-15T00:00:00Z"))))))

(deftest test-parse-temporal-clause
  (testing "Parse AS OF SYSTEM TIME"
    (let [tokens ["AS" "OF" "SYSTEM" "TIME" "'2024-01-15T00:00:00Z'"]
          result (temporal/parse-temporal-clause tokens)]
      (is (= :as-of (:type result)))
      (is (some? (:timestamp result)))))

  (testing "Parse FOR SYSTEM_TIME BETWEEN"
    (let [tokens ["FOR" "SYSTEM_TIME" "BETWEEN" "'2024-01-01'" "AND" "'2024-01-31'"]
          result (temporal/parse-temporal-clause tokens)]
      (is (= :between (:type result)))
      (is (some? (:start result)))
      (is (some? (:end result)))))

  (testing "Parse VERSIONS BETWEEN"
    (let [tokens ["VERSIONS" "BETWEEN" "'2024-01-01'" "AND" "'2024-01-31'"]
          result (temporal/parse-temporal-clause tokens)]
      (is (= :versions (:type result))))))

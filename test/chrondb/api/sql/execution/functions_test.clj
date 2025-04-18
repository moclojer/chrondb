(ns chrondb.api.sql.execution.functions-test
  (:require [clojure.test :refer [deftest is testing]]
            [chrondb.api.sql.execution.functions :as functions]))

(defn- approx-equals
  "Compare two numbers for approximate equality within a small delta"
  [a b]
  (let [delta 0.0001]
    (< (Math/abs (- a b)) delta)))

(deftest test-process-aggregate-result
  (testing "process-aggregate-result with count function"
    (is (= {:count_users 5}
           (functions/process-aggregate-result :count 5 "users"))))

  (testing "process-aggregate-result with sum function"
    (is (= {:sum_value 100}
           (functions/process-aggregate-result :sum 100 "value"))))

  (testing "process-aggregate-result with avg function"
    (is (= {:avg_price 25.5}
           (functions/process-aggregate-result :avg 25.5 "price"))))

  (testing "process-aggregate-result with nil result for count"
    (is (= {:count_users 0}
           (functions/process-aggregate-result :count nil "users"))))

  (testing "process-aggregate-result with nil result for sum"
    (is (= {:sum_value 0}
           (functions/process-aggregate-result :sum nil "value"))))

  (testing "process-aggregate-result with nil result for avg"
    (is (= {:avg_price 0}
           (functions/process-aggregate-result :avg nil "price")))))

(deftest test-execute-aggregate-function-advanced
  (let [test-docs [{:id "user:1", :name "Alice", :age 30, :score 85.5}
                   {:id "user:2", :name "Bob", :age 25, :score 92.0}
                   {:id "user:3", :name "Charlie", :age 35, :score 78.3}
                   {:id "user:4", :name "Diana", :age 28, :score 90.1}
                   {:id "user:5", :name "Eve", :age nil, :score nil}]]

    (testing "count function for all documents"
      (is (= 5 (functions/execute-aggregate-function :count test-docs "*"))))

    (testing "count function for non-null field"
      (is (= 4 (functions/execute-aggregate-function :count test-docs "age"))))

    (testing "sum function"
      (is (= 118 (functions/execute-aggregate-function :sum test-docs "age"))))

    (testing "avg function"
      (is (approx-equals 29.5 (functions/execute-aggregate-function :avg test-docs "age"))))

    (testing "min function"
      (is (approx-equals 25.0 (functions/execute-aggregate-function :min test-docs "age"))))

    (testing "max function"
      (is (approx-equals 35.0 (functions/execute-aggregate-function :max test-docs "age"))))

    (testing "numeric extraction from id field"
      (is (approx-equals 15 (functions/execute-aggregate-function :sum test-docs "id"))))

    (testing "unsupported aggregate function"
      (is (nil? (functions/execute-aggregate-function :unknown test-docs "age"))))))
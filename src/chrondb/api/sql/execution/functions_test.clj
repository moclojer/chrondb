(ns chrondb.api.sql.execution.functions-test
  (:require [clojure.test :refer [deftest is testing]]
            [chrondb.api.sql.execution.functions :as functions]
            [clojure.math.numeric-tower :refer [approximately=]]))

;; Sample data for testing
(def test-docs
  [; Use plain IDs now
   {:id "1", :name "Alice", :age 30, :city "New York"}
   {:id "2", :name "Bob", :age 25, :city "Los Angeles"}
   {:id "3", :name "Charlie", :age 35, :city "New York"}
   {:id "4", :name "David", :age 28, :city "Chicago"}
   {:id "5", :name "Eve", :age nil, :city "Los Angeles"}]) ; Use plain IDs now

(deftest test-execute-aggregate-function
  (testing "numeric extraction from id field"
    (is (approximately= 15 (functions/execute-aggregate-function :sum test-docs "id"))) ; Should work now with plain IDs
    )

  (testing "unsupported aggregate function"
    ;; ... existing code ...
    ))
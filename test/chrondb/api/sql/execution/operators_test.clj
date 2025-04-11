(ns chrondb.api.sql.execution.operators-test
  (:require [clojure.test :refer [deftest is testing]]
            [chrondb.api.sql.execution.operators :as operators]))

(def test-docs
  [{:id "user:1", :name "Alice", :age 30, :active true}
   {:id "user:2", :name "Bob", :age 25, :active false}
   {:id "user:3", :name "Charlie", :age 35, :active true}
   {:id "user:4", :name "David", :age 28, :active true}])

(deftest test-apply-where-conditions
  (testing "Equality condition"
    (let [conditions [{:field "name" :op "=" :value "Alice"}]
          result (operators/apply-where-conditions test-docs conditions)]
      (is (= 1 (count result)))
      (is (= "Alice" (:name (first result))))))

  (testing "Greater than condition"
    (let [conditions [{:field "age" :op ">" :value "30"}]
          result (operators/apply-where-conditions test-docs conditions)]
      (is (= 1 (count result)))
      (is (= "Charlie" (:name (first result))))))

  (testing "Less than condition"
    (let [conditions [{:field "age" :op "<" :value "28"}]
          result (operators/apply-where-conditions test-docs conditions)]
      (is (= 1 (count result)))
      (is (= "Bob" (:name (first result))))))

  (testing "Greater than or equal condition"
    (let [conditions [{:field "age" :op ">=" :value "30"}]
          result (operators/apply-where-conditions test-docs conditions)]
      (is (= 2 (count result)))
      (is (= #{"Alice" "Charlie"} (set (map :name result))))))

  (testing "Less than or equal condition"
    (let [conditions [{:field "age" :op "<=" :value "28"}]
          result (operators/apply-where-conditions test-docs conditions)]
      (is (= 2 (count result)))
      (is (= #{"Bob" "David"} (set (map :name result))))))

  (testing "Not equal condition"
    (let [conditions [{:field "name" :op "!=" :value "Alice"}]
          result (operators/apply-where-conditions test-docs conditions)]
      (is (= 3 (count result)))
      (is (= #{"Bob" "Charlie" "David"} (set (map :name result))))))

  (testing "Boolean condition (true)"
    (let [conditions [{:field "active" :op "=" :value "true"}]
          result (operators/apply-where-conditions test-docs conditions)]
      (is (= 3 (count result)))
      (is (= #{"Alice" "Charlie" "David"} (set (map :name result))))))

  (testing "Boolean condition (false)"
    (let [conditions [{:field "active" :op "=" :value "false"}]
          result (operators/apply-where-conditions test-docs conditions)]
      (is (= 1 (count result)))
      (is (= "Bob" (:name (first result))))))

  (testing "Multiple conditions (AND)"
    (let [conditions [{:field "age" :op ">" :value "25"}
                      {:field "active" :op "=" :value "true"}]
          result (operators/apply-where-conditions test-docs conditions)]
      (is (= 3 (count result)))
      (is (= #{"Alice" "Charlie" "David"} (set (map :name result))))))

  (testing "Handling invalid field"
    (let [conditions [{:field "non_existent" :op "=" :value "anything"}]
          result (operators/apply-where-conditions test-docs conditions)]
      (is (= 0 (count result)))))

  (testing "Empty conditions"
    (let [result (operators/apply-where-conditions test-docs [])]
      (is (= 4 (count result)))
      (is (= test-docs result)))))

(deftest test-sort-docs-by
  (testing "Sort by single column ascending"
    (let [docs [{:name "Bob", :age 25}
                {:name "Alice", :age 30}
                {:name "Charlie", :age 35}]
          order-clauses [{:column "name" :direction :asc}]
          result (operators/sort-docs-by docs order-clauses)]
      (is (= ["Alice" "Bob" "Charlie"] (map :name result)))))

  (testing "Sort by single column descending"
    (let [docs [{:name "Bob", :age 25}
                {:name "Alice", :age 30}
                {:name "Charlie", :age 35}]
          order-clauses [{:column "age" :direction :desc}]
          result (operators/sort-docs-by docs order-clauses)]
      (is (= ["Charlie" "Alice" "Bob"] (map :name result)))))

  (testing "Sort by multiple columns"
    (let [docs [{:name "Bob", :active false, :age 25}
                {:name "Alice", :active true, :age 30}
                {:name "Charlie", :active true, :age 35}
                {:name "David", :active true, :age 30}]
          order-clauses [{:column "active" :direction :desc}
                         {:column "age" :direction :asc}
                         {:column "name" :direction :asc}]
          result (operators/sort-docs-by docs order-clauses)]
      (is (= ["Alice" "David" "Charlie" "Bob"] (map :name result))))))

(deftest test-apply-limit
  (testing "Apply limit"
    (let [result (operators/apply-limit test-docs 2)]
      (is (= 2 (count result)))
      (is (= ["Alice" "Bob"] (map :name result)))))

  (testing "Limit greater than collection size"
    (let [result (operators/apply-limit test-docs 10)]
      (is (= 4 (count result)))
      (is (= test-docs result))))

  (testing "Limit zero"
    (let [result (operators/apply-limit test-docs 0)]
      (is (= 0 (count result)))))

  (testing "Limit nil"
    (let [result (operators/apply-limit test-docs nil)]
      (is (= 4 (count result)))
      (is (= test-docs result)))))

(deftest test-group-docs-by
  (testing "Group docs by single field"
    (let [docs [{:name "Alice", :dept "IT", :active true}
                {:name "Bob", :dept "HR", :active false}
                {:name "Charlie", :dept "IT", :active true}
                {:name "David", :dept "HR", :active true}]
          group-by [{:column "dept"}]
          result (operators/group-docs-by docs group-by)]
      (is (= 2 (count result)))))

  (testing "Group docs by multiple fields"
    (let [docs [{:name "Alice", :dept "IT", :active true}
                {:name "Bob", :dept "HR", :active false}
                {:name "Charlie", :dept "IT", :active true}
                {:name "David", :dept "HR", :active true}]
          group-by [{:column "dept"} {:column "active"}]
          result (operators/group-docs-by docs group-by)]
      (is (= 3 (count result))))))
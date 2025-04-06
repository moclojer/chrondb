(ns chrondb.api.sql.execution.operators-test
  (:require [clojure.test :refer [deftest is testing]]
            [chrondb.api.sql.execution.operators :as operators]))

(deftest test-evaluate-condition
  (let [doc {:id "user:1"
             :name "Alice"
             :age 30
             :active true}]

    (testing "Equal operator"
      (is (operators/evaluate-condition doc {:field "name" :op "=" :value "Alice"}))
      (is (not (operators/evaluate-condition doc {:field "name" :op "=" :value "Bob"}))))

    (testing "Not equal operators"
      (is (operators/evaluate-condition doc {:field "name" :op "!=" :value "Bob"}))
      (is (not (operators/evaluate-condition doc {:field "name" :op "!=" :value "Alice"})))

      (is (operators/evaluate-condition doc {:field "name" :op "<>" :value "Bob"}))
      (is (not (operators/evaluate-condition doc {:field "name" :op "<>" :value "Alice"}))))

    (testing "Greater than operator"
      (is (operators/evaluate-condition doc {:field "age" :op ">" :value "20"}))
      (is (not (operators/evaluate-condition doc {:field "age" :op ">" :value "30"}))))

    (testing "Less than operator"
      (is (operators/evaluate-condition doc {:field "age" :op "<" :value "40"}))
      (is (not (operators/evaluate-condition doc {:field "age" :op "<" :value "30"}))))

    (testing "Greater than or equal operator"
      (is (operators/evaluate-condition doc {:field "age" :op ">=" :value "30"}))
      (is (not (operators/evaluate-condition doc {:field "age" :op ">=" :value "31"}))))

    (testing "Less than or equal operator"
      (is (operators/evaluate-condition doc {:field "age" :op "<=" :value "30"}))
      (is (not (operators/evaluate-condition doc {:field "age" :op "<=" :value "29"}))))

    (testing "LIKE operator"
      (is (operators/evaluate-condition doc {:field "name" :op "like" :value "Al%"}))
      (is (operators/evaluate-condition doc {:field "name" :op "like" :value "%ice"}))
      (is (operators/evaluate-condition doc {:field "name" :op "like" :value "%lic%"}))
      (is (not (operators/evaluate-condition doc {:field "name" :op "like" :value "Bob%"}))))

    (testing "Unsupported operator"
      (is (not (operators/evaluate-condition doc {:field "name" :op "unknown" :value "Alice"}))))))

(deftest test-apply-where-conditions
  (let [docs [{:id "user:1" :name "Alice" :age 30 :active true}
              {:id "user:2" :name "Bob" :age 25 :active false}
              {:id "user:3" :name "Charlie" :age 35 :active true}
              {:id "user:4" :name "Diana" :age 28 :active false}]]

    (testing "Empty conditions"
      (is (= docs (operators/apply-where-conditions docs []))))

    (testing "Single condition"
      (let [conditions [{:field "age" :op ">" :value "28"}]]
        (is (= 2 (count (operators/apply-where-conditions docs conditions))))
        (is (= ["user:1" "user:3"] (map :id (operators/apply-where-conditions docs conditions))))))

    (testing "Multiple conditions (AND)"
      (let [conditions [{:field "age" :op ">" :value "25"}
                        {:field "active" :op "=" :value "true"}]]
        (is (= 2 (count (operators/apply-where-conditions docs conditions))))
        (is (= ["user:1" "user:3"] (map :id (operators/apply-where-conditions docs conditions))))))

    (testing "LIKE condition"
      (let [conditions [{:field "name" :op "LIKE" :value "D%"}]]
        (is (= 1 (count (operators/apply-where-conditions docs conditions))))
        (is (= ["user:4"] (map :id (operators/apply-where-conditions docs conditions))))))))

(deftest test-group-docs-by
  (let [docs [{:id "user:1" :name "Alice" :dept "IT" :role "Dev"}
              {:id "user:2" :name "Bob" :dept "HR" :role "Manager"}
              {:id "user:3" :name "Charlie" :dept "IT" :role "Dev"}
              {:id "user:4" :name "Diana" :dept "HR" :role "Admin"}]]

    (testing "Empty group fields"
      (is (= [docs] (operators/group-docs-by docs []))))

    (testing "Group by single field"
      (let [groups (operators/group-docs-by docs [{:column "dept"}])
            expected-count 2]  ;; IT and HR groups
        (is (= expected-count (count groups)))
        ;; Each group should have docs with the same dept
        (doseq [group groups]
          (is (apply = (map :dept group))))))

    (testing "Group by multiple fields"
      (let [groups (operators/group-docs-by docs [{:column "dept"} {:column "role"}])
            expected-count 3]  ;; IT+Dev, HR+Manager, HR+Admin
        (is (= expected-count (count groups)))))))

(deftest test-sort-docs-by
  (let [docs [{:id "user:1" :name "Alice" :age 30 :dept "IT"}
              {:id "user:2" :name "Bob" :age 25 :dept "HR"}
              {:id "user:3" :name "Charlie" :age 35 :dept "IT"}
              {:id "user:4" :name "Diana" :age 28 :dept "HR"}]]

    (testing "Empty order clauses"
      (is (= docs (operators/sort-docs-by docs []))))

    (testing "Sort by ascending age"
      (let [sorted (operators/sort-docs-by docs [{:column "age" :direction :asc}])]
        (is (= ["user:2" "user:4" "user:1" "user:3"] (map :id sorted)))))

    (testing "Sort by descending age"
      (let [sorted (operators/sort-docs-by docs [{:column "age" :direction :desc}])]
        (is (= ["user:3" "user:1" "user:4" "user:2"] (map :id sorted)))))

    (testing "Sort by multiple fields (dept asc, age desc)"
      (let [sorted (operators/sort-docs-by docs [{:column "dept" :direction :asc}
                                                 {:column "age" :direction :desc}])]
        (is (= ["user:4" "user:2" "user:3" "user:1"] (map :id sorted)))))))

(deftest test-apply-limit
  (let [docs [{:id "user:1" :name "Alice"}
              {:id "user:2" :name "Bob"}
              {:id "user:3" :name "Charlie"}
              {:id "user:4" :name "Diana"}
              {:id "user:5" :name "Eve"}]]

    (testing "Nil limit returns all docs"
      (is (= 5 (count (operators/apply-limit docs nil)))))

    (testing "Zero limit returns empty collection"
      (is (empty? (operators/apply-limit docs 0))))

    (testing "Limit less than total count"
      (is (= 3 (count (operators/apply-limit docs 3))))
      (is (= ["user:1" "user:2" "user:3"] (map :id (operators/apply-limit docs 3)))))

    (testing "Limit equal to total count"
      (is (= 5 (count (operators/apply-limit docs 5)))))

    (testing "Limit greater than total count"
      (is (= 5 (count (operators/apply-limit docs 10)))))))
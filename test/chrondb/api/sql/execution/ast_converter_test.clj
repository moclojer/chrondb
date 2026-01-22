 (ns chrondb.api.sql.execution.ast-converter-test
   (:require [clojure.test :refer [deftest is testing]]
             [chrondb.api.sql.execution.ast-converter :as converter]
             [chrondb.query.ast :as ast]))

(deftest condition->ast-clause-fts-test
  (is (= (ast/fts :content "time travel")
         (converter/condition->ast-clause
          {:type :fts-match
           :field "content"
           :op "MATCH"
           :query "time travel"}))))

(deftest condition->ast-clause-standard-test
  (testing "Equality removes surrounding quotes"
    (let [condition {:type :standard
                     :field "name"
                     :op "="
                     :value "'Alice'"}
          expected (ast/term :name "Alice")]
      (is (= expected (converter/condition->ast-clause condition)))))

  (testing "Not equal delegates to ast/not"
    (let [condition {:type :standard
                     :field "status"
                     :op "!="
                     :value "archived"}
          expected (ast/not (ast/term :status "archived"))]
      (is (= expected (converter/condition->ast-clause condition)))))

  (testing "Alternate not equal operator <> behaves the same"
    (let [condition {:type :standard
                     :field "status"
                     :op "<>"
                     :value "archived"}
          expected (ast/not (ast/term :status "archived"))]
      (is (= expected (converter/condition->ast-clause condition)))))

  (testing "Greater than parses longs"
    (let [condition {:type :standard
                     :field "age"
                     :op ">"
                     :value "30"}
          expected (ast/range-long :age 30 nil {:include-lower? false})]
      (is (= expected (converter/condition->ast-clause condition)))))

  (testing "Greater than parses doubles"
    (let [condition {:type :standard
                     :field "price"
                     :op ">"
                     :value "42.5"}
          expected (ast/range-double :price 42.5 nil {:include-lower? false})]
      (is (= expected (converter/condition->ast-clause condition)))))

  (testing "Greater than falls back to string range when numeric parse fails"
    (let [condition {:type :standard
                     :field "code"
                     :op ">"
                     :value "'A'"}
          expected (ast/range :code "A" nil {:include-lower? false})]
      (is (= expected (converter/condition->ast-clause condition)))))

  (testing "Less-than-or-equal builds inclusive upper bound"
    (let [condition {:type :standard
                     :field "age"
                     :op "<="
                     :value "65"}
          expected (ast/range-long :age nil 65 {:include-upper? true})]
      (is (= expected (converter/condition->ast-clause condition)))))

  (testing "LIKE converts SQL wildcards to Lucene wildcards"
    (let [condition {:type :standard
                     :field "title"
                     :op "LIKE"
                     :value "report-%Q1%"}
          expected (ast/wildcard :title "report-*Q1*")]
      (is (= expected (converter/condition->ast-clause condition)))))

  (testing "IS NULL returns missing clause"
    (let [condition {:type :standard
                     :field "deleted_at"
                     :op "IS NULL"}
          expected (ast/missing :deleted_at)]
      (is (= expected (converter/condition->ast-clause condition)))))

  (testing "IS NOT NULL returns exists clause"
    (let [condition {:type :standard
                     :field "deleted_at"
                     :op "IS NOT NULL"}
          expected (ast/exists :deleted_at)]
      (is (= expected (converter/condition->ast-clause condition)))))

  (testing "Unknown condition type returns nil"
    (is (nil? (converter/condition->ast-clause
               {:type :unsupported
                :field "name"
                :op "="
                :value "Alice"})))))

(deftest conditions->ast-clauses-test
  (testing "Empty conditions produce nil"
    (is (nil? (converter/conditions->ast-clauses []))))

  (testing "Single condition returns clause without wrapping"
    (let [condition {:type :standard :field "age" :op ">" :value "30"}
          expected (converter/condition->ast-clause condition)]
      (is (= expected (converter/conditions->ast-clauses [condition])))))

  (testing "Multiple conditions combine with AND and drop nil clauses"
    (let [cond-a {:type :standard :field "age" :op ">" :value "30"}
          cond-b {:type :standard :field "status" :op "=" :value "active"}
          cond-nil {:type :unsupported :field "ignored" :op "=" :value "nope"}
          expected (ast/and (converter/condition->ast-clause cond-a)
                            (converter/condition->ast-clause cond-b))]
      (is (= expected
             (converter/conditions->ast-clauses [cond-a cond-b cond-nil]))))))


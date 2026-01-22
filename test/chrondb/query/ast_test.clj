(ns chrondb.query.ast-test
  (:require [clojure.test :refer [deftest is testing]]
            [chrondb.query.ast :as ast]))

(deftest match-and-term-clauses-test
   (testing "match-all returns the expected clause"
     (is (= {:type :match-all}
            (ast/match-all))))

   (testing "term clauses require both field and value"
    (is (= {:type :term :field "status" :value "active"}
           (ast/term :status "active")))
    (is (= {:type :term :field "score" :value "42"}
           (ast/term :score 42)))
     (is (nil? (ast/term nil "active")))
     (is (nil? (ast/term :status nil))))

   (testing "wildcard clauses normalize field and value"
     (is (= {:type :wildcard :field "name" :value "foo"}
            (ast/wildcard :name "foo")))
     (is (nil? (ast/wildcard :name nil))))

   (testing "prefix delegates to wildcard with appended asterisk"
     (is (= (ast/wildcard :path "logs*")
            (ast/prefix :path "logs")))
     (is (nil? (ast/prefix :path nil))))

   (testing "full-text search clauses include analyzer metadata"
     (is (= {:type :fts :field "content" :value "history" :analyzer :fts}
            (ast/fts :content "history")))
     (is (nil? (ast/fts :content nil))))

   (testing "exists and missing clauses reject nil fields"
     (is (= {:type :exists :field "updated_at"}
            (ast/exists :updated_at)))
     (is (nil? (ast/exists nil)))
     (is (= {:type :missing :field "deleted_at"}
            (ast/missing :deleted_at)))
     (is (nil? (ast/missing nil)))))

(deftest range-clauses-test
   (testing "default range is inclusive with stringified bounds"
     (is (= {:type :range
             :field "timestamp"
             :lower "10"
             :upper "20"
             :include-lower? true
             :include-upper? true
             :value-type nil}
            (ast/range :timestamp 10 20))))

   (testing "range supports open bounds and option overrides"
     (is (= {:type :range
             :field "created_at"
             :lower nil
             :upper "100"
             :include-lower? false
             :include-upper? false
             :value-type :string}
            (ast/range :created_at nil 100
                       {:include-lower? false
                        :include-upper? false
                        :type :string}))))

   (testing "numeric helper ranges set the value type"
     (is (= {:type :range
             :field "age"
             :lower "18"
             :upper "65"
             :include-lower? true
             :include-upper? false
             :value-type :long}
            (ast/range-long :age 18 65 {:include-upper? false})))
     (is (= {:type :range
             :field "score"
             :lower "0.1"
             :upper "0.9"
             :include-lower? true
             :include-upper? true
             :value-type :double}
            (ast/range-double :score 0.1 0.9 {})))))

(deftest boolean-node-test
   (testing "empty boolean collapses to match-all"
     (is (= (ast/match-all)
            (ast/boolean {}))))

   (testing "single should clause collapses to the clause itself"
     (let [clause (ast/term :status "active")]
       (is (= clause
              (ast/boolean {:should [clause]})))))

   (testing "must-not clauses receive an implicit match-all must"
     (let [clause (ast/term :status "inactive")]
       (is (= {:type :boolean
               :must [(ast/match-all)]
               :should []
               :must-not [clause]
               :filter []}
              (ast/boolean {:must []
                             :should []
                             :must-not [clause]
                             :filter []})))))

   (testing "general boolean vectorizes inputs and removes nils"
     (let [must-a (ast/term :status "active")
           should-a (ast/term :role "admin")
           result (ast/boolean {:must [must-a nil]
                                :should [nil should-a]
                                :must-not [nil]
                                :filter nil})]
       (is (= {:type :boolean
               :must [must-a]
               :should [should-a]
               :must-not []
               :filter []}
              result)))))

(deftest logical-combinators-test
   (testing "and collapses edge cases"
     (let [clause (ast/term :status "active")]
       (is (= (ast/match-all) (ast/and)))
       (is (= clause (ast/and clause)))
       (is (= {:type :boolean
               :must [clause (ast/match-all)]
               :should []
               :must-not []
               :filter []}
              (ast/and clause nil (ast/match-all))))))

   (testing "or collapses edge cases"
     (let [clause (ast/term :role "admin")]
       (is (= (ast/match-all) (ast/or)))
       (is (= clause (ast/or clause)))
       (is (= {:type :boolean
               :must []
               :should [clause (ast/match-all)]
               :must-not []
               :filter []}
              (ast/or clause nil (ast/match-all))))))

   (testing "not wraps the clause with must-not and implicit must"
     (let [clause (ast/term :status "inactive")]
       (is (= {:type :boolean
               :must [(ast/match-all)]
               :should []
               :must-not [clause]
               :filter []}
              (ast/not clause)))))

  (testing "not defaults to match-all when clause is nil"
    (is (= {:type :boolean
            :must [(ast/match-all)]
            :should []
            :must-not [(ast/match-all)]
            :filter []}
           (ast/not nil)))))

(deftest sort-and-query-metadata-test
   (testing "sort descriptors infer defaults"
     (is (= {:field "timestamp" :direction :asc :type :string}
            (ast/sort-by :timestamp)))
     (is (= {:field "timestamp" :direction :desc :type :string}
            (ast/sort-by :timestamp :desc)))
     (is (= {:field "timestamp" :direction :desc :type :long}
            (ast/sort-by :timestamp :desc :long))))

   (testing "query wraps clauses and optional metadata"
     (let [clause (ast/term :status "active")
           sort-desc (ast/sort-by :timestamp :desc :long)
           query (ast/query [clause]
                            {:sort [sort-desc]
                             :limit 25
                             :offset 10
                             :branch "feature"
                             :hints {:refresh true}
                             :after {:doc 42}})]
       (is (= {:clauses [clause]
               :sort [sort-desc]
               :limit 25
               :offset 10
               :branch "feature"
               :hints {:refresh true}
               :after {:doc 42}}
              query))))

   (testing "query vectorizes sort descriptors when provided"
     (let [clause (ast/match-all)
           sort-desc (ast/sort-by :timestamp)
           query (ast/query [clause] {:sort [sort-desc]})]
       (is (= [sort-desc] (:sort query))))))

(deftest query-transformers-test
  (let [base-query (ast/query [(ast/match-all)])
        sort-desc (ast/sort-by :timestamp :desc :long)]
    (testing "with-branch replaces the branch"
      (is (= "main"
             (:branch (ast/with-branch base-query "main")))))

    (testing "with-hints merges successive hint maps"
      (is (= {:refresh true :track-total-hits true}
             (:hints (-> base-query
                         (ast/with-hints {:refresh true})
                         (ast/with-hints {:track-total-hits true})))))
      (is (nil? (:hints (ast/with-hints base-query nil)))))

    (testing "with-search-after attaches the cursor"
      (is (= {:doc 9 :score 0.42}
             (:after (ast/with-search-after base-query {:doc 9 :score 0.42})))))

    (testing "with-pagination conditionally sets pagination keys"
      (is (= {:limit 100 :offset 20 :after {:doc 1}}
             (select-keys (ast/with-pagination base-query
                                              {:limit 100
                                               :offset 20
                                               :after {:doc 1}})
                          [:limit :offset :after])))
      (is (= base-query (ast/with-pagination base-query {}))))

    (testing "with-sort vectorizes descriptors"
      (is (= [sort-desc]
             (:sort (ast/with-sort base-query sort-desc))))
      (is (= [sort-desc]
             (:sort (ast/with-sort base-query [sort-desc])))))))


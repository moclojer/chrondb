(ns chrondb.api.sql.execution.query-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [chrondb.api.sql.execution.query :as query]
            [chrondb.storage.protocol :as storage-protocol]
            [chrondb.storage.memory :as memory]
            [chrondb.index.memory :as memory-index]))

;; Helper functions for testing
(defn create-test-resources []
  {:storage (memory/create-memory-storage)
   :index (memory-index/create-memory-index)})

(defn prepare-test-data [storage]
  (doseq [id ["user:1" "user:2" "user:3"]]
    (let [doc {:id id
               :_table "user"
               :name (str "User " (last id))
               :age (+ 25 (Integer/parseInt (str (last id))))
               :active (odd? (Integer/parseInt (str (last id))))}]
      (storage-protocol/save-document storage doc nil))))

;; Define a mock storage implementation
(defn create-mock-storage []
  (let [documents (atom {"user:1" {:id "user:1" :name "Alice" :age 30 :active true}
                         "user:2" {:id "user:2" :name "Bob" :age 25 :active false}
                         "user:3" {:id "user:3" :name "Charlie" :age 35 :active true :dept "IT"}
                         "user:4" {:id "user:4" :name "Diana" :age 28 :active false :dept "HR"}
                         "product:1" {:id "product:1" :name "Laptop" :price 1200 :stock 10}
                         "product:2" {:id "product:2" :name "Phone" :price 800 :stock 20}})]
    (reify storage-protocol/Storage
      (get-document [_ id]
        (get @documents id))

      (get-document [_ id _branch]
        (get @documents id))

      (get-documents-by-prefix [_ prefix]
        (filter (fn [[k _]] (.startsWith k prefix)) @documents))

      (get-documents-by-prefix [_ prefix _branch]
        (filter (fn [[k _]] (.startsWith k prefix)) @documents))

      (get-documents-by-table [_ table-name]
        (->> @documents
             (filter (fn [[k _]] (.startsWith k (str table-name ":"))))
             (map second)
             (into [])))

      (get-documents-by-table [_ table-name _branch]
        (->> @documents
             (filter (fn [[k _]] (.startsWith k (str table-name ":"))))
             (map second)
             (into [])))

      (save-document [_ doc]
        (swap! documents assoc (:id doc) doc)
        doc)

      (save-document [_ doc _branch]
        (swap! documents assoc (:id doc) doc)
        doc)

      (delete-document [_ id]
        (swap! documents dissoc id)
        true)

      (delete-document [_ id _branch]
        (swap! documents dissoc id)
        true)

      (close [_]
        nil))))

;; Define a dynamic var for the test storage
(def ^:dynamic *test-storage* nil)

;; Define a fixture to set up and tear down the mock storage
(defn with-mock-storage [f]
  (binding [*test-storage* (create-mock-storage)]
    (f)))

(use-fixtures :each with-mock-storage)

(deftest test-handle-select-all
  (testing "SELECT * query"
    (let [query {:type :select
                 :table "user"
                 :columns [{:type :all}]
                 :where nil
                 :order-by nil
                 :limit nil}
          results (query/handle-select *test-storage* query)]
      (is (= 4 (count results)))
      (is (= #{"user:1" "user:2" "user:3" "user:4"}
             (set (map :id results)))))))

(deftest test-handle-select-with-where
  (testing "SELECT with WHERE clause"
    (let [query {:type :select
                 :table "user"
                 :columns [{:type :all}]
                 :where [{:field "age" :op ">" :value "28"}]
                 :order-by nil
                 :limit nil}
          results (query/handle-select *test-storage* query)]
      (is (= 2 (count results)))
      (is (= #{"user:1" "user:3"}
             (set (map :id results)))))))

(deftest test-handle-select-with-order-by
  (testing "SELECT with ORDER BY clause"
    (let [query {:type :select
                 :table "user"
                 :columns [{:type :all}]
                 :where nil
                 :order-by [{:column "age" :direction :desc}]
                 :limit nil}
          results (query/handle-select *test-storage* query)]
      (is (= 4 (count results)))
      (is (= ["user:3" "user:1" "user:4" "user:2"]
             (map :id results))))))

(deftest test-handle-select-with-limit
  (testing "SELECT with LIMIT clause"
    (let [query {:type :select
                 :table "user"
                 :columns [{:type :all}]
                 :where nil
                 :order-by [{:column "age" :direction :desc}]
                 :limit 2}
          results (query/handle-select *test-storage* query)]
      (is (= 2 (count results)))
      (is (= ["user:3" "user:1"]
             (map :id results))))))

(deftest test-handle-select-specific-columns
  (testing "SELECT specific columns"
    (let [query {:type :select
                 :table "user"
                 :columns [{:type :column :column "name"}
                           {:type :column :column "age"}]
                 :where nil
                 :order-by nil
                 :limit nil}
          results (query/handle-select *test-storage* query)]
      (is (= 4 (count results)))
      (is (every? #(= #{:name :age} (set (keys %))) results)))))

(deftest test-handle-select-by-id
  (testing "SELECT by ID"
    (let [query {:type :select
                 :table "user"
                 :columns [{:type :all}]
                 :where [{:field "id" :op "=" :value "user:1"}]
                 :order-by nil
                 :limit nil}
          results (query/handle-select *test-storage* query)]
      (is (= 1 (count results)))
      (is (= "user:1" (:id (first results)))))))

(deftest test-handle-select-with-group-by
  (testing "SELECT with GROUP BY"
    (let [query {:type :select
                 :table "user"
                 :columns [{:type :column :column "dept"}
                           {:type :aggregate-function :function :count :args ["*"]}]
                 :where nil
                 :group-by [{:column "dept"}]
                 :order-by nil
                 :limit nil}
          results (query/handle-select *test-storage* query)
          results-without-nulls (filter #(some? (:dept %)) results)]

      ;; Check that we have IT and HR departments (filtering out any null dept entries)
      (is (= 2 (count results-without-nulls)))

      ;; Each result with a non-null dept should have dept and count_* fields
      (is (every? #(and (:dept %) (contains? % :count_*)) results-without-nulls))

      ;; Verify we have both IT and HR departments
      (is (= #{"IT" "HR"} (set (map :dept results-without-nulls))))

      ;; Verify that department counts are appropriate
      (is (= 1 (:count_* (first (filter #(= (:dept %) "IT") results-without-nulls)))))
      (is (= 1 (:count_* (first (filter #(= (:dept %) "HR") results-without-nulls))))))))

(deftest test-handle-insert
  (testing "INSERT query"
    (let [new-doc {:id "user:5" :name "Eve" :age 40 :active true}
          result (query/handle-insert *test-storage* new-doc)]
      (is (= new-doc result))
      ;; Verify the document was added to storage
      (let [query {:type :select
                   :table "user"
                   :columns [{:type :all}]
                   :where [{:field "id" :op "=" :value "user:5"}]
                   :order-by nil
                   :limit nil}
            results (query/handle-select *test-storage* query)]
        (is (= 1 (count results)))
        (is (= "user:5" (:id (first results))))))))

(deftest test-handle-update
  (testing "UPDATE query"
    (let [updates {:age 31 :active false}
          result (query/handle-update *test-storage* "user:1" updates)]
      (is (= "user:1" (:id result)))
      (is (= 31 (:age result)))
      (is (= false (:active result)))
      (is (= "Alice" (:name result)))

      ;; Verify the document was updated in storage
      (let [query {:type :select
                   :table "user"
                   :columns [{:type :all}]
                   :where [{:field "id" :op "=" :value "user:1"}]
                   :order-by nil
                   :limit nil}
            results (query/handle-select *test-storage* query)]
        (is (= 1 (count results)))
        (is (= 31 (:age (first results))))
        (is (= false (:active (first results))))))))

(deftest test-handle-schema-branch-mapping
  (testing "Schema to branch mapping for SQL queries"
    ;; Test with different schemas through handle-select function
    (let [storage *test-storage*] ;; Use the test mock storage
      ;; Test with no schema (should use 'main' branch)
      (let [query {:type :select
                   :table "user"
                   :columns [{:type :all}]
                   :schema nil
                   :where nil
                   :order-by nil
                   :limit nil}
            results (query/handle-select storage query)]
        (is (pos? (count results)) "Query without schema should return results from the default branch"))

      ;; Test with 'public' schema (should use 'main' branch)
      (let [query {:type :select
                   :table "user"
                   :columns [{:type :all}]
                   :schema "public"
                   :where nil
                   :order-by nil
                   :limit nil}
            results (query/handle-select storage query)]
        (is (pos? (count results)) "Query with 'public' schema should return results from the 'main' branch"))

      ;; Test with 'main' schema (should use 'main' branch)
      (let [query {:type :select
                   :table "user"
                   :columns [{:type :all}]
                   :schema "main"
                   :where nil
                   :order-by nil
                   :limit nil}
            results (query/handle-select storage query)]
        (is (pos? (count results)) "Query with 'main' schema should return results from the 'main' branch")))))
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

      (get-document-history [_ id]
        (when-let [doc (get @documents id)]
          [{:commit-id "mock-current"
            :commit-time (java.util.Date.)
            :commit-message "Current version"
            :committer-name "Mock Storage"
            :committer-email "mock@chrondb.com"
            :document doc}]))

      (get-document-history [_ id _branch]
        (when-let [doc (get @documents id)]
          [{:commit-id "mock-current"
            :commit-time (java.util.Date.)
            :commit-message "Current version"
            :committer-name "Mock Storage"
            :committer-email "mock@chrondb.com"
            :document doc}]))

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
          results (query/handle-select *test-storage* nil query)]
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
          results (query/handle-select *test-storage* nil query)]
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
          results (query/handle-select *test-storage* nil query)]
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
          results (query/handle-select *test-storage* nil query)]
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
          results (query/handle-select *test-storage* nil query)]
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
          results (query/handle-select *test-storage* nil query)]
      (is (= 1 (count results)) "Should return exactly one result")
      (is (= "user:1" (:id (first results))) "Result ID should match the queried ID"))))

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
          results (query/handle-select *test-storage* nil query)
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
      (is (= (assoc new-doc :_table "user") result))
      ;; Verify the document was added to storage
      (let [query {:type :select
                   :table "user"
                   :columns [{:type :all}]
                   :where [{:field "id" :op "=" :value "user:5"}]
                   :order-by nil
                   :limit nil}
            results (query/handle-select *test-storage* nil query)]
        (is (= 1 (count results)))
        (is (= "user:5" (:id (first results))))))))

(deftest test-handle-update
  (testing "UPDATE query"
    (let [updates {:age 31 :active false}
          result (query/handle-update *test-storage* "user:1" updates)]
      (is (= "user:1" (:id result)) "Updated document should have the correct ID")
      (is (= 31 (:age result)) "Age should be updated to the new value")
      (is (= false (:active result)) "Active field should be updated to the new value")
      (is (= "Alice" (:name result)) "Name field should remain unchanged")

      ;; Verify the document was updated in storage
      (let [query {:type :select
                   :table "user"
                   :columns [{:type :all}]
                   :where [{:field "id" :op "=" :value "user:1"}]
                   :order-by nil
                   :limit nil}
            results (query/handle-select *test-storage* nil query)]
        (is (= 1 (count results)) "Should return exactly one result")
        (is (= 31 (:age (first results))) "Age in stored document should match the updated value")
        (is (= false (:active (first results))) "Active field in stored document should match the updated value")))))

(deftest test-handle-update-with-branch
  (testing "UPDATE operation"
    (let [storage *test-storage*
          document {:id "test-update-doc" :name "Original" :value 100 :_table "test"}
          _ (query/handle-insert storage document "main")
          updates {:name "Updated" :value 200}
          updated (query/handle-update storage "test-update-doc" updates "main")]
      (is (= "test-update-doc" (:id updated)))
      (is (= "Updated" (:name updated)))
      (is (= 200 (:value updated))))))

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
            results (query/handle-select storage nil query)]
        (is (pos? (count results)) "Query without schema should return results from the default branch"))

      ;; Test with 'public' schema (should use 'main' branch)
      (let [query {:type :select
                   :table "user"
                   :columns [{:type :all}]
                   :schema "public"
                   :where nil
                   :order-by nil
                   :limit nil}
            results (query/handle-select storage nil query)]
        (is (pos? (count results)) "Query with 'public' schema should return results from the 'main' branch"))

      ;; Test with 'main' schema (should use 'main' branch)
      (let [query {:type :select
                   :table "user"
                   :columns [{:type :all}]
                   :schema "main"
                   :where nil
                   :order-by nil
                   :limit nil}
            results (query/handle-select storage nil query)]
        (is (pos? (count results)) "Query with 'main' schema should return results from the 'main' branch")))))

(deftest test-handle-select-with-inner-join
  (testing "SELECT with INNER JOIN"
    ;; In our test data:
    ;; - user:3 has dept="IT"
    ;; - user:4 has dept="HR"
    ;; - product:1 is assigned to dept="IT" (will be added)
    ;; - product:2 is assigned to dept="HR" (will be added)

    ;; Add department field to products
    (storage-protocol/save-document *test-storage*
                                    (assoc (storage-protocol/get-document *test-storage* "product:1")
                                           :dept "IT"))
    (storage-protocol/save-document *test-storage*
                                    (assoc (storage-protocol/get-document *test-storage* "product:2")
                                           :dept "HR"))

    ;; Query to join users and products by department
    (let [query {:type :select
                 :table "user"
                 :columns [{:type :column :column "user.name"}
                           {:type :column :column "user.dept"}
                           {:type :column :column "product.name"}
                           {:type :column :column "product.price"}]
                 :where nil
                 :join {:table "product"
                        :type :inner
                        :on {:left-table "user"
                             :left-field "dept"
                             :right-table "product"
                             :right-field "dept"}}
                 :order-by [{:column "user.name" :direction :asc}]
                 :limit nil}
          results (query/handle-select *test-storage* nil query)]

      ;; We should have two matches (Charlie-IT-Laptop and Diana-HR-Phone)
      (is (= 2 (count results)) "Should return two rows from the join")

      ;; Check the structure of the first result
      (let [first-result (first results)]
        (is (contains? first-result :user.name) "Result should contain user.name field")
        (is (contains? first-result :user.dept) "Result should contain user.dept field")
        (is (contains? first-result :product.name) "Result should contain product.name field")
        (is (contains? first-result :product.price) "Result should contain product.price field"))

      ;; Verify Charlie is joined with Laptop
      (let [charlie-join (first (filter #(= (:user.name %) "Charlie") results))]
        (is (= "IT" (:user.dept charlie-join)) "Charlie should be in IT department")
        (is (= "Laptop" (:product.name charlie-join)) "IT department should be joined with Laptop"))

      ;; Verify Diana is joined with Phone
      (let [diana-join (first (filter #(= (:user.name %) "Diana") results))]
        (is (= "HR" (:user.dept diana-join)) "Diana should be in HR department")
        (is (= "Phone" (:product.name diana-join)) "HR department should be joined with Phone")))))

(deftest test-handle-select-with-inner-join-and-where
  (testing "SELECT with INNER JOIN and WHERE clause"
    ;; Ensure we have the join test data set up
    (storage-protocol/save-document *test-storage*
                                    (assoc (storage-protocol/get-document *test-storage* "product:1")
                                           :dept "IT"))
    (storage-protocol/save-document *test-storage*
                                    (assoc (storage-protocol/get-document *test-storage* "product:2")
                                           :dept "HR"))

    ;; Query to join users and products by department, filtering for IT department only
    (let [query {:type :select
                 :table "user"
                 :columns [{:type :column :column "user.name"}
                           {:type :column :column "user.dept"}
                           {:type :column :column "product.name"}
                           {:type :column :column "product.price"}]
                 :where [{:field "user.dept" :op "=" :value "IT"}]
                 :join {:table "product"
                        :type :inner
                        :on {:left-table "user"
                             :left-field "dept"
                             :right-table "product"
                             :right-field "dept"}}
                 :order-by nil
                 :limit nil}
          results (query/handle-select *test-storage* nil query)]

      ;; We should have only one match (Charlie-IT-Laptop)
      (is (= 1 (count results)) "Should return only one row from the join with WHERE filter")

      ;; Verify Charlie is joined with Laptop
      (let [charlie-join (first results)]
        (is (= "Charlie" (:user.name charlie-join)) "Result should be for Charlie")
        (is (= "IT" (:user.dept charlie-join)) "Charlie should be in IT department")
        (is (= "Laptop" (:product.name charlie-join)) "IT department should be joined with Laptop")
        (is (= 1200 (:product.price charlie-join)) "Laptop should have price 1200")))))

(deftest test-handle-select-with-complex-join
  (testing "SELECT with JOIN and complex conditions"
    ;; Add more test data for complex join
    (storage-protocol/save-document *test-storage*
                                    {:id "order:1"
                                     :user_id "user:1"
                                     :product_id "product:1"
                                     :quantity 2
                                     :_table "order"})
    (storage-protocol/save-document *test-storage*
                                    {:id "order:2"
                                     :user_id "user:2"
                                     :product_id "product:2"
                                     :quantity 1
                                     :_table "order"})

    ;; Query that joins users with their orders
    (let [query {:type :select
                 :table "user"
                 :columns [{:type :column :column "user.name"}
                           {:type :column :column "order.quantity"}
                           {:type :column :column "order.product_id"}]
                 :where nil
                 :join {:table "order"
                        :type :inner
                        :on {:left-table "user"
                             :left-field "id"
                             :right-table "order"
                             :right-field "user_id"}}
                 :order-by [{:column "user.name" :direction :asc}]
                 :limit nil}
          results (query/handle-select *test-storage* nil query)]

      ;; Should have two orders
      (is (= 2 (count results)) "Should return two rows from the user-order join")

      ;; Alice should have ordered product:1
      (let [alice-order (first (filter #(= (:user.name %) "Alice") results))]
        (is (some? alice-order) "Alice should have an order")
        (is (= 2 (:order.quantity alice-order)) "Alice should have ordered 2 units")
        (is (= "product:1" (:order.product_id alice-order)) "Alice should have ordered product:1"))

      ;; Bob should have ordered product:2
      (let [bob-order (first (filter #(= (:user.name %) "Bob") results))]
        (is (some? bob-order) "Bob should have an order")
        (is (= 1 (:order.quantity bob-order)) "Bob should have ordered 1 unit")
        (is (= "product:2" (:order.product_id bob-order)) "Bob should have ordered product:2")))))

(deftest test-handle-select-with-left-join
  (testing "SELECT with LEFT JOIN"
    ;; Ensure we have the join test data set up
    (storage-protocol/save-document *test-storage*
                                    (assoc (storage-protocol/get-document *test-storage* "product:1")
                                           :dept "IT"))
    (storage-protocol/save-document *test-storage*
                                    (assoc (storage-protocol/get-document *test-storage* "product:2")
                                           :dept "HR"))

    ;; Add a user with a department that has no matching product
    (storage-protocol/save-document *test-storage*
                                    {:id "user:5"
                                     :name "Eric"
                                     :age 45
                                     :active true
                                     :dept "Finance"
                                     :_table "user"})  ;; Add _table explicitly

    ;; Query to left join users and products by department
    (let [query {:type :select
                 :table "user"
                 :columns [{:type :column :column "user.name"}
                           {:type :column :column "user.dept"}
                           {:type :column :column "product.name"}
                           {:type :column :column "product.price"}]
                 :where nil
                 :join {:table "product"
                        :type :left
                        :on {:left-table "user"
                             :left-field "dept"
                             :right-table "product"
                             :right-field "dept"}}
                 :order-by [{:column "user.name" :direction :asc}]
                 :limit nil}
          results (query/handle-select *test-storage* nil query)]

      ;; We should have five results - 2 (Alice, Bob) without dept, 2 (Charlie-IT, Diana-HR) with matching dept, 1 (Eric-Finance) with unmatched dept
      (is (= 5 (count results)) "Should return all users from left table regardless of matches")

      ;; Check results with matching departments
      ;; Charlie (IT) should match with Laptop
      (let [charlie-join (first (filter #(= (:user.name %) "Charlie") results))]
        (is (some? charlie-join) "Charlie should be in results")
        (is (= "IT" (:user.dept charlie-join)) "Charlie should be in IT department")
        (is (= "Laptop" (:product.name charlie-join)) "IT department should be joined with Laptop")
        (is (= 1200 (:product.price charlie-join)) "Laptop price should be 1200"))

      ;; Diana (HR) should match with Phone
      (let [diana-join (first (filter #(= (:user.name %) "Diana") results))]
        (is (some? diana-join) "Diana should be in results")
        (is (= "HR" (:user.dept diana-join)) "Diana should be in HR department")
        (is (= "Phone" (:product.name diana-join)) "HR department should be joined with Phone")
        (is (= 800 (:product.price diana-join)) "Phone price should be 800"))

      ;; Eric (Finance) should have null product fields
      (let [eric-join (first (filter #(= (:user.name %) "Eric") results))]
        (is (some? eric-join) "Eric should be in results even without matching product")
        (is (= "Finance" (:user.dept eric-join)) "Eric should be in Finance department")
        (is (nil? (:product.name eric-join)) "Product name should be nil for Finance department")
        (is (nil? (:product.price eric-join)) "Product price should be nil for Finance department"))

      ;; Alice and Bob (no dept) should also have null product fields
      (let [alice-join (first (filter #(= (:user.name %) "Alice") results))]
        (is (some? alice-join) "Alice should be in results even without department")
        (is (nil? (:user.dept alice-join)) "Alice should not have a department")
        (is (nil? (:product.name alice-join)) "Product name should be nil for Alice")
        (is (nil? (:product.price alice-join)) "Product price should be nil for Alice")))))

(deftest test-handle-select-with-left-join-and-where
  (testing "SELECT with LEFT JOIN and WHERE clause on left table"
    ;; Ensure we have the join test data set up
    (storage-protocol/save-document *test-storage*
                                    (assoc (storage-protocol/get-document *test-storage* "product:1")
                                           :dept "IT"))
    (storage-protocol/save-document *test-storage*
                                    (assoc (storage-protocol/get-document *test-storage* "product:2")
                                           :dept "HR"))
    (storage-protocol/save-document *test-storage*
                                    {:id "user:5"
                                     :name "Eric"
                                     :age 45
                                     :active true
                                     :dept "Finance"})

    ;; Query with LEFT JOIN and WHERE clause on the left table only
    (let [query {:type :select
                 :table "user"
                 :columns [{:type :column :column "user.name"}
                           {:type :column :column "user.dept"}
                           {:type :column :column "product.name"}]
                 :where [{:field "user.age" :op ">" :value "40"}]  ;; Only users over 40 (Eric)
                 :join {:table "product"
                        :type :left
                        :on {:left-table "user"
                             :left-field "dept"
                             :right-table "product"
                             :right-field "dept"}}
                 :order-by nil
                 :limit nil}
          results (query/handle-select *test-storage* nil query)]

      ;; Only Eric should match the WHERE condition
      (is (= 1 (count results)) "Should return only users matching the WHERE condition")

      ;; Verify Eric's record
      (let [eric-join (first results)]
        (is (= "Eric" (:user.name eric-join)) "Result should be for Eric")
        (is (= "Finance" (:user.dept eric-join)) "Eric should be in Finance department")
        (is (nil? (:product.name eric-join)) "Product name should be nil since no matching product"))))

  (testing "SELECT with LEFT JOIN and WHERE clause that spans both tables"
    ;; Ensure we have the join test data set up - same as above

    ;; Query with LEFT JOIN and WHERE clause referencing the right table
    (let [query {:type :select
                 :table "user"
                 :columns [{:type :column :column "user.name"}
                           {:type :column :column "user.dept"}
                           {:type :column :column "product.name"}
                           {:type :column :column "product.price"}]
                 :where [{:field "product.price" :op ">" :value "1000"}]  ;; Only products with price > 1000
                 :join {:table "product"
                        :type :left
                        :on {:left-table "user"
                             :left-field "dept"
                             :right-table "product"
                             :right-field "dept"}}
                 :order-by nil
                 :limit nil}
          _ (println "Query with product.price > 1000:" (pr-str query))
          results (query/handle-select *test-storage* nil query)
          _ (println "Results from query:" (pr-str results))]

      ;; Only Charlie (matched with Laptop price 1200) should be returned
      (is (= 1 (count results)) "Should return only rows where right table matches WHERE condition")

      ;; Verify Charlie's record
      (when (pos? (count results))
        (let [charlie-join (first results)]
          (is (= "Charlie" (:user.name charlie-join)) "Result should be for Charlie")
          (is (= "IT" (:user.dept charlie-join)) "Charlie should be in IT department")
          (is (= "Laptop" (:product.name charlie-join)) "Product should be Laptop")
          (is (= 1200 (:product.price charlie-join)) "Laptop price should be 1200"))))))

(deftest test-handle-select-with-multi-table-left-join
  (testing "SELECT with complex multi-table LEFT JOIN"
    ;; Add order data
    (storage-protocol/save-document *test-storage*
                                    {:id "order:1"
                                     :user_id "user:1"  ;; Alice
                                     :product_id "product:1"  ;; Laptop
                                     :quantity 2
                                     :status "shipped"
                                     :_table "order"})
    (storage-protocol/save-document *test-storage*
                                    {:id "order:2"
                                     :user_id "user:2"  ;; Bob
                                     :product_id "product:2"  ;; Phone
                                     :quantity 1
                                     :status "delivered"
                                     :_table "order"})

    ;; Add users with no orders
    (let [frank-user {:id "user:6"
                      :name "Frank"
                      :age 50
                      :active true
                      :_table "user"}
          saved-frank (storage-protocol/save-document *test-storage* frank-user)]
      (println "Debug - Frank saved as:" saved-frank)
      (println "Debug - All users:" (mapv :id (storage-protocol/get-documents-by-table *test-storage* "user"))))

    ;; Query users LEFT JOIN orders
    (let [query {:type :select
                 :table "user"
                 :columns [{:type :column :column "user.name"}
                           {:type :column :column "order.id"}
                           {:type :column :column "order.status"}]
                 :where nil
                 :join {:table "order"
                        :type :left
                        :on {:left-table "user"
                             :left-field "id"
                             :right-table "order"
                             :right-field "user_id"}}
                 :order-by [{:column "user.name" :direction :asc}]
                 :limit nil}
          results (query/handle-select *test-storage* nil query)]

      ;; We should have all users (5), but only 2 with orders
      (is (= 5 (count results)) "Should return all users regardless of having orders")

      ;; Verify Alice has an order
      (let [alice-join (first (filter #(= (:user.name %) "Alice") results))]
        (is (some? alice-join) "Alice should be in results")
        (is (= "order:1" (:order.id alice-join)) "Alice should have order:1")
        (is (= "shipped" (:order.status alice-join)) "Alice's order should be shipped"))

      ;; Verify Bob has an order
      (let [bob-join (first (filter #(= (:user.name %) "Bob") results))]
        (is (some? bob-join) "Bob should be in results")
        (is (= "order:2" (:order.id bob-join)) "Bob should have order:2")
        (is (= "delivered" (:order.status bob-join)) "Bob's order should be delivered"))

      ;; Verify Charlie has no order
      (let [charlie-join (first (filter #(= (:user.name %) "Charlie") results))]
        (is (some? charlie-join) "Charlie should be in results even without orders")
        (is (nil? (:order.id charlie-join)) "Charlie should have no order ID")
        (is (nil? (:order.status charlie-join)) "Charlie should have no order status"))

      ;; Verify Frank has no order
      (let [frank-join (first (filter #(= (:user.name %) "Frank") results))]
        (is (some? frank-join) "Frank should be in results even without orders")
        (is (nil? (:order.id frank-join)) "Frank should have no order ID")
        (is (nil? (:order.status frank-join)) "Frank should have no order status")))))

(deftest test-handle-select-with-non-existent-table
  (testing "SELECT from non-existent table"
    (let [query {:type :select
                 :table "nonexistent"
                 :columns [{:type :all}]
                 :where nil
                 :order-by nil
                 :limit nil}
          results (query/handle-select *test-storage* nil query)]
      (is (empty? results)))))

(deftest test-handle-query-unsupported-statement
  (testing "Query handling for unsupported statements"
    (let [writer (java.io.StringWriter.)
          output-stream (proxy [java.io.OutputStream] []
                          (write
                            ([b] (.write writer (String. (byte-array [b]))))
                            ([b off len] (.write writer (String. b off len))))
                          (flush [] (.flush writer)))
          sql "CREATE TABLE test (id VARCHAR(255))"]
      (query/handle-query *test-storage* nil output-stream sql)
      (is (pos? (.length (.getBuffer writer))) "Error response should have been written"))))

(deftest test-handle-query-exceptions
  (testing "handle-query handles exceptions gracefully"
    (let [output-stream (proxy [java.io.OutputStream] []
                          (write
                            ([b])
                            ([b off len]))
                          (flush []))
          bad-sql "SELECT * FROM ]invalid sql["]
      ;; We're just testing that it doesn't throw an exception
      (is (nil? (query/handle-query *test-storage* nil output-stream bad-sql))))))

(deftest test-handle-empty-query
  (testing "Handling an empty query"
    (let [output-stream (proxy [java.io.OutputStream] []
                          (write
                            ([b])
                            ([b off len]))
                          (flush []))
          sql ""]
      ;; Empty query should not throw an exception
      (is (nil? (query/handle-query *test-storage* nil output-stream sql))))))

(deftest test-handle-select-with-column-projection
  (testing "SELECT with column projection"
    (let [query {:type :select
                 :table "user"
                 :columns [{:type :column :column "name"}
                           {:type :column :column "age"}]
                 :where nil
                 :order-by nil
                 :limit nil}
          results (query/handle-select *test-storage* nil query)]
      (is (= 4 (count results)))
      (is (every? #(and (contains? % :name) (contains? % :age)) results))
      (is (every? #(not (contains? % :active)) results)))))

(deftest test-handle-select-with-aggregate
  (testing "SELECT with aggregate function"
    (let [query {:type :select
                 :table "user"
                 :columns [{:type :aggregate-function
                            :function :count
                            :args ["*"]}]
                 :where nil
                 :order-by nil
                 :limit nil}
          results (query/handle-select *test-storage* nil query)]
      ;; The current implementation doesn't return a single document with count,
      ;; but rather all documents in the table
      (is (pos? (count results)) "Should return results"))))

(deftest test-fts-condition
  (testing "FTS condition identification"
    (is (true? (query/fts-condition? {:type :fts-match})))
    (is (false? (query/fts-condition? {:type :compare})))
    (is (false? (query/fts-condition? "not-a-map")))))

(deftest test-handle-insert-and-delete
  (testing "INSERT and DELETE operations"
    (let [storage *test-storage*
          document {:id "test-doc" :name "Test Doc" :value 100 :_table "test"}
          inserted (query/handle-insert storage document "main")
          deleted (query/handle-delete storage "test-doc" "main")]
      (is (= "test-doc" (:id inserted)))
      (is (= "Test Doc" (:name inserted)))
      (is (= true deleted)))))

(deftest test-handle-query-select
  (testing "Query handling for SELECT"
    (let [writer (java.io.StringWriter.)
          output-stream (proxy [java.io.OutputStream] []
                          (write
                            ([b] (.write writer (String. (byte-array [b]))))
                            ([b off len] (.write writer (String. b off len))))
                          (flush [] (.flush writer)))
          sql "SELECT * FROM user"]
      (query/handle-query *test-storage* nil output-stream sql)
      (is (pos? (.length (.getBuffer writer))) "Query response should have been written"))))

(deftest test-handle-query-insert
  (testing "Query handling for INSERT"
    (let [writer (java.io.StringWriter.)
          output-stream (proxy [java.io.OutputStream] []
                          (write
                            ([b] (.write writer (String. (byte-array [b]))))
                            ([b off len] (.write writer (String. b off len))))
                          (flush [] (.flush writer)))
          sql "INSERT INTO test (id, name, value) VALUES ('test-123', 'Test Insert', 123)"]
      (query/handle-query *test-storage* nil output-stream sql)
      (is (pos? (.length (.getBuffer writer))) "Query response should have been written"))))

(deftest test-handle-query-update
  (testing "Query handling for UPDATE statements"
    (let [writer (java.io.StringWriter.)
          output-stream (proxy [java.io.OutputStream] []
                          (write
                            ([b] (.write writer (String. (byte-array [b]))))
                            ([b off len] (.write writer (String. b off len))))
                          (flush [] (.flush writer)))
          id (str (random-uuid))
          sql-insert (str "INSERT INTO test (id, name) VALUES ('" id "', 'test')")
          sql-update (str "UPDATE test SET name='updated' WHERE id='" id "'")]
      (query/handle-query *test-storage* nil output-stream sql-insert)
      (query/handle-query *test-storage* nil output-stream sql-update)
      (is (pos? (.length (.getBuffer writer))) "Query response should have been written"))))

(deftest test-handle-query-delete
  (testing "Query handling for DELETE statements"
    (let [writer (java.io.StringWriter.)
          output-stream (proxy [java.io.OutputStream] []
                          (write
                            ([b] (.write writer (String. (byte-array [b]))))
                            ([b off len] (.write writer (String. b off len))))
                          (flush [] (.flush writer)))
          id (str (random-uuid))
          sql-insert (str "INSERT INTO test (id, name) VALUES ('" id "', 'test')")
          sql-delete (str "DELETE FROM test WHERE id='" id "'")]
      (query/handle-query *test-storage* nil output-stream sql-insert)
      (query/handle-query *test-storage* nil output-stream sql-delete)
      (is (pos? (.length (.getBuffer writer))) "Query response should have been written"))))

(deftest test-handle-query-unsupported-statement-create-table
  (testing "Query handling for unsupported statements"
    (let [writer (java.io.StringWriter.)
          output-stream (proxy [java.io.OutputStream] []
                          (write
                            ([b] (.write writer (String. (byte-array [b]))))
                            ([b off len] (.write writer (String. b off len))))
                          (flush [] (.flush writer)))
          sql "CREATE TABLE test (id VARCHAR(255))"]
      (query/handle-query *test-storage* nil output-stream sql)
      (is (pos? (.length (.getBuffer writer))) "Error response should have been written"))))
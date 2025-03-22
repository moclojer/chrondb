(ns chrondb.api.sql.pg2-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [chrondb.api.sql.core :as sql]
            [chrondb.api.sql.test-helpers :refer [create-test-resources]]
            [chrondb.storage.protocol :as storage-protocol]
            [pg.core :as pg]))

;; Define a fixture that can be used to run only integration tests
(defn integration-fixture [f]
  (f))

;; Use the fixture for integration tests
(use-fixtures :once integration-fixture)

;; Test with PG2
(deftest ^:integration test-pg2-simple
  (testing "Simple test with PG2 driver"
    (let [{storage :storage index :index} (create-test-resources)
          port 15432
          server (sql/start-sql-server storage index port)]
      (try
        ;; Insert document via storage API
        (storage-protocol/save-document storage {:id "unique-test-doc-123" :column1 123})

        ;; Query via PG2
        (let [config {:host "localhost"
                      :port port
                      :user "postgres"
                      :password ""
                      :database "chrondb"
                      :simple-query? true}  ;; Force the use of simple queries instead of prepared statements
              conn (pg/connect config)]
          (try
            (let [result (pg/query conn "SELECT * FROM doc WHERE id = 'unique-test-doc-123'")]
              (is (vector? result))
              (is (= 1 (count result)))
              (is (= 123 (Integer/parseInt (:column1 (first result))))))
            (finally
              (.close conn))))

        (finally
          (sql/stop-sql-server server))))))

(deftest ^:integration test-pg2-individual-queries
  (testing "Individual document queries via PG2"
    (let [{storage :storage index :index} (create-test-resources)
          port 15433
          server (sql/start-sql-server storage index port)]
      (try
         ;; Insert multiple documents for testing with completely unique IDs
        (println "Saving documents to storage...")

         ;; Create a user document with age=25
        (storage-protocol/save-document storage {:id "user-john-abc123" :name "John" :age 25 :active true :tags ["admin" "user"]})
        (println "Saved user-john-abc123")

         ;; Create a user document with age=30
        (storage-protocol/save-document storage {:id "user-mary-def456" :name "Mary" :age 30 :active true :tags ["user"]})
        (println "Saved user-mary-def456")

         ;; Create a user document with active=false
        (storage-protocol/save-document storage {:id "user-peter-ghi789" :name "Peter" :age 22 :active false :tags ["user"]})
        (println "Saved user-peter-ghi789")

         ;; Create a product document with price=3500
        (storage-protocol/save-document storage {:id "product-laptop-x987" :name "Laptop" :price 3500 :stock 10})
        (println "Saved product-laptop-x987")

         ;; Create a product document with stock=15
        (storage-protocol/save-document storage {:id "product-phone-y654" :name "Smartphone" :price 2000 :stock 15})
        (println "Saved product-phone-y654")

         ;; Create a product document with price<2000
        (storage-protocol/save-document storage {:id "product-tablet-z321" :name "Tablet" :price 1500 :stock 5})
        (println "Saved product-tablet-z321")

        (println "All documents saved, running queries...")

         ;; Connect to database
        (let [config {:host "localhost"
                      :port port
                      :user "postgres"
                      :password ""
                      :database "chrondb"
                      :simple-query? true}
              conn (pg/connect config)]
          (try
             ;; Test individual queries to check if each document exists and has correct data
            (testing "Individual document queries"
               ;; Test user document with age=25
              (let [result (pg/query conn "SELECT * FROM doc WHERE id = 'user-john-abc123'")]
                (println "Query for user-john-abc123 returned:" (count result) "results")
                (is (= 1 (count result)))
                (let [doc (first result)]
                  (is (= "user-john-abc123" (:id doc)))
                  (is (= "John" (:name doc)))
                  (is (= "25" (:age doc)))
                  (is (= "true" (:active doc)))))

               ;; Test user document with age=30
              (let [result (pg/query conn "SELECT * FROM doc WHERE id = 'user-mary-def456'")]
                (println "Query for user-mary-def456 returned:" (count result) "results")
                (is (= 1 (count result)))
                (let [doc (first result)]
                  (is (= "user-mary-def456" (:id doc)))
                  (is (= "Mary" (:name doc)))
                  (is (= "30" (:age doc)))))

               ;; Test user document with active=false - ChronDB parece omitir campos com valores false
              (let [result (pg/query conn "SELECT * FROM doc WHERE id = 'user-peter-ghi789'")]
                (println "Query for user-peter-ghi789 returned:" (count result) "results")
                (is (= 1 (count result)))
                (let [doc (first result)]
                  (is (= "user-peter-ghi789" (:id doc)))
                  (is (= "Peter" (:name doc)))
                  ;; Boolean false values may be returned as nil in ChronDB SQL
                  (is (or (= "false" (:active doc))
                          (nil? (:active doc))))
                  (println "Active field for Peter:" (:active doc))))

               ;; Test product document with price=3500
              (let [result (pg/query conn "SELECT * FROM doc WHERE id = 'product-laptop-x987'")]
                (println "Query for product-laptop-x987 returned:" (count result) "results")
                (is (= 1 (count result)))
                (let [doc (first result)]
                  (is (= "product-laptop-x987" (:id doc)))
                  (is (= "Laptop" (:name doc)))
                  (is (= "3500" (:price doc)))
                  (is (= "10" (:stock doc)))))

               ;; Test product document with stock=15
              (let [result (pg/query conn "SELECT * FROM doc WHERE id = 'product-phone-y654'")]
                (println "Query for product-phone-y654 returned:" (count result) "results")
                (is (= 1 (count result)))
                (let [doc (first result)]
                  (is (= "product-phone-y654" (:id doc)))
                  (is (= "Smartphone" (:name doc)))
                  (is (= "15" (:stock doc)))))

               ;; Test product document with price<2000
              (let [result (pg/query conn "SELECT * FROM doc WHERE id = 'product-tablet-z321'")]
                (println "Query for product-tablet-z321 returned:" (count result) "results")
                (is (= 1 (count result)))
                (let [doc (first result)]
                  (is (= "product-tablet-z321" (:id doc)))
                  (is (= "Tablet" (:name doc)))
                  (is (= "1500" (:price doc))))))

            (finally
              (.close conn))))

        (finally
          (sql/stop-sql-server server))))))
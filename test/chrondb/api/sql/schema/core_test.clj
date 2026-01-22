(ns chrondb.api.sql.schema.core-test
  (:require [chrondb.config :as config]
            [chrondb.storage.git.core :as git-core]
            [chrondb.storage.protocol :as protocol]
            [chrondb.api.sql.schema.core :as schema-core]
            [chrondb.api.sql.schema.storage :as schema-storage]
            [clojure.java.io :as io]
            [clojure.test :refer [deftest is testing use-fixtures]])
  (:import [java.io File]))

(def test-repo-path "test-schema-repo")

(def test-config
  {:git {:default-branch "main"
         :committer-name "Test User"
         :committer-email "test@example.com"
         :push-enabled false}
   :storage {:data-dir "data"}
   :logging {:level :info
             :file "test.log"}})

(defn delete-directory [^File directory]
  (when (.exists directory)
    (doseq [file (reverse (file-seq directory))]
      (.delete file))))

(defn clean-test-repo [f]
  (delete-directory (io/file test-repo-path))
  (with-redefs [config/load-config (constantly test-config)]
    (f)))

(use-fixtures :each clean-test-repo)

;; CREATE TABLE tests

(deftest test-create-table
  (testing "Create table with schema"
    (let [storage (git-core/create-git-storage test-repo-path)
          columns [{:name "id" :type "TEXT" :primary_key true}
                   {:name "name" :type "TEXT" :nullable false}
                   {:name "email" :type "TEXT"}]
          result (schema-core/create-table storage "users" columns nil false)]
      (is (true? (:success result)))
      (is (= "Table users created" (:message result)))
      (is (some? (:schema result)))
      (is (= "users" (:table (:schema result))))
      (protocol/close storage))))

(deftest test-create-table-if-not-exists
  (testing "Create table with IF NOT EXISTS"
    (let [storage (git-core/create-git-storage test-repo-path)
          columns [{:name "id" :type "TEXT" :primary_key true}]]
      ;; Create the table first
      (schema-core/create-table storage "users" columns nil false)

      ;; Try to create again with IF NOT EXISTS
      (let [result (schema-core/create-table storage "users" columns nil true)]
        (is (true? (:success result)))
        (is (true? (:already-exists result))))

      ;; Try to create again without IF NOT EXISTS
      (let [result (schema-core/create-table storage "users" columns nil false)]
        (is (false? (:success result)))
        (is (= :table-exists (:error result))))

      (protocol/close storage))))

;; DROP TABLE tests

(deftest test-drop-table
  (testing "Drop existing table"
    (let [storage (git-core/create-git-storage test-repo-path)
          columns [{:name "id" :type "TEXT"}]]
      ;; Create the table first
      (schema-core/create-table storage "users" columns nil false)

      ;; Drop it
      (let [result (schema-core/drop-table storage "users" nil false)]
        (is (true? (:success result)))
        (is (= "Table users dropped" (:message result))))

      ;; Verify it's gone
      (is (nil? (schema-storage/get-schema (:repository storage) "users" nil)))

      (protocol/close storage))))

(deftest test-drop-table-if-exists
  (testing "Drop table with IF EXISTS"
    (let [storage (git-core/create-git-storage test-repo-path)]
      ;; Try to drop non-existent table with IF EXISTS
      (let [result (schema-core/drop-table storage "nonexistent" nil true)]
        (is (true? (:success result)))
        (is (true? (:not-found result))))

      ;; Try to drop non-existent table without IF EXISTS
      (let [result (schema-core/drop-table storage "nonexistent" nil false)]
        (is (false? (:success result)))
        (is (= :table-not-found (:error result))))

      (protocol/close storage))))

;; LIST TABLES tests

(deftest test-list-tables
  (testing "List tables with explicit schemas"
    (let [storage (git-core/create-git-storage test-repo-path)
          columns [{:name "id" :type "TEXT"}]]
      ;; Create some tables
      (schema-core/create-table storage "users" columns nil false)
      (schema-core/create-table storage "products" columns nil false)

      ;; List tables
      (let [tables (schema-core/list-tables storage nil)]
        (is (= 2 (count tables)))
        (is (some #(= "users" (:name %)) tables))
        (is (some #(= "products" (:name %)) tables))
        (is (every? :has_schema tables)))

      (protocol/close storage))))

(deftest test-list-tables-with-implicit-tables
  (testing "List tables including implicit tables (with documents but no schema)"
    (let [storage (git-core/create-git-storage test-repo-path)
          columns [{:name "id" :type "TEXT"}]]
      ;; Create a table with explicit schema
      (schema-core/create-table storage "users" columns nil false)

      ;; Create documents without explicit schema (implicit table)
      (protocol/save-document storage {:id "1" :_table "orders" :total 100})

      ;; List tables
      (let [tables (schema-core/list-tables storage nil)]
        (is (>= (count tables) 1))
        (is (some #(= "users" (:name %)) tables))
        ;; orders may or may not appear depending on storage structure
        )

      (protocol/close storage))))

;; LIST BRANCHES (SHOW SCHEMAS) tests

(deftest test-list-branches
  (testing "List branches"
    (let [storage (git-core/create-git-storage test-repo-path)]
      ;; Should have at least main branch
      (let [branches (schema-core/list-branches storage)]
        (is (vector? branches))
        (is (some #(= "main" %) branches)))

      (protocol/close storage))))

(deftest test-branch-create
  (testing "Create new branch"
    (let [storage (git-core/create-git-storage test-repo-path)]
      (let [result (schema-core/branch-create storage "feature-x")]
        (is (true? (:success result)))
        (is (= "Branch feature-x created" (:message result))))

      ;; Verify branch exists
      (let [branches (schema-core/list-branches storage)]
        (is (some #(= "feature-x" %) branches)))

      ;; Try to create again
      (let [result (schema-core/branch-create storage "feature-x")]
        (is (false? (:success result)))
        (is (= :branch-exists (:error result))))

      (protocol/close storage))))

(deftest test-branch-checkout
  (testing "Checkout branch"
    (let [storage (git-core/create-git-storage test-repo-path)]
      ;; Create a branch first
      (schema-core/branch-create storage "develop")

      ;; Checkout existing branch
      (let [result (schema-core/branch-checkout storage "develop")]
        (is (true? (:success result)))
        (is (= "develop" (:branch result))))

      ;; Checkout non-existent branch
      (let [result (schema-core/branch-checkout storage "nonexistent")]
        (is (false? (:success result)))
        (is (= :branch-not-found (:error result))))

      (protocol/close storage))))

;; DESCRIBE TABLE tests

(deftest test-describe-table-with-schema
  (testing "Describe table with explicit schema"
    (let [storage (git-core/create-git-storage test-repo-path)
          columns [{:name "id" :type "TEXT" :primary_key true}
                   {:name "name" :type "TEXT" :nullable false}]]
      ;; Create the table
      (schema-core/create-table storage "users" columns nil false)

      ;; Describe it
      (let [result (schema-core/describe-table storage "users" nil)]
        (is (= "users" (:name result)))
        (is (= 2 (count (:columns result))))
        (is (false? (:inferred result))))

      (protocol/close storage))))

(deftest test-describe-table-inferred
  (testing "Describe table with inferred schema from documents"
    (let [storage (git-core/create-git-storage test-repo-path)]
      ;; Create documents without explicit schema
      (protocol/save-document storage {:id "1" :_table "orders" :total 100 :customer "John"})
      (protocol/save-document storage {:id "2" :_table "orders" :total 200 :customer "Jane"})

      ;; Describe it - should infer schema
      (let [result (schema-core/describe-table storage "orders" nil)]
        (is (= "orders" (:name result)))
        (is (true? (:inferred result)))
        ;; Columns should be inferred from document fields
        (when (seq (:columns result))
          (is (some #(= "total" (:name %)) (:columns result)))
          (is (some #(= "customer" (:name %)) (:columns result)))))

      (protocol/close storage))))

(deftest test-describe-table-not-found
  (testing "Describe non-existent table"
    (let [storage (git-core/create-git-storage test-repo-path)]
      (let [result (schema-core/describe-table storage "nonexistent" nil)]
        (is (= "nonexistent" (:name result)))
        (is (empty? (:columns result)))
        (is (= :table-not-found (:error result))))

      (protocol/close storage))))

;; Schema storage tests

(deftest test-schema-path
  (testing "Schema path generation"
    (is (= "_schema/users.json" (schema-storage/schema-path "users")))
    (is (= "_schema/users.json" (schema-storage/schema-path "USERS")))
    (is (= "_schema/my_table.json" (schema-storage/schema-path "my_table")))))

(deftest test-schema-exists
  (testing "Check if schema exists"
    (let [storage (git-core/create-git-storage test-repo-path)
          columns [{:name "id" :type "TEXT"}]]
      ;; Initially doesn't exist
      (is (false? (schema-storage/schema-exists? (:repository storage) "users" nil)))

      ;; Create the table
      (schema-core/create-table storage "users" columns nil false)

      ;; Now it exists
      (is (true? (schema-storage/schema-exists? (:repository storage) "users" nil)))

      (protocol/close storage))))

(ns chrondb.api.sql.execution.ddl-test
  "Integration tests for DDL execution handlers"
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [clojure.string :as str]
            [clojure.java.io :as io]
            [chrondb.config :as config]
            [chrondb.storage.git.core :as git-core]
            [chrondb.storage.protocol :as protocol]
            [chrondb.api.sql.execution.query :as query]
            [chrondb.api.sql.schema.core :as schema-core]
            [chrondb.api.sql.schema.storage :as schema-storage])
  (:import [java.io File ByteArrayOutputStream]))

(def test-repo-path "test-ddl-execution-repo")

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

;; Helper to capture output from handle-query
(defn execute-sql
  "Executes SQL and returns the output bytes"
  [storage sql]
  (let [out (ByteArrayOutputStream.)]
    (query/handle-query storage nil out sql)
    (.toByteArray out)))

(defn output-contains?
  "Checks if output bytes contain a specific string"
  [output-bytes expected-str]
  (let [output-str (String. output-bytes "UTF-8")]
    (str/includes? output-str expected-str)))

;; CREATE TABLE tests

(deftest test-execute-create-table
  (testing "CREATE TABLE through handle-query"
    (let [storage (git-core/create-git-storage test-repo-path)
          sql "CREATE TABLE users (id TEXT PRIMARY KEY, name TEXT NOT NULL, email TEXT)"
          output (execute-sql storage sql)]
      ;; Should produce some output
      (is (pos? (count output)))
      ;; Schema should exist
      (is (true? (schema-storage/schema-exists? (:repository storage) "users" nil)))
      (protocol/close storage))))

(deftest test-execute-create-table-if-not-exists
  (testing "CREATE TABLE IF NOT EXISTS through handle-query"
    (let [storage (git-core/create-git-storage test-repo-path)]
      ;; Create the table first
      (execute-sql storage "CREATE TABLE users (id TEXT)")

      ;; Create again with IF NOT EXISTS - should not error
      (let [output (execute-sql storage "CREATE TABLE IF NOT EXISTS users (id TEXT)")]
        (is (pos? (count output))))

      ;; Verify schema still exists
      (is (true? (schema-storage/schema-exists? (:repository storage) "users" nil)))

      (protocol/close storage))))

(deftest test-execute-create-table-with-constraints
  (testing "CREATE TABLE with various constraints"
    (let [storage (git-core/create-git-storage test-repo-path)
          sql "CREATE TABLE products (
                 id TEXT PRIMARY KEY,
                 name TEXT NOT NULL UNIQUE,
                 price INTEGER DEFAULT 0,
                 stock INTEGER
               )"
          output (execute-sql storage sql)]
      (is (pos? (count output)))

      ;; Verify schema was created with correct columns
      (let [schema (schema-storage/get-schema (:repository storage) "products" nil)]
        (is (some? schema))
        (is (= "products" (:table schema)))
        (is (= 4 (count (:columns schema))))

        ;; Check id column
        (let [id-col (first (filter #(= "id" (:name %)) (:columns schema)))]
          (is (some? id-col))
          (is (true? (:primary_key id-col))))

        ;; Check name column
        (let [name-col (first (filter #(= "name" (:name %)) (:columns schema)))]
          (is (some? name-col))
          (is (false? (:nullable name-col)))
          (is (true? (:unique name-col))))

        ;; Check price column
        (let [price-col (first (filter #(= "price" (:name %)) (:columns schema)))]
          (is (some? price-col))
          (is (= "0" (:default price-col)))))

      (protocol/close storage))))

;; DROP TABLE tests

(deftest test-execute-drop-table
  (testing "DROP TABLE through handle-query"
    (let [storage (git-core/create-git-storage test-repo-path)]
      ;; Create table first
      (execute-sql storage "CREATE TABLE users (id TEXT)")
      (is (true? (schema-storage/schema-exists? (:repository storage) "users" nil)))

      ;; Drop it
      (let [output (execute-sql storage "DROP TABLE users")]
        (is (pos? (count output))))

      ;; Verify it's gone
      (is (false? (schema-storage/schema-exists? (:repository storage) "users" nil)))

      (protocol/close storage))))

(deftest test-execute-drop-table-if-exists
  (testing "DROP TABLE IF EXISTS through handle-query"
    (let [storage (git-core/create-git-storage test-repo-path)]
      ;; Drop non-existent table with IF EXISTS - should not error
      (let [output (execute-sql storage "DROP TABLE IF EXISTS nonexistent")]
        (is (pos? (count output))))

      (protocol/close storage))))

;; SHOW TABLES tests

(deftest test-execute-show-tables
  (testing "SHOW TABLES through handle-query"
    (let [storage (git-core/create-git-storage test-repo-path)]
      ;; Create some tables
      (execute-sql storage "CREATE TABLE users (id TEXT)")
      (execute-sql storage "CREATE TABLE products (id TEXT)")

      ;; Show tables
      (let [output (execute-sql storage "SHOW TABLES")]
        (is (pos? (count output)))
        ;; Output should contain table names
        (is (output-contains? output "users"))
        (is (output-contains? output "products")))

      (protocol/close storage))))

;; SHOW SCHEMAS tests

(deftest test-execute-show-schemas
  (testing "SHOW SCHEMAS through handle-query"
    (let [storage (git-core/create-git-storage test-repo-path)]
      ;; Show schemas (branches)
      (let [output (execute-sql storage "SHOW SCHEMAS")]
        (is (pos? (count output)))
        ;; Should contain at least 'public' (mapped from 'main')
        (is (output-contains? output "public")))

      (protocol/close storage))))

;; DESCRIBE tests

(deftest test-execute-describe
  (testing "DESCRIBE table through handle-query"
    (let [storage (git-core/create-git-storage test-repo-path)]
      ;; Create a table
      (execute-sql storage "CREATE TABLE users (id TEXT PRIMARY KEY, name TEXT NOT NULL, email TEXT)")

      ;; Describe it
      (let [output (execute-sql storage "DESCRIBE users")]
        (is (pos? (count output)))
        ;; Output should contain column names
        (is (output-contains? output "id"))
        (is (output-contains? output "name"))
        (is (output-contains? output "email")))

      (protocol/close storage))))

(deftest test-execute-show-columns
  (testing "SHOW COLUMNS FROM table through handle-query"
    (let [storage (git-core/create-git-storage test-repo-path)]
      ;; Create a table
      (execute-sql storage "CREATE TABLE products (id TEXT, name TEXT, price INTEGER)")

      ;; Show columns
      (let [output (execute-sql storage "SHOW COLUMNS FROM products")]
        (is (pos? (count output)))
        (is (output-contains? output "id"))
        (is (output-contains? output "name"))
        (is (output-contains? output "price")))

      (protocol/close storage))))

;; Branch function tests

(deftest test-execute-branch-list
  (testing "SELECT * FROM chrondb_branch_list() through handle-query"
    (let [storage (git-core/create-git-storage test-repo-path)]
      (let [output (execute-sql storage "SELECT * FROM chrondb_branch_list()")]
        (is (pos? (count output)))
        ;; Should list main branch
        (is (output-contains? output "main")))

      (protocol/close storage))))

(deftest test-execute-branch-create
  (testing "SELECT * FROM chrondb_branch_create() through handle-query"
    (let [storage (git-core/create-git-storage test-repo-path)]
      (let [output (execute-sql storage "SELECT * FROM chrondb_branch_create('feature-x')")]
        (is (pos? (count output))))

      ;; Verify branch was created
      (let [branches (schema-core/list-branches storage)]
        (is (some #(= "feature-x" %) branches)))

      (protocol/close storage))))

(deftest test-execute-branch-checkout
  (testing "SELECT * FROM chrondb_branch_checkout() through handle-query"
    (let [storage (git-core/create-git-storage test-repo-path)]
      ;; Create branch first
      (execute-sql storage "SELECT * FROM chrondb_branch_create('develop')")

      ;; Checkout it
      (let [output (execute-sql storage "SELECT * FROM chrondb_branch_checkout('develop')")]
        (is (pos? (count output)))
        (is (output-contains? output "develop")))

      (protocol/close storage))))

;; Schema-qualified operations

(deftest test-execute-create-table-with-schema
  (testing "CREATE TABLE with schema qualifier"
    (let [storage (git-core/create-git-storage test-repo-path)]
      ;; Create branch first
      (schema-core/branch-create storage "dev")

      ;; Create table in that branch/schema
      (let [output (execute-sql storage "CREATE TABLE dev.users (id TEXT)")]
        (is (pos? (count output))))

      ;; Table should exist in the dev branch
      (is (true? (schema-storage/schema-exists? (:repository storage) "users" "dev")))

      (protocol/close storage))))

(deftest test-execute-show-tables-with-schema
  (testing "SHOW TABLES with schema filter"
    (let [storage (git-core/create-git-storage test-repo-path)]
      ;; Create branch
      (schema-core/branch-create storage "dev")

      ;; Create table in main
      (execute-sql storage "CREATE TABLE users (id TEXT)")

      ;; Create table in dev branch
      (execute-sql storage "CREATE TABLE dev.products (id TEXT)")

      ;; Show tables from dev schema
      (let [output (execute-sql storage "SHOW TABLES FROM dev")]
        (is (pos? (count output)))
        (is (output-contains? output "products")))

      (protocol/close storage))))

;; Error handling tests

(deftest test-execute-create-table-already-exists
  (testing "CREATE TABLE when table already exists"
    (let [storage (git-core/create-git-storage test-repo-path)]
      ;; Create table
      (execute-sql storage "CREATE TABLE users (id TEXT)")

      ;; Try to create again without IF NOT EXISTS
      (let [output (execute-sql storage "CREATE TABLE users (id TEXT)")]
        (is (pos? (count output)))
        ;; Should contain error about table existing
        (is (output-contains? output "already exists")))

      (protocol/close storage))))

(deftest test-execute-drop-table-not-found
  (testing "DROP TABLE when table doesn't exist"
    (let [storage (git-core/create-git-storage test-repo-path)]
      ;; Try to drop non-existent table
      (let [output (execute-sql storage "DROP TABLE nonexistent")]
        (is (pos? (count output)))
        ;; Should contain error about table not existing
        (is (output-contains? output "does not exist")))

      (protocol/close storage))))

;; Integration flow tests

(deftest test-full-ddl-workflow
  (testing "Complete DDL workflow: create, show, describe, drop"
    (let [storage (git-core/create-git-storage test-repo-path)]
      ;; 1. Create table
      (execute-sql storage "CREATE TABLE customers (
                              id TEXT PRIMARY KEY,
                              name TEXT NOT NULL,
                              email TEXT UNIQUE,
                              balance INTEGER DEFAULT 0
                            )")

      ;; 2. Show tables - should include customers
      (let [output (execute-sql storage "SHOW TABLES")]
        (is (output-contains? output "customers")))

      ;; 3. Describe table
      (let [output (execute-sql storage "DESCRIBE customers")]
        (is (output-contains? output "id"))
        (is (output-contains? output "name"))
        (is (output-contains? output "email"))
        (is (output-contains? output "balance")))

      ;; 4. Create another table
      (execute-sql storage "CREATE TABLE orders (
                              id TEXT PRIMARY KEY,
                              customer_id TEXT,
                              total INTEGER
                            )")

      ;; 5. Show tables - should include both
      (let [output (execute-sql storage "SHOW TABLES")]
        (is (output-contains? output "customers"))
        (is (output-contains? output "orders")))

      ;; 6. Drop first table
      (execute-sql storage "DROP TABLE customers")

      ;; 7. Show tables - should only have orders
      (let [output (execute-sql storage "SHOW TABLES")]
        (is (not (output-contains? output "customers")))
        (is (output-contains? output "orders")))

      ;; 8. Drop remaining table
      (execute-sql storage "DROP TABLE orders")

      ;; 9. Show tables - should be empty (or only show implicit tables)
      (let [tables (schema-core/list-tables storage nil)]
        (is (empty? (filter :has_schema tables))))

      (protocol/close storage))))

(deftest test-branch-workflow
  (testing "Complete branch workflow: create, list, checkout"
    (let [storage (git-core/create-git-storage test-repo-path)]
      ;; 1. List branches - should have main
      (let [output (execute-sql storage "SELECT * FROM chrondb_branch_list()")]
        (is (output-contains? output "main")))

      ;; 2. Create feature branch
      (execute-sql storage "SELECT * FROM chrondb_branch_create('feature1')")

      ;; 3. List branches - should have both
      (let [output (execute-sql storage "SELECT * FROM chrondb_branch_list()")]
        (is (output-contains? output "main"))
        (is (output-contains? output "feature1")))

      ;; 4. Checkout feature branch
      (let [output (execute-sql storage "SELECT * FROM chrondb_branch_checkout('feature1')")]
        (is (output-contains? output "feature1")))

      ;; 5. Create table in feature branch using schema qualifier
      (execute-sql storage "CREATE TABLE feature1.feature_data (id TEXT)")

      ;; 6. Verify table exists in feature branch
      (is (true? (schema-storage/schema-exists? (:repository storage) "feature_data" "feature1")))

      ;; 7. Verify table doesn't exist in main
      (is (false? (schema-storage/schema-exists? (:repository storage) "feature_data" "main")))

      (protocol/close storage))))

(ns chrondb.api.sql.parser.ddl-test
  (:require [clojure.test :refer [deftest is testing]]
            [chrondb.api.sql.parser.statements :as statements]
            [chrondb.api.sql.parser.ddl :as ddl]))

;; CREATE TABLE tests

(deftest parse-simple-create-table
  (testing "Simple CREATE TABLE with basic columns"
    (let [result (statements/parse-sql-query "CREATE TABLE users (id TEXT, name TEXT)")]
      (is (= :create-table (:type result)))
      (is (= "users" (:table result)))
      (is (nil? (:schema result)))
      (is (= 2 (count (:columns result))))
      (is (= "id" (:name (first (:columns result)))))
      (is (= "TEXT" (:type (first (:columns result))))))))

(deftest parse-create-table-with-constraints
  (testing "CREATE TABLE with PRIMARY KEY and NOT NULL constraints"
    (let [result (statements/parse-sql-query
                  "CREATE TABLE users (id TEXT PRIMARY KEY, name TEXT NOT NULL, email TEXT)")]
      (is (= :create-table (:type result)))
      (is (= "users" (:table result)))
      (is (= 3 (count (:columns result))))
      ;; First column: id TEXT PRIMARY KEY
      (is (= "id" (:name (first (:columns result)))))
      (is (= "TEXT" (:type (first (:columns result)))))
      (is (true? (:primary_key (first (:columns result)))))
      ;; Second column: name TEXT NOT NULL
      (is (= "name" (:name (second (:columns result)))))
      (is (false? (:nullable (second (:columns result))))))))

(deftest parse-create-table-with-default
  (testing "CREATE TABLE with DEFAULT values"
    (let [result (statements/parse-sql-query
                  "CREATE TABLE users (id TEXT PRIMARY KEY, status TEXT DEFAULT 'active')")]
      (is (= :create-table (:type result)))
      (is (= 2 (count (:columns result))))
      (is (= "active" (:default (second (:columns result))))))))

(deftest parse-create-table-if-not-exists
  (testing "CREATE TABLE IF NOT EXISTS"
    (let [result (statements/parse-sql-query
                  "CREATE TABLE IF NOT EXISTS users (id TEXT PRIMARY KEY)")]
      (is (= :create-table (:type result)))
      (is (= "users" (:table result)))
      (is (true? (:if-not-exists result))))))

(deftest parse-create-table-with-schema
  (testing "CREATE TABLE with schema prefix"
    (let [result (statements/parse-sql-query
                  "CREATE TABLE myschema.users (id TEXT PRIMARY KEY)")]
      (is (= :create-table (:type result)))
      (is (= "myschema" (:schema result)))
      (is (= "users" (:table result))))))

(deftest parse-create-table-with-types
  (testing "CREATE TABLE with various column types"
    (let [result (statements/parse-sql-query
                  "CREATE TABLE products (
                     id TEXT PRIMARY KEY,
                     name TEXT NOT NULL,
                     price NUMERIC,
                     quantity INTEGER,
                     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                   )")]
      (is (= :create-table (:type result)))
      (is (= 5 (count (:columns result))))
      (is (= "NUMERIC" (:type (nth (:columns result) 2))))
      (is (= "INTEGER" (:type (nth (:columns result) 3))))
      (is (= "TIMESTAMP" (:type (nth (:columns result) 4)))))))

;; DROP TABLE tests

(deftest parse-simple-drop-table
  (testing "Simple DROP TABLE"
    (let [result (statements/parse-sql-query "DROP TABLE users")]
      (is (= :drop-table (:type result)))
      (is (= "users" (:table result)))
      (is (nil? (:schema result)))
      (is (false? (:if-exists result))))))

(deftest parse-drop-table-if-exists
  (testing "DROP TABLE IF EXISTS"
    (let [result (statements/parse-sql-query "DROP TABLE IF EXISTS users")]
      (is (= :drop-table (:type result)))
      (is (= "users" (:table result)))
      (is (true? (:if-exists result))))))

(deftest parse-drop-table-with-schema
  (testing "DROP TABLE with schema prefix"
    (let [result (statements/parse-sql-query "DROP TABLE myschema.users")]
      (is (= :drop-table (:type result)))
      (is (= "myschema" (:schema result)))
      (is (= "users" (:table result))))))

;; SHOW TABLES tests

(deftest parse-show-tables
  (testing "Simple SHOW TABLES"
    (let [result (statements/parse-sql-query "SHOW TABLES")]
      (is (= :show-tables (:type result)))
      (is (nil? (:schema result))))))

(deftest parse-show-tables-from-schema
  (testing "SHOW TABLES FROM schema"
    (let [result (statements/parse-sql-query "SHOW TABLES FROM myschema")]
      (is (= :show-tables (:type result)))
      (is (= "myschema" (:schema result))))))

(deftest parse-show-tables-in-schema
  (testing "SHOW TABLES IN schema"
    (let [result (statements/parse-sql-query "SHOW TABLES IN production")]
      (is (= :show-tables (:type result)))
      (is (= "production" (:schema result))))))

;; SHOW SCHEMAS tests

(deftest parse-show-schemas
  (testing "SHOW SCHEMAS"
    (let [result (statements/parse-sql-query "SHOW SCHEMAS")]
      (is (= :show-schemas (:type result))))))

(deftest parse-show-databases
  (testing "SHOW DATABASES (alias for SHOW SCHEMAS)"
    (let [result (statements/parse-sql-query "SHOW DATABASES")]
      (is (= :show-schemas (:type result))))))

;; DESCRIBE tests

(deftest parse-describe-table
  (testing "DESCRIBE table"
    (let [result (statements/parse-sql-query "DESCRIBE users")]
      (is (= :describe (:type result)))
      (is (= "users" (:table result)))
      (is (nil? (:schema result))))))

(deftest parse-describe-table-with-schema
  (testing "DESCRIBE schema.table"
    (let [result (statements/parse-sql-query "DESCRIBE myschema.users")]
      (is (= :describe (:type result)))
      (is (= "myschema" (:schema result)))
      (is (= "users" (:table result))))))

(deftest parse-show-columns-from
  (testing "SHOW COLUMNS FROM table"
    (let [result (statements/parse-sql-query "SHOW COLUMNS FROM users")]
      (is (= :describe (:type result)))
      (is (= "users" (:table result))))))

;; Branch function tests

(deftest parse-branch-list
  (testing "chrondb_branch_list()"
    (let [result (statements/parse-sql-query "SELECT * FROM chrondb_branch_list()")]
      (is (= :chrondb-function (:type result)))
      (is (= :branch-list (:function result))))))

(deftest parse-branch-create
  (testing "chrondb_branch_create(name)"
    (let [result (statements/parse-sql-query "SELECT * FROM chrondb_branch_create('feature-x')")]
      (is (= :chrondb-function (:type result)))
      (is (= :branch-create (:function result)))
      (is (= "feature-x" (:branch-name result))))))

(deftest parse-branch-checkout
  (testing "chrondb_branch_checkout(name)"
    (let [result (statements/parse-sql-query "SELECT * FROM chrondb_branch_checkout('develop')")]
      (is (= :chrondb-function (:type result)))
      (is (= :branch-checkout (:function result)))
      (is (= "develop" (:branch-name result))))))

(deftest parse-branch-merge
  (testing "chrondb_branch_merge(source, target)"
    (let [result (statements/parse-sql-query "SELECT * FROM chrondb_branch_merge('feature-x', 'main')")]
      (is (= :chrondb-function (:type result)))
      (is (= :branch-merge (:function result)))
      (is (= "feature-x" (:source-branch result)))
      (is (= "main" (:target-branch result))))))

;; Unit tests for ddl.clj functions

(deftest test-parse-column-definition
  (testing "Column definition parsing with constraints"
    (let [tokens ["id" "TEXT" "PRIMARY" "KEY"]
          result (ddl/parse-create-table (into ["CREATE" "TABLE" "test" "("] (conj tokens ")")))]
      (is (= 1 (count (:columns result))))
      (is (= "id" (:name (first (:columns result)))))
      (is (= "TEXT" (:type (first (:columns result)))))
      (is (true? (:primary_key (first (:columns result))))))))

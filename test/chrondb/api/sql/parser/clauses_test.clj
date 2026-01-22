(ns chrondb.api.sql.parser.clauses-test
  (:require [clojure.test :refer [deftest is]]
            [chrondb.api.sql.parser.tokenizer :as tokenizer]
            [chrondb.api.sql.parser.clauses :as clauses]))

(defn parse-where-condition [sql]
  (let [tokens (tokenizer/tokenize-sql sql)
        where-idx (tokenizer/find-token-index tokens "where")]
    (clauses/parse-where-condition tokens where-idx (count tokens))))

(deftest parse-standard-condition
  (is (= [{:type :standard :field "name" :op "=" :value "'Alice'"}]
         (parse-where-condition "SELECT * FROM user WHERE name = 'Alice'"))))

(deftest parse-like-condition
  (is (= [{:type :standard :field "name" :op "like" :value "'%Bob%'"}]
         (parse-where-condition "SELECT * FROM user WHERE name LIKE '%Bob%'"))))

(deftest parse-between-condition
  ;; BETWEEN is now implemented
  (let [result (parse-where-condition "SELECT * FROM user WHERE age BETWEEN 20 AND 30")]
    (is (= 1 (count result)))
    (is (= :between (:type (first result))))
    (is (= "age" (:field (first result))))
    (is (= "20" (:lower (first result))))
    (is (= "30" (:upper (first result))))))

(deftest parse-in-condition
  ;; IN is now implemented
  (let [result (parse-where-condition "SELECT * FROM user WHERE name IN ('Alice','Bob')")]
    (is (= 1 (count result)))
    (is (= :in (:type (first result))))
    (is (= "name" (:field (first result))))
    (is (= 2 (count (:values (first result)))))))

(deftest parse-fts-match
  (is (= [{:type :fts-match :field "content" :query "'hello'"}]
         (parse-where-condition "SELECT * FROM doc WHERE fts_match(content, 'hello')"))))

(deftest parse-to-tsquery
  (let [result (parse-where-condition "SELECT * FROM doc WHERE content @@ to_tsquery('foo')")]
    (is (= 1 (count result)))
    (is (= :fts-match (:type (first result))))
    (is (= "content" (:field (first result))))))

(deftest parse-multiple-conditions-implicit-and
  (let [result (parse-where-condition "SELECT * FROM user WHERE name = 'Bob' AND age > 30")]
    (is (= 2 (count result)))
    (is (= :standard (:type (first result))))
    (is (= "name" (:field (first result))))
    (is (= :standard (:type (second result))))
    (is (= "age" (:field (second result))))))

(deftest parse-null-equals
  ;; Note: "field = NULL" is parsed as standard comparison (not same as IS NULL)
  ;; This is technically valid parsing - semantically incorrect SQL should use IS NULL
  (let [result (parse-where-condition "SELECT * FROM user WHERE deleted_at = NULL")]
    (is (= 1 (count result)))
    (is (= :standard (:type (first result))))
    (is (= "deleted_at" (:field (first result))))
    (is (= "=" (:op (first result))))
    (is (= "NULL" (:value (first result))))))

(deftest parse-is-null-condition
  ;; IS NULL is now properly implemented
  (let [result (parse-where-condition "SELECT * FROM user WHERE deleted_at IS NULL")]
    (is (= 1 (count result)))
    (is (= :is-null (:type (first result))))
    (is (= "deleted_at" (:field (first result))))))

(deftest parse-is-not-null-condition
  ;; IS NOT NULL is now properly implemented
  (let [result (parse-where-condition "SELECT * FROM user WHERE email IS NOT NULL")]
    (is (= 1 (count result)))
    (is (= :is-not-null (:type (first result))))
    (is (= "email" (:field (first result))))))

(deftest parse-not-in-condition
  ;; NOT IN is now implemented
  (let [result (parse-where-condition "SELECT * FROM user WHERE role NOT IN ('admin', 'superuser')")]
    (is (= 1 (count result)))
    (is (= :not-in (:type (first result))))
    (is (= "role" (:field (first result))))
    (is (= 2 (count (:values (first result)))))))

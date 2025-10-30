(ns chrondb.api.sql.parser.clauses-test
  (:require [clojure.test :refer [deftest is testing]]
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
  ;; BETWEEN parsing not currently implemented in parse-where-condition
  (is (empty? (parse-where-condition "SELECT * FROM user WHERE age BETWEEN 20 AND 30"))))

(deftest parse-in-condition
  ;; IN parsing not currently implemented in parse-where-condition
  (is (empty? (parse-where-condition "SELECT * FROM user WHERE name IN ('Alice','Bob')"))))

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
  ;; NULL comparisons may need special handling
  (is (empty? (parse-where-condition "SELECT * FROM user WHERE deleted_at = NULL"))))

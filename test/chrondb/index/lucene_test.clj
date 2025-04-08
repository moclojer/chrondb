(ns chrondb.index.lucene-test
  (:require [clojure.test :refer [deftest testing is use-fixtures]]
            [chrondb.index.lucene :as lucene]
            [chrondb.index.protocol :as index]
            [chrondb.test-helpers :as helpers]
            [clojure.java.io :as io]))

(def ^:dynamic *test-index* nil)

(defn with-test-index [test-fn]
  (fn []
    (let [index-dir (helpers/create-temp-dir)
          index (lucene/create-lucene-index index-dir)]
      (try
        (binding [*test-index* index]
          (test-fn))
        (finally
          (.close index)
          (io/delete-file index-dir true))))))

(use-fixtures :each with-test-index)

(deftest test-index-basic-operations
  (testing "Index and search document"
    (let [doc {:id "user:1" :name "John Doe" :age 30 :email "john@example.com"}]
      (index/index-document *test-index* doc)
      (let [results (index/search *test-index* "name" "John" "main")]
        (is (= 1 (count results)))
        (is (= doc (first results))))))

  (testing "Remove document"
    (let [doc {:id "user:1" :name "John Doe" :age 30 :email "john@example.com"}]
      (index/index-document *test-index* doc)
      (let [before-remove (index/search *test-index* "name" "John" "main")]
        (is (= 1 (count before-remove)))
        (index/delete-document *test-index* (:id doc))
        (let [after-remove (index/search *test-index* "name" "John" "main")]
          (is (empty? after-remove)))))))

(deftest test-index-edge-cases
  (testing "Update indexed document"
    (let [doc1 {:id "user:1" :name "Update Test" :age 30}
          doc2 {:id "user:1" :name "Update Test" :age 31}]
      (index/index-document *test-index* doc1)
      (index/index-document *test-index* doc2)
      (let [results (index/search *test-index* "name" "Update" "main")]
        (is (= 1 (count results)))
        (is (= doc2 (first results))))))

  (testing "Index with nil values"
    (let [doc {:id "user:1" :name nil :age 30}]
      (index/index-document *test-index* doc)
      (let [results (index/search *test-index* "age" "30" "main")]
        (is (= 1 (count results)))
        (is (= doc (first results))))))

  (testing "Search with empty query"
    (let [doc {:id "user:1" :name "John Doe" :age 30}]
      (index/index-document *test-index* doc)
      (let [results (index/search *test-index* "name" "" "main")]
        (is (empty? results)))))

  (testing "Search with non-existent term"
    (let [doc {:id "user:1" :name "John Doe" :age 30}]
      (index/index-document *test-index* doc)
      (let [results (index/search *test-index* "name" "NonExistent" "main")]
        (is (empty? results))))))
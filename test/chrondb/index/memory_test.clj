(ns chrondb.index.memory-test
  (:require [clojure.test :refer [deftest testing is use-fixtures]]
            [chrondb.index.memory :as memory]
            [chrondb.index.protocol :as index]))

(def ^:dynamic *test-index* nil)

(defn with-memory-index [test-fn]
  (binding [*test-index* (memory/create-memory-index)]
    (test-fn)))

(use-fixtures :each with-memory-index)

(deftest test-index-document
  (testing "Index a document"
    (let [doc {:id "user:1" :name "John Doe" :age 30 :email "john@example.com"}
          result (index/index-document *test-index* doc)]
      (is (= doc result))
      (is (= doc (get (.data *test-index*) "user:1"))))))

(deftest test-delete-document
  (testing "Delete a document"
    (let [doc {:id "user:1" :name "John Doe" :age 30}]
      ;; First add document
      (index/index-document *test-index* doc)
      (is (= doc (get (.data *test-index*) "user:1")))

      ;; Then delete it
      (index/delete-document *test-index* "user:1")
      (is (nil? (get (.data *test-index*) "user:1"))))))

(deftest test-search-by-field
  (testing "Search documents by field"
    (let [doc1 {:id "user:1" :name "John Doe" :age 30}
          doc2 {:id "user:2" :name "Jane Smith" :age 25}
          doc3 {:id "user:3" :name "John Smith" :age 40}]

      ;; Add documents
      (index/index-document *test-index* doc1)
      (index/index-document *test-index* doc2)
      (index/index-document *test-index* doc3)

      ;; Search for "John" in name field
      (let [results-map (index/search-query *test-index* {:field "name" :value "John"} "main" {:limit 10})
            results (:ids results-map)]
        (is (= 2 (count results)))
        (is (contains? (set results) "user:1"))
        (is (contains? (set results) "user:3"))))))

(deftest test-search-with-content-field
  (testing "Search documents using content field"
    (let [doc1 {:id "user:1" :name "John" :description "A software developer"}
          doc2 {:id "user:2" :name "Jane" :description "A data scientist"}]

      ;; Add documents
      (index/index-document *test-index* doc1)
      (index/index-document *test-index* doc2)

      ;; Search for "developer" in content field
      (let [results-map (index/search-query *test-index* {:field "content" :value "developer"} "main" {:limit 10})
            results (:ids results-map)]
        (is (= 1 (count results)))
        (is (= "user:1" (first results)))))))

(deftest test-search-case-insensitivity
  (testing "Search is case insensitive"
    (let [doc {:id "user:1" :name "John Doe" :age 30}]

      ;; Add document
      (index/index-document *test-index* doc)

      ;; Search with different cases
      (let [results-map1 (index/search-query *test-index* {:field "name" :value "john"} "main" {:limit 10})
            results-map2 (index/search-query *test-index* {:field "name" :value "JOHN"} "main" {:limit 10})
            results-map3 (index/search-query *test-index* {:field "name" :value "John"} "main" {:limit 10})
            results1 (:ids results-map1)
            results2 (:ids results-map2)
            results3 (:ids results-map3)]
        (is (= 1 (count results1)))
        (is (= 1 (count results2)))
        (is (= 1 (count results3)))
        (is (= "user:1" (first results1)))
        (is (= "user:1" (first results2)))
        (is (= "user:1" (first results3)))))))
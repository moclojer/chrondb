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
      (let [results (index/search-query *test-index* {:field "name" :value "John"} "main" {:limit 10})]
        (is (= 1 (count results)))
        (is (= "user:1" (first results))))))

  (testing "Remove document"
    (let [doc {:id "user:1" :name "John Doe" :age 30 :email "john@example.com"}]
      (index/index-document *test-index* doc)
      (let [before-remove (index/search-query *test-index* {:field "name" :value "John"} "main" {:limit 10})]
        (is (= 1 (count before-remove)))
        (index/delete-document *test-index* (:id doc))
        (let [after-remove (index/search-query *test-index* {:field "name" :value "John"} "main" {:limit 10})]
          (is (empty? after-remove)))))))

(deftest test-index-edge-cases
  (testing "Update indexed document"
    (let [doc1 {:id "user:1" :name "Update Test" :age 30}
          doc2 {:id "user:1" :name "Update Test" :age 31}]
      (index/index-document *test-index* doc1)
      (index/index-document *test-index* doc2)
      (let [results (index/search-query *test-index* {:field "name" :value "Update"} "main" {:limit 10})]
        (is (= 1 (count results)))
        (is (= "user:1" (first results))))))

  (testing "Index with nil values"
    (let [doc {:id "user:1" :name nil :age 30}]
      (index/index-document *test-index* doc)
      (let [results (index/search-query *test-index* {:field "age" :value "30"} "main" {:limit 10})]
        (is (= 1 (count results)))
        (is (= "user:1" (first results))))))

  (testing "Search with empty query"
    (let [doc {:id "user:1" :name "John Doe" :age 30}]
      (index/index-document *test-index* doc)
      (let [results (index/search-query *test-index* {:field "name" :value ""} "main" {:limit 10})]
        (is (empty? results)))))

  (testing "Search with non-existent term"
    (let [doc {:id "user:1" :name "John Doe" :age 30}]
      (index/index-document *test-index* doc)
      (let [results (index/search-query *test-index* {:field "name" :value "NonExistent"} "main" {:limit 10})]
        (is (empty? results))))))

(deftest test-full-text-search-capabilities
  (testing "Search with accented characters"
    (let [doc {:id "user:1" :name "José García" :city "São Paulo"}]
      (index/index-document *test-index* doc)
      ;; Should find with or without accents
      (let [results1 (index/search-query *test-index* {:field "name" :value "José"} "main" {:limit 10})
            results2 (index/search-query *test-index* {:field "name" :value "Jose"} "main" {:limit 10})
            results3 (index/search-query *test-index* {:field "city" :value "Sao"} "main" {:limit 10})]
        (is (= 1 (count results1)))
        (is (= 1 (count results2)))
        (is (= 1 (count results3)))
        (is (= "user:1" (first results1)))
        (is (= "user:1" (first results2)))
        (is (= "user:1" (first results3))))))

  (testing "Search with wildcard queries"
    (let [doc1 {:id "user:1" :name "John Smith" :title "Developer"}
          doc2 {:id "user:2" :name "Jane Smith" :title "Designer"}
          doc3 {:id "user:3" :name "Robert Johnson" :title "Developer Advocate"}]
      (index/index-document *test-index* doc1)
      (index/index-document *test-index* doc2)
      (index/index-document *test-index* doc3)

      ;; Test prefix search (automatically adds * to end)
      (let [results1 (index/search-query *test-index* {:field "name" :value "Jo"} "main" {:limit 10})]
        (is (= 2 (count results1)))
        (is (contains? (set results1) "user:1"))
        (is (contains? (set results1) "user:3")))

      ;; Test infix search (with wildcards)
      (let [results2 (index/search-query *test-index* {:field "title" :value "Dev*"} "main" {:limit 10})]
        (is (= 2 (count results2)))
        (is (contains? (set results2) "user:1"))
        (is (contains? (set results2) "user:3"))))))

(deftest test-search-with-custom-fields
  (testing "Search with FTS-optimized fields"
    (let [doc {:id "user:1"
               :name "John Smith"
               :description "Senior Software Engineer with 10+ years experience"
               :description_fts "custom searchable text"}]
      (index/index-document *test-index* doc)

      ;; Standard field search
      (let [results1 (index/search-query *test-index* {:field "description" :value "Software Engineer"} "main" {:limit 10})]
        (is (= 1 (count results1)))
        (is (= "user:1" (first results1))))

      ;; Dedicated FTS field search
      (let [results2 (index/search-query *test-index* {:field "description_fts" :value "custom" :analyzer :fts} "main" {:limit 10})]
        (is (= 1 (count results2)))
        (is (= "user:1" (first results2)))))))

(deftest test-index-error-conditions
  (testing "Handle exceptions during indexing"
    ;; Close the writer to force an error in the next operation
    (.close (.writer *test-index*))

    ;; Should handle error gracefully when writer is closed
    (let [doc {:id "test:error" :name "Error Test"}
          result (index/index-document *test-index* doc)]
      (is (nil? result))))

  (testing "Handle exceptions during deletion"
    ;; Already closed the writer in previous test
    (let [result (index/delete-document *test-index* "test:error")]
      (is (false? result)))))

(deftest test-create-lucene-index
  (testing "Create index with invalid directory"
    (let [invalid-dir "/nonexistent/path/that/cannot/be/created"
          index (lucene/create-lucene-index invalid-dir)]
      (is (nil? index)))))

(deftest test-search-with-reader-unavailable
  (testing "Search when reader is nil"
    ;; Create a scenario where reader is nil
    (reset! (.reader-atom *test-index*) nil)

    ;; Should handle gracefully
    (let [results (index/search-query *test-index* {:field "name" :value "test"} "main" {:limit 10})]
      (is (empty? results)))))
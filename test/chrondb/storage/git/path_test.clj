(ns chrondb.storage.git.path-test
  (:require [chrondb.config :as config]
            [chrondb.storage.git :as git]
            [chrondb.storage.git.path :as path]
            [chrondb.storage.protocol :as protocol]
            [clojure.java.io :as io]
            [clojure.test :refer [deftest is testing use-fixtures]]))

(def test-repo-path "test-repo")
(def test-clone-path "test-repo-clone")

(def test-config
  {:git {:default-branch "main"
         :committer-name "Test User"
         :committer-email "test@example.com"
         :push-enabled false}
   :logging {:level :info
             :file "test.log"}})

(defn delete-directory [directory]
  (when (.exists directory)
    (doseq [file (reverse (file-seq directory))]
      (.delete file))))

(defn clean-test-repo [f]
  (delete-directory (io/file test-repo-path))
  (delete-directory (io/file test-clone-path))
  (with-redefs [config/load-config (constantly test-config)]
    (f)))

(use-fixtures :each clean-test-repo)

(deftest test-extract-table-and-id
  (testing "Extract table and ID from prefixed ID"
    (is (= ["user" "123"] (path/extract-table-and-id "user:123")))
    (is (= ["product" "abc-123"] (path/extract-table-and-id "product:abc-123")))
    (is (= ["order" "2023-01-01"] (path/extract-table-and-id "order:2023-01-01"))))

  (testing "Extract table and ID from ID without prefix"
    (is (= [nil "123"] (path/extract-table-and-id "123")))
    (is (= [nil "abc-123"] (path/extract-table-and-id "abc-123")))))

(deftest test-encode-path
  (testing "Encode path with special characters"
    (is (= "test_COLON_123" (path/encode-path "test:123")))
    (is (= "test_SLASH_path" (path/encode-path "test/path")))
    (is (= "test_BACKSLASH_path" (path/encode-path "test\\path")))
    (is (= "a_PLUS_b_EQUALS_c" (path/encode-path "a+b=c")))))

(deftest test-decode-path
  (testing "Decode encoded path parts"
    (is (= "test:123" (path/decode-path "test_COLON_123")))
    (is (= "test/path" (path/decode-path "test_SLASH_path")))
    (is (= "test\\path" (path/decode-path "test_BACKSLASH_path")))
    (is (= "a+b=c" (path/decode-path "a_PLUS_b_EQUALS_c")))))

(deftest test-get-file-path
  (testing "Get file path for document ID"
    (is (= "data/user/123.json" (path/get-file-path "data" "123" "user")))
    (is (= "custom/product/abc-123.json" (path/get-file-path "custom" "abc-123" "product"))))

  (testing "Get file path for document ID with special characters"
    (is (= "data/order/2023_SLASH_01_SLASH_01.json"
           (path/get-file-path "data" "2023/01/01" "order")))))

(deftest test-document-path-generation
  (testing "Document path generation in real repository"
    (let [storage (git/create-git-storage test-repo-path)
          docs [{:id "user:1" :_table "user"}
                {:id "product:abc-123" :_table "product"}
                {:id "order:2023-04-15-001" :_table "order"}
                {:id "no-prefix"}]]

      (doseq [doc docs]
        (protocol/save-document storage doc))

      ;; All documents should be retrievable
      (doseq [doc docs]
        (is (= doc (protocol/get-document storage (:id doc)))))

      (protocol/close storage))))
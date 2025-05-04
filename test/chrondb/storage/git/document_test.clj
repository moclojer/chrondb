(ns chrondb.storage.git.document-test
  (:require [chrondb.config :as config]
            [chrondb.storage.git :as git]
            [chrondb.storage.git.document :as document]
            [chrondb.storage.git.commit :as git-commit]
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

(deftest test-save-get-document
  (testing "Save and get document"
    (let [storage (git/create-git-storage test-repo-path)
          doc {:id "test:1" :name "Test Document" :value 42 :_table "test"}]

      ;; Save document and verify return
      (is (= doc (protocol/save-document storage doc)))

      ;; Get document and verify contents
      (is (= doc (protocol/get-document storage "test:1")))

      (protocol/close storage))))

(deftest test-save-document-error-handling
  (testing "Error handling during document save"
    (let [storage (git/create-git-storage test-repo-path)
          doc {:id "test:error" :name "Error Test" :value "fail"}]

      ;; Test nil document
      (is (thrown-with-msg? Exception #"Document cannot be nil"
                            (protocol/save-document storage nil)))

      ;; Test commit error
      (with-redefs [git-commit/commit-virtual (fn [& _] (throw (Exception. "Commit error")))]
        (is (thrown? Exception (protocol/save-document storage doc))))

      (protocol/close storage))))

(deftest test-delete-document
  (testing "Delete document"
    (let [storage (git/create-git-storage test-repo-path)
          doc {:id "test:delete" :name "To Be Deleted" :value 99 :_table "test"}]

      ;; Save document
      (protocol/save-document storage doc)
      (is (= doc (protocol/get-document storage "test:delete")))

      ;; Delete document and verify
      (is (true? (protocol/delete-document storage "test:delete")))
      (is (nil? (protocol/get-document storage "test:delete")))

      ;; Delete non-existent document
      (is (false? (protocol/delete-document storage "non-existent")))

      (protocol/close storage))))

(deftest test-get-document-path
  (testing "Get document path"
    (let [storage (git/create-git-storage test-repo-path)
          doc {:id "user:123" :name "Path Test" :_table "user"}]

      ;; Create document to ensure repository is properly set up
      (protocol/save-document storage doc)

      ;; Test getting document path
      (let [repo (:repository storage)
            path (document/get-document-path repo "user:123" "main")]
        (is (not (nil? path)))
        (is (.contains path "user")))

      (protocol/close storage))))

(deftest test-get-documents-by-prefix
  (testing "Get documents by prefix"
    (let [storage (git/create-git-storage test-repo-path)
          docs [{:id "user:1" :name "Alice" :age 30 :_table "user"}
                {:id "user:2" :name "Bob" :age 25 :_table "user"}
                {:id "product:1" :name "Laptop" :price 1200 :_table "product"}
                {:id "product:2" :name "Phone" :price 800 :_table "product"}]]

      ;; Save all test documents
      (doseq [doc docs]
        (protocol/save-document storage doc))

      (testing "Get documents by prefix"
        (let [user-docs (protocol/get-documents-by-prefix storage "user:")]
          (is (= 2 (count user-docs)))
          (is (= #{"user:1" "user:2"} (set (map :id user-docs))))))

      (testing "Get documents by non-existing prefix"
        (let [docs (protocol/get-documents-by-prefix storage "nonexistent:")]
          (is (empty? docs))))

      (protocol/close storage))))

(deftest test-get-documents-by-table
  (testing "Get documents by table"
    (let [storage (git/create-git-storage test-repo-path)
          docs [{:id "user:1" :name "Alice" :age 30 :_table "user"}
                {:id "user:2" :name "Bob" :age 25 :_table "user"}
                {:id "product:1" :name "Laptop" :price 1200 :_table "product"}
                {:id "product:2" :name "Phone" :price 800 :_table "product"}]]

      ;; Save all test documents
      (doseq [doc docs]
        (protocol/save-document storage doc))

      (testing "Get documents by table name"
        (let [product-docs (protocol/get-documents-by-table storage "product")]
          (is (= 2 (count product-docs)))
          (is (= #{"product:1" "product:2"} (set (map :id product-docs))))))

      (testing "Get documents by non-existing table name"
        (let [docs (protocol/get-documents-by-table storage "nonexistent")]
          (is (empty? docs))))

      (protocol/close storage))))

(deftest test-branch-operations
  (testing "Document operations in different branches"
    (let [storage (git/create-git-storage test-repo-path)
          doc {:id "test:1" :name "Test" :value 42 :_table "test"}
          dev-doc {:id "test:1" :name "Test Dev" :value 99 :_table "test"}]

      ;; Save document on main branch
      (protocol/save-document storage doc)
      (is (= doc (protocol/get-document storage "test:1")))

      ;; Save document on dev branch
      (protocol/save-document storage dev-doc "dev")

      ;; Verify correct document on each branch
      (is (= doc (protocol/get-document storage "test:1" "main")))
      (is (= dev-doc (protocol/get-document storage "test:1" "dev")))

      (protocol/close storage))))
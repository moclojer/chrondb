(ns chrondb.storage.git-test
  (:require [chrondb.config :as config]
            [chrondb.storage.git :as git]
            [chrondb.storage.protocol :as protocol]
            [clojure.java.io :as io]
            [clojure.test :refer [deftest is testing use-fixtures]])
  (:import [java.io File]
           [org.eclipse.jgit.api Git]))

(def test-repo-path "test-repo")
(def test-clone-path "test-repo-clone")

(def test-config
  {:git {:default-branch "main"
         :committer-name "Test User"
         :committer-email "test@example.com"
         :push-enabled false}
   :logging {:level :info
             :file "test.log"}})

(defn delete-directory [^File directory]
  (when (.exists directory)
    (doseq [file (reverse (file-seq directory))]
      (.delete file))))

(defn clean-test-repo [f]
  (delete-directory (io/file test-repo-path))
  (delete-directory (io/file test-clone-path))
  (with-redefs [config/load-config (constantly test-config)]
    (f)))

(use-fixtures :each clean-test-repo)

(defn get-initial-commit-message [git]
  (.getFullMessage (first (iterator-seq (.iterator (.call (.log git)))))))

(defn clone-repo [repo-path]
  (let [clone-dir (io/file test-clone-path)]
    (delete-directory clone-dir)
    (-> (Git/cloneRepository)
        (.setURI (str "file://" repo-path))
        (.setDirectory clone-dir)
        (.call))))

(deftest test-ensure-directory
  (testing "Cannot create directory"
    (let [test-file (io/file test-repo-path "test-file")]
      (io/make-parents test-file)
      (.createNewFile test-file)
      (with-redefs [io/file (fn [path]
                              (proxy [File] [path]
                                (exists [] false)
                                (mkdirs [] false)))]
        (is (thrown-with-msg? Exception #"Could not create directory"
                              (git/ensure-directory "test-file"))))
      (.delete test-file))))

(deftest test-create-repository
  (testing "Create new bare repository"
    (let [storage (git/create-git-storage test-repo-path)
          repo (clone-repo (.getAbsolutePath (io/file test-repo-path)))
          git-api (Git. (.getRepository repo))]
      (is (.exists (io/file test-repo-path)))
      (is (= "Initial empty commit" (get-initial-commit-message git-api)))
      (.close repo)
      (protocol/close storage))))

(deftest test-git-storage
  (testing "Git storage operations"
    (let [storage (git/create-git-storage test-repo-path)
          doc {:id "test:1" :name "Test" :value 42}]
      (testing "Save document"
        (is (= doc (protocol/save-document storage doc))))

      (testing "Get document"
        (is (= doc (protocol/get-document storage "test:1"))))

      (testing "Delete document"
        (is (true? (protocol/delete-document storage "test:1")))
        (is (nil? (protocol/get-document storage "test:1"))))

      (testing "Close storage"
        (is (nil? (protocol/close storage)))))))

(deftest test-git-storage-error-cases
  (testing "Git storage error handling"
    (let [storage (git/create-git-storage test-repo-path)]
      (testing "Save nil document"
        (is (thrown-with-msg? Exception #"Document cannot be nil"
                              (protocol/save-document storage nil))))

      (testing "Delete non-existent document"
        (is (false? (protocol/delete-document storage "non-existent"))))

      (testing "Close already closed storage"
        (protocol/close storage)
        (is (nil? (protocol/close storage)))))))

(deftest test-git-storage-with-custom-data-dir
  (testing "Git storage with custom data directory"
    (let [data-dir "custom-data"
          storage (git/create-git-storage test-repo-path data-dir)
          doc {:id "test:1" :name "Test" :value 42}]
      (testing "Save document in custom directory"
        (is (= doc (protocol/save-document storage doc)))
        (is (= doc (protocol/get-document storage "test:1"))))

      (testing "Delete document from custom directory"
        (is (true? (protocol/delete-document storage "test:1")))
        (is (nil? (protocol/get-document storage "test:1"))))

      (protocol/close storage))))
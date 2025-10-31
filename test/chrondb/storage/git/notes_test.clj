(ns chrondb.storage.git.notes-test
  (:require [chrondb.config :as config]
            [chrondb.storage.git.commit :as commit]
            [chrondb.storage.git.core :as git-core]
            [chrondb.storage.git.notes :as notes]
            [chrondb.storage.protocol :as protocol]
            [clojure.java.io :as io]
            [clojure.test :refer [deftest is testing use-fixtures]])
  (:import [java.io File]
           [org.eclipse.jgit.api Git]))

(def test-repo-path "data/notes-test-repo")

(def test-config
  {:git {:default-branch "main"
         :committer-name "Test User"
         :committer-email "test@example.com"
         :push-enabled false}
   :logging {:level :info
             :file "test.log"}
   :storage {:data-dir "data"}})

(defn- delete-directory [^File directory]
  (when (.exists directory)
    (doseq [file (reverse (file-seq directory))]
      (.delete file))))

(defn clean-test-repo [f]
  (delete-directory (io/file test-repo-path))
  (with-redefs [config/load-config (constantly test-config)]
    (f)))

(use-fixtures :each clean-test-repo)

(defn- create-test-storage []
  (git-core/create-git-storage test-repo-path))

(deftest add-and-read-note
  (testing "writes git note with transaction payload"
    (let [storage (create-test-storage)
          repository (:repository storage)
          git (Git/wrap repository)
          commit-id (commit/commit-virtual git
                                           "main"
                                           "foo.txt"
                                           "content"
                                           "Initial commit"
                                           "Test User"
                                           "test@example.com")
          payload {:tx_id "tx-123"
                   :origin "rest"
                   :timestamp "2025-01-01T00:00:00Z"
                   :flags ["bulk-load"]}]
      (try
        (let [merged (notes/add-git-note git commit-id payload)
              note (notes/read-note git commit-id)]
          (is (= "tx-123" (:tx_id merged)))
          (is (= "rest" (:origin note)))
          (is (= ["bulk-load"] (:flags note))))
        (finally
          (protocol/close storage))))))

(deftest merge-notes
  (testing "merges flags and metadata without duplication"
    (let [storage (create-test-storage)
          repository (:repository storage)
          git (Git/wrap repository)
          commit-id (commit/commit-virtual git
                                           "main"
                                           "bar.txt"
                                           "payload"
                                           "Second commit"
                                           "Test User"
                                           "test@example.com")]
      (try
        (notes/add-git-note git commit-id {:tx_id "tx-456"
                                           :flags ["bulk-load"]})
        (let [merged (notes/add-git-note git commit-id {:flags ["migration" "bulk-load"]
                                                        :metadata {:request "abc"}})
              note (notes/read-note git commit-id)]
          (is (= "tx-456" (:tx_id merged)))
          (is (= #{"bulk-load" "migration"}
                 (set (:flags note))))
          (is (= {:request "abc"} (:metadata note))))
        (finally
          (protocol/close storage))))))


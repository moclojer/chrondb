(ns chrondb.storage.git.commit-test
  (:require [chrondb.config :as config]
            [chrondb.storage.git :as git]
            [chrondb.storage.git.commit :as commit]
            [chrondb.storage.protocol :as protocol]
            [clojure.java.io :as io]
            [clojure.test :refer [deftest is testing use-fixtures]])
  (:import [java.io File]
           [org.eclipse.jgit.api Git]
           [org.eclipse.jgit.revwalk RevWalk]
           [org.eclipse.jgit.lib ObjectId]))

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

(defn get-commit-message [repository commit-id]
  (when (and repository commit-id)
    (let [rev-walk (RevWalk. repository)
          normalized-id (commit/normalize-commit-hash commit-id)
          commit-obj (ObjectId/fromString normalized-id)
          commit (.parseCommit rev-walk commit-obj)]
      (.getFullMessage commit))))

(deftest test-configure-repository
  (testing "Configure Git repository"
    (let [storage (git/create-git-storage test-repo-path)
          repository (:repository storage)]

      ;; Configure repository
      (is (nil? (commit/configure-repository repository)))

      ;; Verify configuration
      (let [git (Git/wrap repository)
            config (.getRepository git)]
        (is (= "Test User" (-> config .getConfig (.getString "user" nil "name"))))
        (is (= "test@example.com" (-> config .getConfig (.getString "user" nil "email")))))

      (protocol/close storage))))

(deftest test-normalize-commit-hash
  (testing "Normalize commit hash"
    (is (= "abcdef1234567890" (commit/normalize-commit-hash "abcdef1234567890")))
    (is (= "abcdef1234567890" (commit/normalize-commit-hash "commit abcdef1234567890 branch")))
    (is (= "abcdef1234567890" (commit/normalize-commit-hash "commit abcdef1234567890")))
    (is (= "abcdef" (commit/normalize-commit-hash "abcdef")))))

(deftest test-commit-virtual
  (testing "Create virtual commit"
    (let [repository (-> (git/create-git-storage test-repo-path) :repository)
          git (Git/wrap repository)
          branch-name "main"
          file-path "test.txt"
          file-content "test content"
          commit-message "Test virtual commit"]

      ;; Create virtual commit
      (let [commit-id (commit/commit-virtual git
                                             branch-name
                                             file-path
                                             file-content
                                             commit-message
                                             "Test User"
                                             "test@example.com")]

        ;; Verify commit was created
        (is (not (nil? commit-id)))
        (is (= commit-message (get-commit-message repository commit-id)))

        ;; Create another commit with deletion
        (let [another-commit-id (commit/commit-virtual git
                                                       branch-name
                                                       file-path
                                                       nil
                                                       "Delete file"
                                                       "Test User"
                                                       "test@example.com")]
          (is (not (nil? another-commit-id)))
          (is (= "Delete file" (get-commit-message repository another-commit-id)))))

      (.close repository))))

(deftest test-checkout-or-create-branch
  (testing "Checkout existing branch"
    (let [storage (git/create-git-storage test-repo-path)
          repository (:repository storage)
          git (Git/wrap repository)
          doc {:id "test:1" :name "Test" :_table "test"}]

      ;; Create document on main branch
      (protocol/save-document storage doc)

      ;; Checkout main branch
      (let [result (commit/checkout-or-create-branch git "main")]
        (is (= "main" (.getName result))))

      (protocol/close storage)))

  (testing "Create and checkout new branch"
    (let [storage (git/create-git-storage test-repo-path)
          repository (:repository storage)
          git (Git/wrap repository)]

      ;; Create and checkout new branch
      (let [result (commit/checkout-or-create-branch git "feature")]
        (is (= "feature" (.getName result))))

      (protocol/close storage))))
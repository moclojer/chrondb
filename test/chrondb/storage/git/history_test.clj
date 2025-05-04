(ns chrondb.storage.git.history-test
  (:require [chrondb.config :as config]
            [chrondb.storage.git :as git]
            [chrondb.storage.git.history :as history]
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

(deftest test-find-all-document-paths
  (testing "Find document paths"
    (let [storage (git/create-git-storage test-repo-path)
          repository (:repository storage)
          doc {:id "history:test" :name "Test" :_table "history"}]

      ;; Save document to create path
      (protocol/save-document storage doc)

      ;; Find paths
      (let [paths (history/find-all-document-paths repository "history:test" "main")]
        (is (not (empty? paths)))
        (is (some #(.contains % "history") paths)))

      (protocol/close storage))))

(deftest test-document-history
  (testing "Document history retrieval"
    (let [storage (git/create-git-storage test-repo-path)
          doc-v1 {:id "history-test:1" :name "Original" :value 1 :_table "history"}
          doc-v2 {:id "history-test:1" :name "Updated" :value 2 :_table "history"}
          doc-v3 {:id "history-test:1" :name "Final" :value 3 :_table "history"}]

      ;; Create document with multiple versions
      (println "Saving first version of document")
      (protocol/save-document storage doc-v1)
      ;; Add a small delay between commits to ensure they are properly ordered
      (Thread/sleep 200)
      (println "Saving second version of document")
      (protocol/save-document storage doc-v2)
      (Thread/sleep 200)
      (println "Saving third version of document")
      (protocol/save-document storage doc-v3)

      ;; Make sure changes are flushed before getting history
      (Thread/sleep 200)

      ;; Get document history
      (let [history-entries (git/get-document-history storage "history-test:1")]
        (println "Document history entries:" (count history-entries))

        ;; Debug: print the actual history entries with type info
        (doseq [entry history-entries]
          (println "Entry type:" (type entry))
          (println "Entry keys:" (keys entry))
          (println "Entry document type:" (type (:document entry)))
          (println "Entry:"
                   "commit-id:" (:commit-id entry)
                   "time:" (:commit-time entry)
                   "document:" (:document entry)))

        (is (= 3 (count history-entries)) "Should have 3 versions in history")

        (when (>= (count history-entries) 3)
          ;; Check that most recent version comes first
          (is (= 3 (get-in history-entries [0 :document :value])) "Most recent version should be first")
          (is (= 2 (get-in history-entries [1 :document :value])) "Second version should be second")
          (is (= 1 (get-in history-entries [2 :document :value])) "Original version should be last")

          ;; Verify commit metadata exists
          (is (string? (get-in history-entries [0 :commit-id])) "Should have commit ID")
          (is (instance? java.util.Date (get-in history-entries [0 :commit-time])) "Should have commit time")
          (is (string? (get-in history-entries [0 :committer-name])) "Should have committer name")
          (is (string? (get-in history-entries [0 :commit-message])) "Should have commit message")))

      (protocol/close storage))))

(deftest test-get-document-at-commit
  (testing "Get document at specific commit"
    (let [storage (git/create-git-storage test-repo-path)
          doc-v1 {:id "commit-test:1" :name "Original" :value 1 :_table "commit"}
          doc-v2 {:id "commit-test:1" :name "Updated" :value 2 :_table "commit"}]

      ;; Create document with multiple versions
      (protocol/save-document storage doc-v1)
      ;; Add a small delay between commits to ensure they are properly ordered
      (Thread/sleep 100)
      (protocol/save-document storage doc-v2)

      ;; Make sure changes are flushed before getting history
      (Thread/sleep 100)

      ;; Get document history to find commit hash
      (let [history-entries (protocol/get-document-history storage "commit-test:1")
            _ (println "History entries:" (count history-entries))]

        (when (>= (count history-entries) 2)
          (let [first-commit-hash (get-in history-entries [1 :commit-id]) ;; Original version commit
                repository (:repository storage)]

            ;; Print commit hash for debugging
            (println "Commit hash:" first-commit-hash)

            ;; Get document at specific commit and verify
            (when first-commit-hash
              (let [doc-at-commit (history/get-document-at-commit repository "commit-test:1" first-commit-hash)]
                (is (= 1 (:value doc-at-commit)) "Should retrieve original version")
                (is (= "Original" (:name doc-at-commit)) "Should have original name"))))))

      (protocol/close storage))))

(deftest test-restore-document-version
  (testing "Restore document to previous version"
    (let [storage (git/create-git-storage test-repo-path)
          doc-v1 {:id "restore-test:1" :name "Original" :value 1 :_table "restore"}
          doc-v2 {:id "restore-test:1" :name "Updated" :value 2 :_table "restore"}]

      ;; Create document with multiple versions
      (protocol/save-document storage doc-v1)
      ;; Add a small delay between commits to ensure they are properly ordered
      (Thread/sleep 100)
      (protocol/save-document storage doc-v2)

      ;; Make sure changes are flushed before getting history
      (Thread/sleep 100)

      ;; Current version should be v2
      (is (= 2 (:value (protocol/get-document storage "restore-test:1"))))

      ;; Get document history to find commit hash
      (let [history-entries (protocol/get-document-history storage "restore-test:1")
            _ (println "Restore history entries:" (count history-entries))]

        (when (>= (count history-entries) 2)
          (let [original-commit-hash (get-in history-entries [1 :commit-id])] ;; Original version commit
            (println "Original commit hash for restore:" original-commit-hash)

            ;; Restore document to original version and check value (if we have a valid hash)
            (when original-commit-hash
              (let [restored (history/restore-document-version storage "restore-test:1" original-commit-hash)]
                (is (= 1 (:value restored)) "Restored document should have original value")

                ;; Verify current document is restored version
                (is (= 1 (:value (protocol/get-document storage "restore-test:1"))) "Current document should have original value")

                ;; Check history again - should have 3 versions now (original, update, restore)
                (let [new-history (protocol/get-document-history storage "restore-test:1")]
                  (is (= 3 (count new-history)) "Should have 3 versions in history now")
                  (when (>= (count new-history) 1)
                    (is (= 1 (get-in new-history [0 :document :value])) "Most recent should be restored version")
                    (when (and (>= (count new-history) 1) (get-in new-history [0 :commit-message]))
                      (is (.contains (get-in new-history [0 :commit-message]) "Restore")
                          "Commit message should indicate restoration")))))))))

      (protocol/close storage))))

(deftest test-restore-document-version-errors
  (testing "Error handling when restoring document versions"
    (let [storage (git/create-git-storage test-repo-path)
          doc {:id "error-test:1" :name "Test" :value 1 :_table "error"}]

      ;; Create document
      (protocol/save-document storage doc)

      ;; Try to restore with invalid commit hash
      (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Document version not found"
                            (history/restore-document-version storage "error-test:1" "invalid-hash")))

      ;; Try to restore non-existent document
      (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Document version not found"
                            (history/restore-document-version storage "non-existent:1" "any-hash")))

      (protocol/close storage))))

(deftest test-document-version-rollback-history
  (testing "Document rollback maintains complete history"
    (let [storage (git/create-git-storage test-repo-path)
          doc-v1 {:id "abc" :value 123 :_table "kv"}
          doc-v2 {:id "abc" :value 1234 :_table "kv"}]

      ;; 1. Create key "abc" with value 123
      (protocol/save-document storage doc-v1)
      (Thread/sleep 100)
      (is (= 123 (:value (protocol/get-document storage "abc"))))

      ;; 2. Edit key "abc" with value 1234
      (protocol/save-document storage doc-v2)
      (Thread/sleep 100)
      (is (= 1234 (:value (protocol/get-document storage "abc"))))

      ;; Verify we have 2 commits so far
      (let [history-before (protocol/get-document-history storage "abc")]
        (is (= 2 (count history-before)) "Should have 2 versions before rollback")

        (when (>= (count history-before) 2)
          ;; Get the initial commit hash
          (let [initial-commit-hash (get-in history-before [1 :commit-id])]
            (println "Initial commit hash:" initial-commit-hash)

            (when initial-commit-hash
              ;; 3. Rollback to the original version (value 123) and test
              (let [restored (history/restore-document-version storage "abc" initial-commit-hash)]
                (is (= 123 (:value restored)) "Should restore original value")

                ;; Verify the current value after rollback
                (is (= 123 (:value (protocol/get-document storage "abc"))) "Current value should be the original one")

                ;; 4. Verify the history contains 3 commits
                (let [history-after (protocol/get-document-history storage "abc")]
                  (is (= 3 (count history-after)) "Should have 3 versions in history")

                  (when (>= (count history-after) 3)
                    ;; Verify the chronology of commits (most recent first)
                    (is (= 123 (get-in history-after [0 :document :value])) "Most recent commit should be the rollback (123)")
                    (is (= 1234 (get-in history-after [1 :document :value])) "Second commit should be the edit (1234)")
                    (is (= 123 (get-in history-after [2 :document :value])) "Third commit should be the original (123)")

                    ;; Verify the commit messages
                    (when (and (get-in history-after [0 :commit-message])
                               (get-in history-after [1 :commit-message])
                               (get-in history-after [2 :commit-message]))
                      (is (.contains (get-in history-after [0 :commit-message]) "Restore")
                          "Commit message should indicate restoration")
                      (is (.contains (get-in history-after [1 :commit-message]) "Save")
                          "Second commit should be a save operation")
                      (is (.contains (get-in history-after [2 :commit-message]) "Save")
                          "Initial commit should be a save operation")))))))))

      (protocol/close storage))))
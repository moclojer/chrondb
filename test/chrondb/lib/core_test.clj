(ns chrondb.lib.core-test
  (:require [clojure.test :refer [deftest testing is use-fixtures]]
            [chrondb.lib.core :as lib]
            [clojure.java.io :as io])
  (:import [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]))

(def ^:dynamic *test-data-dir* nil)
(def ^:dynamic *test-index-dir* nil)

(defn- create-temp-dir
  "Creates a temporary directory for testing."
  []
  (str (Files/createTempDirectory "chrondb-lib-test" (make-array FileAttribute 0))))

(defn- delete-directory
  "Recursively deletes a directory."
  [path]
  (when (.exists (io/file path))
    (doseq [f (reverse (file-seq (io/file path)))]
      (try
        (io/delete-file f true)
        (catch Exception _)))))

(defn temp-dirs-fixture [f]
  (let [data-dir (create-temp-dir)
        index-dir (create-temp-dir)]
    (binding [*test-data-dir* data-dir
              *test-index-dir* index-dir]
      (try
        (f)
        (finally
          (delete-directory data-dir)
          (delete-directory index-dir))))))

(use-fixtures :each temp-dirs-fixture)

(deftest test-lib-open-close
  (testing "lib-open should return valid handle and lib-close should close it"
    (let [handle (lib/lib-open *test-data-dir* *test-index-dir*)]
      (is (some? handle) "lib-open should return a handle")
      (is (>= handle 0) "handle should be >= 0")
      (let [close-result (lib/lib-close handle)]
        (is (= 0 close-result) "lib-close should return 0 on success")))))

(deftest test-lib-put-operation
  (testing "lib-put should save document without StackOverflowError"
    (let [handle (lib/lib-open *test-data-dir* *test-index-dir*)]
      (is (some? handle) "lib-open should return valid handle")
      (is (>= handle 0) "handle should be >= 0")
      (try
        (let [doc-id "user:1"
              doc-json "{\"name\": \"Test User\", \"email\": \"test@example.com\"}"
              result (lib/lib-put handle doc-id doc-json nil)]
          (is (some? result) "lib-put should return saved document")
          (is (string? result) "result should be JSON string")
          (is (.contains result "Test User") "result should contain saved data")
          (is (.contains result "user:1") "result should contain the id")

          ;; Verify document can be retrieved
          (let [retrieved (lib/lib-get handle doc-id nil)]
            (is (some? retrieved) "lib-get should return document")
            (is (.contains retrieved "Test User") "document should contain saved data")))
        (finally
          (lib/lib-close handle))))))

(deftest test-lib-put-multiple-documents
  (testing "lib-put should work with multiple documents"
    (let [handle (lib/lib-open *test-data-dir* *test-index-dir*)]
      (is (>= handle 0) "handle should be valid")
      (try
        (doseq [i (range 10)]
          (let [doc-id (str "item:" i)
                doc-json (format "{\"index\": %d, \"value\": \"test-%d\"}" i i)
                result (lib/lib-put handle doc-id doc-json nil)]
            (is (some? result) (str "lib-put should work for doc " i))))

        ;; Verify all documents were saved
        (doseq [i (range 10)]
          (let [doc-id (str "item:" i)
                retrieved (lib/lib-get handle doc-id nil)]
            (is (some? retrieved) (str "lib-get should return doc " i))
            (is (.contains retrieved (str "\"index\":" i)) (str "doc " i " should contain correct data"))))
        (finally
          (lib/lib-close handle))))))

(deftest test-lib-put-and-delete
  (testing "lib-put followed by lib-delete should work"
    (let [handle (lib/lib-open *test-data-dir* *test-index-dir*)]
      (try
        (let [doc-id "temp:1"
              doc-json "{\"temp\": true}"
              _ (lib/lib-put handle doc-id doc-json nil)
              delete-result (lib/lib-delete handle doc-id nil)]
          (is (= 0 delete-result) "delete should return 0")

          ;; Verify document was deleted
          (let [retrieved (lib/lib-get handle doc-id nil)]
            (is (nil? retrieved) "deleted document should not be found")))
        (finally
          (lib/lib-close handle))))))

(deftest test-lib-list-by-prefix
  (testing "lib-list-by-prefix should return documents with prefix"
    (let [handle (lib/lib-open *test-data-dir* *test-index-dir*)]
      (try
        ;; Create some documents
        (lib/lib-put handle "product:1" "{\"name\": \"Product 1\"}" nil)
        (lib/lib-put handle "product:2" "{\"name\": \"Product 2\"}" nil)
        (lib/lib-put handle "category:1" "{\"name\": \"Category 1\"}" nil)

        ;; List by prefix
        (let [result (lib/lib-list-by-prefix handle "product:" nil)]
          (is (some? result) "result should exist")
          (is (.contains result "Product 1") "should contain Product 1")
          (is (.contains result "Product 2") "should contain Product 2")
          (is (not (.contains result "Category 1")) "should not contain Category 1"))
        (finally
          (lib/lib-close handle))))))

(deftest test-lib-history
  (testing "lib-history should return modification history"
    (let [handle (lib/lib-open *test-data-dir* *test-index-dir*)]
      (try
        ;; Create and update document
        (lib/lib-put handle "doc:1" "{\"version\": 1}" nil)
        (lib/lib-put handle "doc:1" "{\"version\": 2}" nil)

        ;; Verify history
        (let [history (lib/lib-history handle "doc:1" nil)]
          (is (some? history) "history should exist")
          (is (string? history) "history should be JSON string"))
        (finally
          (lib/lib-close handle))))))

(deftest test-lib-open-returns-number-on-failure
  (testing "lib-open should return -1 (not nil) on failure"
    ;; Use invalid path that causes failure
    (let [result (lib/lib-open nil nil)]
      (is (number? result) "result should be number, not nil")
      (is (= -1 result) "result should be -1 on error"))))

(deftest test-lib-open-invalid-paths
  (testing "lib-open with empty paths should return -1"
    (let [result (lib/lib-open "" "")]
      (is (number? result) "result should be number")
      (is (= -1 result) "empty paths should return -1"))))

(deftest test-lib-reopen-preserves-data
  (testing "Data should persist when reopening database"
    ;; First session - create and save
    (let [handle1 (lib/lib-open *test-data-dir* *test-index-dir*)]
      (is (>= handle1 0) "First open should succeed")
      (let [result (lib/lib-put handle1 "persist:1" "{\"key\": \"value\", \"num\": 42}" nil)]
        (is (some? result) "Put should succeed"))
      (lib/lib-close handle1))

    ;; Second session - reopen and verify
    (let [handle2 (lib/lib-open *test-data-dir* *test-index-dir*)]
      (is (>= handle2 0) "Second open should succeed (not recreate!)")
      (let [doc (lib/lib-get handle2 "persist:1" nil)]
        (is (some? doc) "Document should persist after reopen")
        (is (.contains doc "value") "Document content should be intact")
        (is (.contains doc "42") "Document values should be intact"))
      (lib/lib-close handle2))

    ;; Third session - verify data still persists
    (let [handle3 (lib/lib-open *test-data-dir* *test-index-dir*)]
      (is (>= handle3 0) "Third open should succeed")
      (let [doc (lib/lib-get handle3 "persist:1" nil)]
        (is (some? doc) "Document should still persist after multiple reopens"))
      (lib/lib-close handle3))))

(deftest test-lib-open-cleans-stale-locks
  (testing "lib-open should clean orphan .lock files and open normally"
    ;; First, create a valid database
    (let [handle (lib/lib-open *test-data-dir* *test-index-dir*)]
      (is (>= handle 0) "first open should succeed")
      ;; Save a document to ensure repo is functional
      (lib/lib-put handle "test:1" "{\"test\": true}" nil)
      (lib/lib-close handle))

    ;; Simulate crash by creating orphan lock files
    (let [stale-lock (io/file *test-data-dir* "refs" "heads" "main.lock")
          lucene-lock (io/file *test-index-dir* "write.lock")]
      ;; Create orphan locks
      (.mkdirs (.getParentFile stale-lock))
      (spit stale-lock "stale lock content")
      (spit lucene-lock "stale lucene lock")
      ;; Set modification time to more than 60 seconds ago
      (.setLastModified stale-lock (- (System/currentTimeMillis) 120000))
      (.setLastModified lucene-lock (- (System/currentTimeMillis) 120000))

      (is (.exists stale-lock) "git lock file should exist before reopen")
      (is (.exists lucene-lock) "lucene lock file should exist before reopen")

      ;; Reopen should clean orphan locks and succeed
      ;; Without cleanup, this would fail with OpenFailed("")
      (let [handle (lib/lib-open *test-data-dir* *test-index-dir*)]
        (is (>= handle 0) "lib-open should succeed after cleaning orphan locks")
        (when (>= handle 0)
          ;; Verify data is still accessible
          (let [doc (lib/lib-get handle "test:1" nil)]
            (is (some? doc) "document should be accessible after reopen"))
          ;; Git lock should have been removed (not recreated when opening bare repo)
          (is (not (.exists stale-lock)) "orphan git lock should have been removed")
          (lib/lib-close handle))))))

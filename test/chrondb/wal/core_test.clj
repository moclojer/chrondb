(ns chrondb.wal.core-test
  (:require [clojure.test :refer [deftest testing is use-fixtures]]
            [chrondb.wal.core :as wal]
            [clojure.java.io :as io]))

(def ^:dynamic *test-wal-dir* nil)

(defn with-temp-wal-dir [f]
  (let [dir (str "target/test-wal-" (System/currentTimeMillis))]
    (binding [*test-wal-dir* dir]
      (try
        (f)
        (finally
          ;; Cleanup
          (let [d (io/file dir)]
            (when (.exists d)
              (doseq [file (.listFiles d)]
                (.delete file))
              (.delete d))))))))

(use-fixtures :each with-temp-wal-dir)

(deftest test-create-file-wal
  (testing "Creating a FileWAL"
    (let [wal-instance (wal/create-file-wal *test-wal-dir*)]
      (is (some? wal-instance))
      (is (instance? chrondb.wal.core.FileWAL wal-instance)))))

(deftest test-append-and-get-entry
  (testing "Appending and retrieving WAL entries"
    (let [wal-instance (wal/create-file-wal *test-wal-dir*)
          entry (wal/create-wal-entry {:operation :save
                                       :document-id "test:1"
                                       :branch "main"
                                       :content {:id "test:1" :name "Test"}})
          appended (wal/append! wal-instance entry)
          retrieved (wal/get-entry wal-instance (:id entry))]
      (is (= (:id entry) (:id appended)))
      (is (= :pending (:state appended)))
      (is (some? retrieved))
      (is (= (:document-id entry) (:document-id retrieved))))))

(deftest test-mark-state
  (testing "Marking WAL entry states"
    (let [wal-instance (wal/create-file-wal *test-wal-dir*)
          entry (wal/wal-save-document! wal-instance {:id "test:1" :name "Test"} "main")]

      (is (= :pending (:state (wal/get-entry wal-instance (:id entry)))))

      (wal/wal-commit-git! wal-instance (:id entry))
      (is (= :git-committed (:state (wal/get-entry wal-instance (:id entry)))))

      (wal/wal-commit-index! wal-instance (:id entry))
      (is (= :index-committed (:state (wal/get-entry wal-instance (:id entry)))))

      (wal/wal-complete! wal-instance (:id entry))
      (is (= :completed (:state (wal/get-entry wal-instance (:id entry))))))))

(deftest test-pending-entries
  (testing "Getting pending entries"
    (let [wal-instance (wal/create-file-wal *test-wal-dir*)
          entry1 (wal/wal-save-document! wal-instance {:id "test:1"} "main")
          entry2 (wal/wal-save-document! wal-instance {:id "test:2"} "main")
          _entry3 (wal/wal-save-document! wal-instance {:id "test:3"} "main")]

      ;; All three should be pending
      (is (= 3 (count (wal/pending-entries wal-instance))))

      ;; Complete one
      (wal/wal-complete! wal-instance (:id entry1))
      (is (= 2 (count (wal/pending-entries wal-instance))))

      ;; Rollback one
      (wal/wal-rollback! wal-instance (:id entry2))
      (is (= 1 (count (wal/pending-entries wal-instance)))))))

(deftest test-truncate
  (testing "Truncating completed entries"
    (let [wal-instance (wal/create-file-wal *test-wal-dir*)
          entry1 (wal/wal-save-document! wal-instance {:id "test:1"} "main")
          entry2 (wal/wal-save-document! wal-instance {:id "test:2"} "main")]

      (wal/wal-complete! wal-instance (:id entry1))

      (let [truncated (wal/truncate! wal-instance)]
        (is (= 1 truncated))
        ;; entry1 file should be deleted
        (is (nil? (wal/get-entry wal-instance (:id entry1))))
        ;; entry2 should still exist
        (is (some? (wal/get-entry wal-instance (:id entry2))))))))

(deftest test-wal-delete-document
  (testing "WAL delete document entry"
    (let [wal-instance (wal/create-file-wal *test-wal-dir*)
          entry (wal/wal-delete-document! wal-instance "test:1" "main")]

      (is (= :delete (:operation entry)))
      (is (= "test:1" (:document-id entry)))
      (is (nil? (:content entry))))))

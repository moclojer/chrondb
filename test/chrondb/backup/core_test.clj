(ns chrondb.backup.core-test
  (:require [chrondb.backup.core :as backup]
            [chrondb.backup.scheduler :as scheduler]
            [chrondb.backup.archive :as archive]
            [chrondb.backup.test-support :as support]
            [chrondb.storage.protocol :as protocol]
            [clojure.java.io :as io]
            [clojure.test :refer [deftest is]]))

(def bundle-refs ["refs/heads/main"])

(deftest full-backup-and-restore-tar
  (support/with-storage
    (fn [{:keys [storage ctx]}]
      (let [doc-file (io/file (:data-dir ctx) "snapshots/example.txt")]
        (io/make-parents doc-file)
        (spit doc-file "backup-tar-test")
        (let [output (str (:backup-dir ctx) "/full.tar.gz")
              result (backup/create-full-backup storage {:output-path output})
              file (io/file output)]
          (is (.exists file))
          (is (= :ok (:status result))))
        (is (= "backup-tar-test" (slurp doc-file)))
        (.delete doc-file)
        (is (not (.exists doc-file)))
        (let [restore-result (backup/restore-backup storage {:input-path (str (:backup-dir ctx) "/full.tar.gz")})]
          (is (= :ok (:status restore-result)))
          (is (= :archive (:restore-type restore-result))))
        (is (= "backup-tar-test" (slurp doc-file)))))))

(deftest full-backup-bundle-and-restore
  (support/with-storage
    (fn [{:keys [storage ctx]}]
      (protocol/save-document storage {:id "test:bundle" :value 42 :_table "test"})
      (let [bundle (str (:backup-dir ctx) "/snapshot.bundle")
            result (backup/create-full-backup storage {:output-path bundle
                                                       :format :bundle
                                                       :refs bundle-refs})]
        (is (= :ok (:status result)))
        (is (.exists (io/file bundle)))
        (protocol/delete-document storage "test:bundle")
        (is (nil? (protocol/get-document storage "test:bundle")))
        (let [restore-result (backup/restore-backup storage {:input-path bundle
                                                             :format :bundle
                                                             :refs bundle-refs})]
          (is (= :bundle (:restore-type restore-result)))
          (is (= 42 (:value (protocol/get-document storage "test:bundle")))))))))

(deftest incremental-backup-behaviour
  (support/with-storage
    (fn [{:keys [storage ctx]}]
      (protocol/save-document storage {:id "test:1" :value 1 :_table "test"})
      (let [full-bundle (str (:backup-dir ctx) "/base.bundle")]
        (backup/create-full-backup storage {:output-path full-bundle :format :bundle :refs bundle-refs})
        (let [base-oid (.resolve (:repository ctx) "HEAD")
              base-commit (when base-oid (.name base-oid))
              incremental (str (:backup-dir ctx) "/inc.bundle")]
          (is (some? base-commit))
          (protocol/save-document storage {:id "test:2" :value 2 :_table "test"})
          (let [result (backup/create-incremental-backup storage {:output-path incremental
                                                                  :base-commit base-commit})]
            (is (= :ok (:status result)))
            (is (.exists (io/file incremental)))))
        (is (thrown-with-msg? Exception #"Incremental backup requires :base-commit"
                              (backup/create-incremental-backup storage {:output-path "ignored"})))
        (is (thrown-with-msg? Exception #"Incremental backup requires :output-path"
                              (backup/create-incremental-backup storage {:base-commit "abc"})))
        (is (thrown-with-msg? Exception #"Incremental backups only supported"
                              (backup/create-incremental-backup storage {:output-path "ignored"
                                                                         :base-commit "abc"
                                                                         :format :tar})))))))

(deftest restore-errors-when-input-missing
  (support/with-storage
    (fn [{:keys [storage]}]
      (is (thrown-with-msg? Exception #"Backup file not found"
                            (backup/restore-backup storage {:input-path "/tmp/non-existent.tar.gz"})))
      (is (thrown-with-msg? Exception #"export-snapshot requires :output"
                            (backup/export-snapshot storage {})))
      (is (thrown-with-msg? Exception #"import-snapshot requires :input"
                            (backup/import-snapshot storage {}))))))

(deftest archive-incremental-raises
  (is (thrown-with-msg? Exception #"Tar-based incremental backups are not supported"
                        (archive/create-incremental-archive nil nil nil))))

(deftest scheduler-executes-and-cancels
  (let [executions (atom 0)
        job-id (scheduler/schedule #(swap! executions inc) 1)]
    (Thread/sleep 100)
    (is (pos? @executions))
    (is (true? (scheduler/cancel job-id)))))

(deftest enforcement-trims-old-backups
  (support/with-storage
    (fn [{:keys [ctx]}]
      (let [dir (:backup-dir ctx)
            now (System/currentTimeMillis)]
        (doseq [i (range 4)]
          (let [f (io/file dir (format "backup-%d.tar.gz" i))]
            (spit f "data")
            (.setLastModified f (- now (* 1000 i)))))
        (scheduler/enforce-retention dir 2)
        (is (= 2 (count (filter #(.isFile ^java.io.File %)
                                (.listFiles (io/file dir))))))))))

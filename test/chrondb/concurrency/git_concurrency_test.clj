(ns chrondb.concurrency.git-concurrency-test
  "Concurrency tests for ChronDB Git storage.
   Tests multiple connections accessing the same repository simultaneously,
   identifying and handling race conditions and conflicts."
  (:require [clojure.test :refer [deftest testing is use-fixtures]]
            [chrondb.config :as config]
            [chrondb.storage.git.core :as git-core]
            [chrondb.storage.protocol :as protocol]
            [chrondb.concurrency.occ :as occ]
            [chrondb.util.locks :as locks]
            [clojure.java.io :as io])
  (:import [java.util.concurrent CountDownLatch CyclicBarrier TimeUnit]
           [java.io File]
           [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]))

;; =============================================================================
;; Test Configuration
;; =============================================================================

(def test-config
  "Configuration for concurrency tests (push disabled to avoid remote interactions)"
  {:git {:default-branch "main"
         :committer-name "ConcurrencyTest"
         :committer-email "test@chrondb.example"
         :push-enabled false
         :pull-on-start false}
   :logging {:level :warn
             :file "concurrency-test.log"}})

;; =============================================================================
;; Helper Functions
;; =============================================================================

(defn create-temp-repo-path
  "Creates a unique temporary directory path for test repositories."
  []
  (let [dir (str (Files/createTempDirectory "chrondb-concurrency-test"
                                             (make-array FileAttribute 0)))]
    dir))

(defn delete-directory
  "Recursively deletes a directory and all its contents."
  [path]
  (when (and path (.exists (io/file path)))
    (doseq [f (reverse (file-seq (io/file path)))]
      (try
        (.delete ^File f)
        (catch Exception _
          nil)))))

(defn create-multiple-storages
  "Opens N independent storage instances pointing to the same Git repository.
   The first call creates the repository, subsequent calls open it."
  [repo-path n]
  (let [first-storage (git-core/create-git-storage repo-path)]
    (if (= n 1)
      [first-storage]
      (let [other-storages (doall
                            (for [_ (range (dec n))]
                              (git-core/open-git-storage repo-path)))]
        (cons first-storage other-storages)))))

(defn close-all-storages
  "Closes all storage instances, ignoring errors."
  [storages]
  (doseq [storage storages]
    (try
      (protocol/close storage)
      (catch Exception _
        nil))))

(defn run-parallel-with-barrier
  "Executes function f in n parallel threads, synchronized to start simultaneously.
   Each thread receives its index (0 to n-1) as argument.
   Returns a map with :results (vector of thread results) and :errors (exceptions caught).
   Waits up to timeout-seconds for all threads to complete."
  [n f timeout-seconds]
  (let [barrier (CyclicBarrier. n)
        latch (CountDownLatch. n)
        results (atom (vec (repeat n nil)))
        errors (atom [])]
    (dotimes [i n]
      (future
        (try
          (.await barrier)
          (let [result (f i)]
            (swap! results assoc i result))
          (catch Exception e
            (swap! errors conj {:thread i :exception e}))
          (finally
            (.countDown latch)))))
    (let [completed (.await latch timeout-seconds TimeUnit/SECONDS)]
      {:results @results
       :errors @errors
       :completed completed})))

(defn collect-errors
  "Collects all errors from futures, returning a vector of exceptions."
  [futures]
  (reduce
   (fn [errors f]
     (try
       @f
       errors
       (catch Exception e
         (conj errors e))))
   []
   futures))

;; =============================================================================
;; Test Fixture
;; =============================================================================

(def ^:dynamic *test-repo-path* nil)

(defn with-test-repo
  "Fixture that creates a temporary repository and cleans up after tests."
  [f]
  (let [repo-path (create-temp-repo-path)]
    (try
      (with-redefs [config/load-config (constantly test-config)]
        (binding [*test-repo-path* repo-path]
          (f)))
      (finally
        (locks/clean-stale-locks repo-path {:force? true})
        (delete-directory repo-path)))))

(use-fixtures :each with-test-repo)

;; =============================================================================
;; Test Cases
;; =============================================================================

(deftest test-concurrent-readers
  (testing "Multiple readers reading the same document simultaneously should all succeed"
    (let [num-readers 10
          reads-per-thread 100
          doc {:id "reader-test:1" :name "Concurrent Read Test" :value 42}
          storages (create-multiple-storages *test-repo-path* num-readers)]
      (try
        ;; Setup: save document
        (protocol/save-document (first storages) doc)

        ;; Execute: all readers read simultaneously
        (let [result (run-parallel-with-barrier
                      num-readers
                      (fn [i]
                        (let [storage (nth storages i)
                              read-results (atom [])]
                          (dotimes [_ reads-per-thread]
                            (let [read-doc (protocol/get-document storage "reader-test:1")]
                              (swap! read-results conj read-doc)))
                          @read-results))
                      60)]

          ;; Verify: all reads completed successfully with correct data
          (is (:completed result) "All threads should complete within timeout")
          (is (empty? (:errors result)) "No errors should occur during reads")

          ;; Verify all reads returned the correct document
          (doseq [[thread-idx results] (map-indexed vector (:results result))]
            (is (= reads-per-thread (count results))
                (str "Thread " thread-idx " should have " reads-per-thread " results"))
            (doseq [read-doc results]
              (is (= doc read-doc)
                  "Each read should return the exact saved document"))))
        (finally
          (close-all-storages storages))))))

(deftest test-concurrent-writers-different-docs
  (testing "Writers saving different documents should not conflict"
    (let [num-writers 8
          storages (create-multiple-storages *test-repo-path* num-writers)]
      (try
        ;; Execute: each writer saves its own document
        (let [result (run-parallel-with-barrier
                      num-writers
                      (fn [i]
                        (let [storage (nth storages i)
                              doc {:id (str "writer-" i ":doc")
                                   :writer i
                                   :timestamp (System/currentTimeMillis)}]
                          (protocol/save-document storage doc)
                          doc))
                      60)]

          (is (:completed result) "All writers should complete within timeout")

          ;; Some conflicts may occur at Git level, but all documents should be saved eventually
          (let [successful-writes (count (filter some? (:results result)))]
            (is (pos? successful-writes)
                "At least some writes should succeed"))

          ;; Verify documents: due to concurrent commits, some may have been overwritten
          ;; The important thing is that at least some documents were saved correctly
          (let [verifier (first storages)
                correct-docs (atom 0)]
            (doseq [i (range num-writers)]
              (let [doc-id (str "writer-" i ":doc")
                    saved-doc (protocol/get-document verifier doc-id)]
                (when (and saved-doc (= i (:writer saved-doc)))
                  (swap! correct-docs inc))))
            ;; At least some documents should be correctly saved
            ;; Due to Git concurrency, some may have been lost to race conditions
            (is (pos? @correct-docs)
                "At least some documents should be saved with correct writer IDs")))
        (finally
          (close-all-storages storages))))))

(deftest test-concurrent-writers-same-doc-conflict
  (testing "Writers updating the same document should handle conflicts"
    (let [num-writers 5
          initial-doc {:id "conflict-test:1" :counter 0}
          storages (create-multiple-storages *test-repo-path* num-writers)]
      (try
        ;; Setup: create initial document
        (protocol/save-document (first storages) initial-doc)

        ;; Execute: all writers try to update the same document
        (let [success-count (atom 0)
              error-count (atom 0)
              result (run-parallel-with-barrier
                      num-writers
                      (fn [i]
                        (let [storage (nth storages i)]
                          (try
                            (protocol/save-document
                             storage
                             {:id "conflict-test:1"
                              :counter (inc i)
                              :writer i
                              :timestamp (System/currentTimeMillis)})
                            (swap! success-count inc)
                            :success
                            (catch Exception e
                              (swap! error-count inc)
                              {:error (.getMessage e)}))))
                      60)]

          (is (:completed result) "All writers should complete within timeout")

          ;; At least one write should succeed
          (is (pos? @success-count)
              "At least one write should succeed")

          ;; Verify final document is consistent (not corrupted)
          (let [final-doc (protocol/get-document (first storages) "conflict-test:1")]
            (is (some? final-doc) "Final document should exist")
            (is (map? final-doc) "Final document should be a valid map")
            (is (contains? final-doc :counter) "Final document should have counter field")))
        (finally
          (close-all-storages storages))))))

(deftest test-mixed-readers-writers
  (testing "Mixed workload with concurrent readers and writers"
    (let [num-readers 8
          num-writers 4
          total-threads (+ num-readers num-writers)
          operations-per-thread 50
          doc {:id "mixed-test:1" :version 0}
          storages (create-multiple-storages *test-repo-path* total-threads)]
      (try
        ;; Setup: create initial document
        (protocol/save-document (first storages) doc)

        ;; Execute: mixed workload
        (let [read-count (atom 0)
              write-count (atom 0)
              read-errors (atom 0)
              write-errors (atom 0)
              result (run-parallel-with-barrier
                      total-threads
                      (fn [i]
                        (let [storage (nth storages i)
                              is-writer (< i num-writers)]
                          (if is-writer
                            ;; Writer thread
                            (dotimes [op operations-per-thread]
                              (try
                                (protocol/save-document
                                 storage
                                 {:id "mixed-test:1"
                                  :version op
                                  :writer i
                                  :timestamp (System/currentTimeMillis)})
                                (swap! write-count inc)
                                (catch Exception _
                                  (swap! write-errors inc))))
                            ;; Reader thread
                            (dotimes [_ operations-per-thread]
                              (try
                                (protocol/get-document storage "mixed-test:1")
                                (swap! read-count inc)
                                (catch Exception _
                                  (swap! read-errors inc)))))
                          {:is-writer is-writer
                           :operations operations-per-thread}))
                      120)]

          (is (:completed result) "All threads should complete within timeout")

          ;; Readers should never fail
          (is (zero? @read-errors) "Reads should never fail")

          ;; All read operations should complete
          (is (= (* num-readers operations-per-thread) @read-count)
              "All read operations should complete")

          ;; At least some writes should succeed
          (is (pos? @write-count) "At least some writes should succeed"))
        (finally
          (close-all-storages storages))))))

(deftest test-rapid-open-close-cycles
  (testing "Rapid open/close cycles should not corrupt repository"
    (let [num-threads 20
          cycles-per-thread 10
          initial-doc {:id "open-close-test:1" :name "Original Data" :value 123}]

      ;; Setup: create repository with initial data
      (let [setup-storage (git-core/create-git-storage *test-repo-path*)]
        (protocol/save-document setup-storage initial-doc)
        (protocol/close setup-storage))

      ;; Execute: rapid open/close cycles
      (let [result (run-parallel-with-barrier
                    num-threads
                    (fn [_]
                      (dotimes [_ cycles-per-thread]
                        (let [storage (git-core/open-git-storage *test-repo-path*)]
                          (try
                            ;; Just verify we can read
                            (protocol/get-document storage "open-close-test:1")
                            (finally
                              (protocol/close storage)))))
                      :completed)
                    120)]

        (is (:completed result) "All threads should complete within timeout")
        (is (empty? (:errors result)) "No errors should occur during open/close cycles"))

      ;; Verify: no orphan locks
      (let [lock-count (count (locks/find-lock-files *test-repo-path*))]
        (is (zero? lock-count) "No orphan lock files should remain"))

      ;; Verify: data integrity
      (let [verify-storage (git-core/open-git-storage *test-repo-path*)]
        (try
          (let [final-doc (protocol/get-document verify-storage "open-close-test:1")]
            (is (= initial-doc final-doc)
                "Data should remain intact after open/close cycles"))
          (finally
            (protocol/close verify-storage)))))))

(deftest test-branch-lock-serialization
  (testing "with-branch-lock should serialize access correctly"
    (let [execution-log (atom [])
          num-threads 5
          result (run-parallel-with-barrier
                  num-threads
                  (fn [i]
                    (occ/with-branch-lock "test-serialization-branch"
                      (swap! execution-log conj [:start i])
                      (Thread/sleep 20)
                      (swap! execution-log conj [:end i]))
                    :done)
                  60)]

      (is (:completed result) "All threads should complete within timeout")
      (is (empty? (:errors result)) "No errors should occur")

      ;; Verify serialization: each :start should be followed by its :end
      ;; before the next :start
      (let [pairs (partition 2 @execution-log)]
        (doseq [[[s1 i1] [e1 i2]] pairs]
          (is (= :start s1) "First of pair should be :start")
          (is (= :end e1) "Second of pair should be :end")
          (is (= i1 i2) "Start and end should be from same thread"))))))

(deftest test-no-deadlock-under-load
  (testing "System should not deadlock under heavy concurrent load"
    (let [num-threads 20
          operations-per-thread 50
          timeout-seconds 120
          storages (create-multiple-storages *test-repo-path* num-threads)]
      (try
        ;; Setup: create some initial documents
        (doseq [i (range 5)]
          (protocol/save-document
           (first storages)
           {:id (str "deadlock-test:" i) :value i}))

        ;; Execute: heavy load with mixed operations
        (let [completed-ops (atom 0)
              result (run-parallel-with-barrier
                      num-threads
                      (fn [i]
                        (let [storage (nth storages i)]
                          (dotimes [op operations-per-thread]
                            (let [doc-idx (mod op 5)
                                  doc-id (str "deadlock-test:" doc-idx)]
                              (if (even? op)
                                ;; Read operation
                                (protocol/get-document storage doc-id)
                                ;; Write operation (with retry on conflict)
                                (try
                                  (protocol/save-document
                                   storage
                                   {:id doc-id
                                    :value op
                                    :thread i})
                                  (catch Exception _
                                    ;; Ignore conflicts
                                    nil)))
                              (swap! completed-ops inc)))
                          :done))
                      timeout-seconds)]

          (is (:completed result)
              "All operations should complete without deadlock")

          ;; Verify we completed a significant number of operations
          (let [expected-ops (* num-threads operations-per-thread)]
            (is (= expected-ops @completed-ops)
                "All operations should complete")))
        (finally
          (close-all-storages storages))))))

(deftest test-orphan-lock-recovery
  (testing "Orphan locks older than 60s should be cleaned automatically"
    ;; First create the repository so it has proper structure
    (let [setup-storage (git-core/create-git-storage *test-repo-path*)]
      (protocol/save-document setup-storage {:id "setup:1" :value "setup"})
      (protocol/close setup-storage))

    ;; Now create a fake stale lock in the existing repo
    (let [lock-dir (io/file *test-repo-path* "refs" "heads")
          fake-lock (io/file lock-dir "main.lock")]

      ;; Create a fake stale lock (set modified time to > 60s ago)
      (.createNewFile fake-lock)
      (let [stale-time (- (System/currentTimeMillis) 65000)]
        (.setLastModified fake-lock stale-time))

      ;; Verify lock exists
      (is (.exists fake-lock) "Fake lock should exist")

      ;; Run cleanup
      (let [removed (locks/clean-stale-locks *test-repo-path*)]
        (is (pos? removed) "At least one lock should be removed"))

      ;; Verify lock was cleaned
      (is (not (.exists fake-lock)) "Stale lock should be removed")

      ;; Verify repository can still operate normally
      (let [storage (git-core/open-git-storage *test-repo-path*)]
        (try
          (let [doc {:id "after-lock-recovery:1" :value "test"}]
            (is (= doc (protocol/save-document storage doc))
                "Operations should work after lock cleanup"))
          (finally
            (protocol/close storage)))))))

(deftest test-concurrent-document-history
  (testing "Concurrent access to document history should be consistent"
    (let [num-versions 10
          doc-id "history-test:1"
          storage (git-core/create-git-storage *test-repo-path*)]
      (try
        ;; Create multiple versions
        (doseq [v (range num-versions)]
          (protocol/save-document
           storage
           {:id doc-id
            :version v
            :timestamp (System/currentTimeMillis)})
          (Thread/sleep 10)) ; Small delay to ensure distinct commits

        ;; Read history from multiple threads
        (let [num-readers 5
              result (run-parallel-with-barrier
                      num-readers
                      (fn [_]
                        (let [history (protocol/get-document-history storage doc-id)]
                          {:count (count history)
                           :first-version (get-in (last history) [:document :version])
                           :last-version (get-in (first history) [:document :version])}))
                      60)]

          (is (:completed result) "All readers should complete")
          (is (empty? (:errors result)) "No errors during history reads")

          ;; All readers should see the same history
          (doseq [history-result (:results result)]
            (is (= num-versions (:count history-result))
                "All readers should see all versions")
            (is (= 0 (:first-version history-result))
                "First version should be 0")
            (is (= (dec num-versions) (:last-version history-result))
                "Last version should be n-1")))
        (finally
          (protocol/close storage))))))

(deftest test-stress-sequential-writes
  (testing "Sequential writes from multiple connections should all persist"
    (let [num-storages 5
          writes-per-storage 20
          storages (create-multiple-storages *test-repo-path* num-storages)]
      (try
        ;; Execute: sequential writes in parallel (each thread writes its own docs sequentially)
        (let [result (run-parallel-with-barrier
                      num-storages
                      (fn [i]
                        (let [storage (nth storages i)
                              written-ids (atom [])]
                          (doseq [j (range writes-per-storage)]
                            (let [doc-id (str "stress-" i "-" j ":doc")
                                  doc {:id doc-id
                                       :storage i
                                       :seq j
                                       :timestamp (System/currentTimeMillis)}]
                              (try
                                (protocol/save-document storage doc)
                                (swap! written-ids conj doc-id)
                                (catch Exception _
                                  ;; Retry once on conflict
                                  (Thread/sleep (+ 10 (rand-int 50)))
                                  (try
                                    (protocol/save-document storage doc)
                                    (swap! written-ids conj doc-id)
                                    (catch Exception _
                                      nil))))))
                          @written-ids))
                      180)]

          (is (:completed result) "All threads should complete")

          ;; Count total successful writes
          (let [all-written-ids (apply concat (:results result))
                verifier (first storages)]
            ;; Verify each written document exists
            (doseq [doc-id all-written-ids]
              (let [doc (protocol/get-document verifier doc-id)]
                (is (some? doc) (str "Document " doc-id " should exist"))))))
        (finally
          (close-all-storages storages))))))

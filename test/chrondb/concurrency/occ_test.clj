(ns chrondb.concurrency.occ-test
  (:require [clojure.test :refer [deftest testing is]]
            [chrondb.concurrency.occ :as occ]))

(deftest test-create-version-tracker
  (testing "Creating a version tracker"
    (let [tracker (occ/create-version-tracker)]
      (is (some? tracker))
      (is (instance? chrondb.concurrency.occ.InMemoryVersionTracker tracker)))))

(deftest test-version-tracking
  (testing "Version increment and get"
    (let [tracker (occ/create-version-tracker)]
      ;; Initial version should be 0 or nil
      (is (= 0 (occ/get-version tracker "doc:1" "main")))

      ;; Increment version
      (occ/increment-version tracker "doc:1" "main")
      (is (= 1 (occ/get-version tracker "doc:1" "main")))

      ;; Increment again
      (occ/increment-version tracker "doc:1" "main")
      (is (= 2 (occ/get-version tracker "doc:1" "main")))

      ;; Different document should have version 0
      (is (= 0 (occ/get-version tracker "doc:2" "main"))))))

(deftest test-version-verification
  (testing "Version verification success"
    (let [tracker (occ/create-version-tracker)]
      (occ/increment-version tracker "doc:1" "main")
      ;; Should not throw with correct expected version
      (is (nil? (occ/verify-version tracker "doc:1" "main" 1)))))

  (testing "Version verification failure"
    (let [tracker (occ/create-version-tracker)]
      (occ/increment-version tracker "doc:1" "main")
      (occ/increment-version tracker "doc:1" "main")
      ;; Should throw with incorrect expected version
      (is (thrown? clojure.lang.ExceptionInfo
                   (occ/verify-version tracker "doc:1" "main" 1))))))

(deftest test-occ-retry
  (testing "OCC retry with no conflicts"
    (let [call-count (atom 0)
          result (occ/with-occ-retry
                   (fn []
                     (swap! call-count inc)
                     :success)
                   {})]
      (is (= :success result))
      (is (= 1 @call-count))))

  (testing "OCC retry with eventual success"
    (let [call-count (atom 0)
          result (occ/with-occ-retry
                   (fn []
                     (swap! call-count inc)
                     (if (< @call-count 3)
                       (throw (occ/version-conflict-exception "doc:1" "main" 1 2))
                       :success))
                   {:max-retries 5})]
      (is (= :success result))
      (is (= 3 @call-count))))

  (testing "OCC retry exceeds max retries"
    (let [call-count (atom 0)]
      (is (thrown? clojure.lang.ExceptionInfo
                   (occ/with-occ-retry
                     (fn []
                       (swap! call-count inc)
                       (throw (occ/version-conflict-exception "doc:1" "main" 1 2)))
                     {:max-retries 3}))))))

(deftest test-branch-lock
  (testing "Branch lock serializes access"
    (let [results (atom [])
          threads (doall
                   (for [i (range 5)]
                     (Thread.
                      (fn []
                        (occ/with-branch-lock "test-branch"
                          (swap! results conj [:start i])
                          (Thread/sleep 10)
                          (swap! results conj [:end i]))))))]
      ;; Start all threads
      (doseq [t threads] (.start t))
      ;; Wait for completion
      (doseq [t threads] (.join t))

      ;; Verify serialization - each :start should be followed by its :end
      ;; before the next :start
      (let [pairs (partition 2 @results)]
        (doseq [[[s1 i1] [e1 i2]] pairs]
          (is (= :start s1))
          (is (= :end e1))
          (is (= i1 i2)))))))

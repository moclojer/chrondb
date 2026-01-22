(ns chrondb.observability.health-test
  (:require [clojure.test :refer [deftest testing is]]
            [clojure.string :as str]
            [chrondb.observability.health :as health]))

(deftest test-health-status-constants
  (testing "Health status constants"
    (is (= :healthy health/HEALTHY))
    (is (= :degraded health/DEGRADED))
    (is (= :unhealthy health/UNHEALTHY))))

(deftest test-create-health-check
  (testing "Create health check"
    (let [check (health/create-health-check
                 :test-check
                 "Test check"
                 (fn [] {:status :healthy}))]
      (is (= :test-check (:name check)))
      (is (= "Test check" (:description check)))
      (is (fn? (:check-fn check))))))

(deftest test-run-health-check
  (testing "Run healthy check"
    (let [check (health/create-health-check
                 :test-check
                 "Test"
                 (fn [] {:status :healthy :message "OK"}))
          result (health/run-health-check check)]
      (is (= :healthy (:status result)))
      (is (= "OK" (:message result)))
      (is (some? (:duration-ms result)))))

  (testing "Run unhealthy check"
    (let [check (health/create-health-check
                 :failing-check
                 "Test"
                 (fn [] {:status :unhealthy :message "Failed"}))
          result (health/run-health-check check)]
      (is (= :unhealthy (:status result)))
      (is (= "Failed" (:message result)))))

  (testing "Check that throws becomes unhealthy"
    (let [check (health/create-health-check
                 :error-check
                 "Test"
                 (fn [] (throw (Exception. "Boom"))))
          result (health/run-health-check check)]
      (is (= :unhealthy (:status result)))
      (is (str/includes? (:message result) "Boom")))))

(deftest test-aggregate-status
  (testing "All healthy -> healthy"
    (let [results [{:status :healthy} {:status :healthy}]]
      (is (= :healthy (health/aggregate-status results)))))

  (testing "One degraded -> degraded"
    (let [results [{:status :healthy} {:status :degraded}]]
      (is (= :degraded (health/aggregate-status results)))))

  (testing "One unhealthy -> unhealthy"
    (let [results [{:status :healthy} {:status :unhealthy}]]
      (is (= :unhealthy (health/aggregate-status results)))))

  (testing "Empty results -> healthy"
    (is (= :healthy (health/aggregate-status [])))))

(deftest test-health-checker
  (testing "Create and run health checker"
    (let [checker (health/create-health-checker
                   [(health/create-health-check
                     :check1 "Check 1" (fn [] {:status :healthy}))
                    (health/create-health-check
                     :check2 "Check 2" (fn [] {:status :healthy}))])
          result (health/run-all-checks checker)]
      (is (= :healthy (:status result)))
      (is (= 2 (count (:checks result)))))))

(deftest test-disk-space-check
  (testing "Disk space check runs"
    (let [result ((health/disk-space-check "/tmp"))]
      (is (contains? result :status))
      (is (contains? result :total-gb))
      (is (contains? result :free-gb))
      (is (contains? result :used-percent)))))

(deftest test-memory-check
  (testing "Memory check runs"
    (let [result ((health/memory-check))]
      (is (contains? result :status))
      (is (contains? result :max-mb))
      (is (contains? result :used-mb))
      (is (contains? result :used-percent)))))

(deftest test-status-to-http-code
  (testing "Status to HTTP code conversion"
    (is (= 200 (health/status->http-code :healthy)))
    (is (= 200 (health/status->http-code :degraded)))
    (is (= 503 (health/status->http-code :unhealthy)))))

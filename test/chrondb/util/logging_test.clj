(ns chrondb.util.logging-test
  (:require [chrondb.util.logging :as log]
            [clojure.test :refer [deftest testing is use-fixtures]]
            [clojure.string :as str]))

(def test-config
  {:min-level :debug})

(defn setup-logging [f]
  (log/init! test-config)
  (f))

(use-fixtures :each setup-logging)

(deftest test-logging-levels
  (testing "Basic logging"
    (let [output (with-out-str
                   (log/log-debug "Debug message")
                   (log/log-info "Info message")
                   (log/log-warn "Warning message")
                   (log/log-error "Error message"))]
      (is (str/includes? output "Debug message"))
      (is (str/includes? output "Info message"))
      (is (str/includes? output "Warning message"))
      (is (str/includes? output "Error message"))))

  (testing "Exception logging"
    (let [output (with-out-str
                   (try
                     (throw (Exception. "Test exception"))
                     (catch Exception e
                       (log/log-error "An error occurred" e))))]
      (is (str/includes? output "Test exception"))
      (is (str/includes? output "java.lang.Exception"))
      (is (str/includes? output "chrondb.util.logging_test")))))
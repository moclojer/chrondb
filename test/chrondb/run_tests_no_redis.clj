(ns chrondb.run-tests-no-redis
  (:require [clojure.test :as test]
            [clojure.tools.namespace.find :as find]
            [clojure.java.io :as io]))

(defn run-tests-except-redis []
  (println "Running all tests except Redis tests...")

  ;; Find all test namespaces
  (let [all-namespaces (find/find-namespaces-in-dir (io/file "test"))
        ;; Filter out Redis namespaces
        non-redis-namespaces (filter #(not (.contains (str %) "chrondb.api.redis")) all-namespaces)
        results (atom {:test 0 :pass 0 :fail 0 :error 0})]

    (println "Found" (count non-redis-namespaces) "test namespaces to run")

    ;; Require and run each namespace
    (doseq [ns-sym non-redis-namespaces]
      (println "Testing" ns-sym)
      (require ns-sym)
      (let [ns-result (test/run-tests ns-sym)]
        (swap! results update :test + (:test ns-result 0))
        (swap! results update :pass + (:pass ns-result 0))
        (swap! results update :fail + (:fail ns-result 0))
        (swap! results update :error + (:error ns-result 0))))

    ;; Return the combined results
    @results))

(defn -main [& args]
  (let [result (run-tests-except-redis)
        exit-code (if (= 0 (+ (:fail result 0) (:error result 0))) 0 1)]
    (println "\nSummary:")
    (println "Tests:" (:test result 0))
    (println "Assertions:" (:pass result 0))
    (println "Failures:" (:fail result 0))
    (println "Errors:" (:error result 0))
    (System/exit exit-code)))

;; Run the tests when this script is executed directly
(when (= *file* (System/getProperty "babashka.file"))
  (-main))
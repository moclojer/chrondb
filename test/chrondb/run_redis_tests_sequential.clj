(ns chrondb.run-redis-tests-sequential
  (:require [chrondb.test-helpers :as helpers]
            [clojure.test :as test]))

(def redis-test-namespaces
  ["chrondb.api.redis.redis-test"
   "chrondb.api.redis.redis-integration-test"
   "chrondb.api.redis.redis-jedis-test"
   "chrondb.api.redis.redis-list-test"
   "chrondb.api.redis.redis-benchmark-test"])

(defn run-test-with-timeout
  "Run a test namespace with a timeout."
  [ns-name]
  (println "Running" ns-name "...")
  (try
    (require (symbol ns-name))
    (let [result (test/run-tests (symbol ns-name))]
      (if (= 0 (+ (:fail result 0) (:error result 0)))
        (println ns-name "completed successfully.")
        (println "ERROR:" ns-name "failed!")))
    (helpers/kill-processes)
    (catch Exception e
      (println "ERROR: Exception running" ns-name "-" (.getMessage e)))))

(defn run-all-redis-tests []
  (println "Running Redis Protocol Implementation Tests for ChronDB")
  (println "======================================================")
  (println)

  (doseq [ns-name redis-test-namespaces]
    (run-test-with-timeout ns-name)
    (println))

  (println "All Redis tests completed!"))

(defn -main [& args]
  (run-all-redis-tests)
  (System/exit 0))

;; Run the tests when this script is executed directly
(when (= *file* (System/getProperty "babashka.file"))
  (-main))
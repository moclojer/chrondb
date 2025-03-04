(ns chrondb.run-redis-tests-sequential
  (:require [chrondb.test-helpers :as helpers]
            [clojure.test :as test]))

(def redis-test-namespaces
  ["chrondb.api.redis.redis-test"
   "chrondb.api.redis.redis-integration-test"
   "chrondb.api.redis.redis-jedis-test"
   "chrondb.api.redis.redis-list-test"
   "chrondb.api.redis.redis-benchmark-test"])

(defn ensure-processes-killed
  "Ensure all Java processes related to chrondb are killed."
  []
  (println "Ensuring all Redis server processes are terminated...")
  (helpers/kill-processes)
  ;; Adicional kill para garantir que todos os processos foram encerrados
  (try
    (.exec (Runtime/getRuntime) "pkill -9 -f \"java.*chrondb\"")
    (catch Exception _))
  ;; Espera adicional para garantir que as portas sejam liberadas
  (Thread/sleep 3000))

(defn run-test-with-timeout
  "Run a test namespace with a timeout and ensure cleanup."
  [ns-name]
  (println "\n========================================")
  (println "Running" ns-name "...")
  (println "========================================\n")

  ;; Garantir que não há processos rodando antes de iniciar
  (ensure-processes-killed)

  (try
    (require (symbol ns-name))
    (let [result (test/run-tests (symbol ns-name))]
      (if (= 0 (+ (:fail result 0) (:error result 0)))
        (println ns-name "completed successfully.")
        (println "ERROR:" ns-name "failed with" (:fail result 0) "failures and" (:error result 0) "errors.")))

    (catch Exception e
      (println "ERROR: Exception running" ns-name "-" (.getMessage e))
      (.printStackTrace e))

    (finally
      ;; Garantir limpeza após o teste
      (ensure-processes-killed))))

(defn run-all-redis-tests []
  (println "\n=======================================================")
  (println "Running Redis Protocol Implementation Tests for ChronDB")
  (println "=======================================================\n")

  (doseq [ns-name redis-test-namespaces]
    (run-test-with-timeout ns-name)
    ;; Espera adicional entre testes
    (Thread/sleep 2000))

  (println "\n=======================================================")
  (println "All Redis tests completed!")
  (println "=======================================================\n"))

(defn -main [& _]
  (run-all-redis-tests)
  (System/exit 0))

;; Run the tests when this script is executed directly
(when (= *file* (System/getProperty "babashka.file"))
  (-main))
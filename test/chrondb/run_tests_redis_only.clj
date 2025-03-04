(ns chrondb.run-tests-redis-only
  (:require [clojure.test :as test]
            [clojure.tools.namespace.find :as find]
            [clojure.java.io :as io]
            [clojure.string :as str]))

(defn parse-args [args]
  (loop [args args
         result {}]
    (if (empty? args)
      result
      (let [arg (first args)]
        (cond
          (= arg "--namespace-regex")
          (if (second args)
            (recur (drop 2 args) (assoc result :namespace-regex (second args)))
            (recur (rest args) result))

          :else
          (recur (rest args) result))))))

(defn run-tests-redis-only [& {:keys [namespace-regex]}]
  (println "Running only Redis tests...")
  (when namespace-regex
    (println "Using namespace regex filter:" namespace-regex))

  ;; Find all Redis test namespaces
  (let [all-namespaces (find/find-namespaces-in-dir (io/file "test"))
        ;; Filter to include only Redis namespaces
        redis-namespaces (filter #(.contains (str %) "chrondb.api.redis") all-namespaces)
        ;; Apply additional namespace regex filter if provided
        filtered-namespaces (if namespace-regex
                              (filter #(re-find (re-pattern namespace-regex) (str %)) redis-namespaces)
                              redis-namespaces)
        results (atom {:test 0 :pass 0 :fail 0 :error 0})]

    (println "Found" (count filtered-namespaces) "Redis test namespaces to run")

    ;; Require and run each namespace
    (doseq [ns-sym filtered-namespaces]
      (println "Testing" ns-sym)
      (require ns-sym)
      (let [ns-result (test/run-tests ns-sym)]
        (swap! results update :test + (:test ns-result 0))
        (swap! results update :pass + (:pass ns-result 0))
        (swap! results update :fail + (:fail ns-result 0))
        (swap! results update :error + (:error ns-result 0)))

      ;; Kill any lingering Java processes that might be holding ports
      (try
        (.exec (Runtime/getRuntime) "pkill -f \"java.*chrondb\"")
        (Thread/sleep 2000) ;; Sleep for 2 seconds
        (catch Exception e
          (println "Warning: Failed to kill processes:" (.getMessage e)))))

    ;; Return the combined results
    @results))

(defn -main [& args]
  (let [opts (parse-args args)
        result (run-tests-redis-only :namespace-regex (:namespace-regex opts))
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
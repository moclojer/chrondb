(ns chrondb.test-helpers
  (:require [chrondb.storage.memory :as memory]
            [chrondb.index.lucene :as lucene]
            [clojure.java.io :as io]
            [clojure.test :as test]
            [clojure.tools.namespace.find :as find])
  (:import [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]))

(defn create-temp-dir []
  (let [dir (str (Files/createTempDirectory "chrondb-test" (make-array FileAttribute 0)))]
    (-> (io/file dir)
        (.deleteOnExit))
    dir))

(defn delete-directory [path]
  (when (.exists (io/file path))
    (try
      (doseq [f (reverse (file-seq (io/file path)))]
        (try
          (io/delete-file f true)
          (catch Exception e
            (println "Warning: Failed to delete file" (.getPath f) "-" (.getMessage e)))))
      (catch Exception e
        (println "Warning: Failed to clean up directory" path "-" (.getMessage e))))))

(defmacro with-test-data [[storage-sym index-sym] & body]
  `(let [index-dir# (create-temp-dir)
         ~storage-sym (memory/create-memory-storage)
         ~index-sym (lucene/create-lucene-index index-dir#)]
     (try
       ~@body
       (finally
         (try
           (.close ~index-sym)
           (catch Exception e#
             (println "Warning: Failed to close index -" (.getMessage e#))))
         (delete-directory index-dir#)))))

;; Funções compartilhadas para execução de testes

(defn parse-args
  "Parse command line arguments into a map of options."
  [args]
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

(defn kill-processes
  "Kill any lingering Java processes that might be holding ports."
  []
  (try
    (.exec (Runtime/getRuntime) "pkill -f \"java.*chrondb\"")
    (Thread/sleep 2000) ;; Sleep for 2 seconds
    (catch Exception e
      (println "Warning: Failed to kill processes:" (.getMessage e)))))

(defn run-tests-with-filter
  "Run tests with namespace filtering.
   Options:
   - :namespace-filter - Function to filter namespaces (required)
   - :namespace-regex - Additional regex to filter namespaces (optional)
   - :cleanup-fn - Function to run after each test (optional)
   - :description - Description of the test run (required)"
  [& {:keys [namespace-filter namespace-regex cleanup-fn description]}]
  (println description)
  (when namespace-regex
    (println "Using namespace regex filter:" namespace-regex))

  ;; Find all test namespaces
  (let [all-namespaces (find/find-namespaces-in-dir (io/file "test"))
        ;; Apply primary filter
        filtered-namespaces-1 (filter namespace-filter all-namespaces)
        ;; Apply additional namespace regex filter if provided
        filtered-namespaces (if namespace-regex
                              (filter #(re-find (re-pattern namespace-regex) (str %)) filtered-namespaces-1)
                              filtered-namespaces-1)
        results (atom {:test 0 :pass 0 :fail 0 :error 0})]

    (println "Found" (count filtered-namespaces) "test namespaces to run")

    ;; Require and run each namespace
    (doseq [ns-sym filtered-namespaces]
      (println "Testing" ns-sym)
      (require ns-sym)
      (let [ns-result (test/run-tests ns-sym)]
        (swap! results update :test + (:test ns-result 0))
        (swap! results update :pass + (:pass ns-result 0))
        (swap! results update :fail + (:fail ns-result 0))
        (swap! results update :error + (:error ns-result 0)))

      ;; Run cleanup function if provided
      (when cleanup-fn (cleanup-fn)))

    ;; Return the combined results
    @results))

(defn print-summary
  "Print a summary of test results."
  [result]
  (println "\nSummary:")
  (println "Tests:" (:test result 0))
  (println "Assertions:" (:pass result 0))
  (println "Failures:" (:fail result 0))
  (println "Errors:" (:error result 0)))

(defn run-test-main
  "Main function for running tests with command line arguments.
   Options:
   - :args - Command line arguments
   - :namespace-filter - Function to filter namespaces
   - :cleanup-fn - Function to run after each test (optional)
   - :description - Description of the test run"
  [& {:keys [args namespace-filter cleanup-fn description]}]
  (let [opts (parse-args args)
        result (run-tests-with-filter
                :namespace-filter namespace-filter
                :namespace-regex (:namespace-regex opts)
                :cleanup-fn cleanup-fn
                :description description)
        exit-code (if (= 0 (+ (:fail result 0) (:error result 0))) 0 1)]
    (print-summary result)
    (System/exit exit-code)))
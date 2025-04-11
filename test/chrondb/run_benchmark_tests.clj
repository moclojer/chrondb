(ns chrondb.run-benchmark-tests
  (:require [chrondb.test-helpers :as helpers]))

(defn benchmark-namespace? [ns-sym]
  (.contains (str ns-sym) "chrondb.benchmark"))

(defn -main [& args]
  (helpers/run-test-main
   :args args
   :namespace-filter benchmark-namespace?
   :cleanup-fn helpers/kill-processes
   :description "Running only benchmark tests..."))

;; Run the tests when this script is executed directly
(when (= *file* (System/getProperty "babashka.file"))
  (-main))
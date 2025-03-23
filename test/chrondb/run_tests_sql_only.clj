(ns chrondb.run-tests-sql-only
  (:require [chrondb.test-helpers :as helpers]))

(defn sql-namespace? [ns-sym]
  (.contains (str ns-sym) "chrondb.api.sql"))

(defn -main [& args]
  (helpers/run-test-main
   :args args
   :namespace-filter sql-namespace?
   :cleanup-fn helpers/kill-processes
   :description "Running only SQL tests..."))

;; Run the tests when this script is executed directly
(when (= *file* (System/getProperty "babashka.file"))
  (-main))
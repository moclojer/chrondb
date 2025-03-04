(ns chrondb.run-tests-redis-only
  (:require [chrondb.test-helpers :as helpers]))

(defn redis-namespace? [ns-sym]
  (.contains (str ns-sym) "chrondb.api.redis"))

(defn -main [& args]
  (helpers/run-test-main
   :args args
   :namespace-filter redis-namespace?
   :cleanup-fn helpers/kill-processes
   :description "Running only Redis tests..."))

;; Run the tests when this script is executed directly
(when (= *file* (System/getProperty "babashka.file"))
  (-main))
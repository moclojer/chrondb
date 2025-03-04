(ns chrondb.run-tests-no-redis
  (:require [chrondb.test-helpers :as helpers]))

(defn non-redis-namespace? [ns-sym]
  (not (.contains (str ns-sym) "chrondb.api.redis")))

(defn -main [& args]
  (helpers/run-test-main
   :args args
   :namespace-filter non-redis-namespace?
   :description "Running all tests except Redis tests..."))

;; Run the tests when this script is executed directly
(when (= *file* (System/getProperty "babashka.file"))
  (-main))
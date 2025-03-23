(ns chrondb.run-tests-non-external-protocol
  (:require [chrondb.test-helpers :as helpers]))

(defn non-external-protocol? [ns-sym]
  (and (not (.contains (str ns-sym) "chrondb.api.redis"))
       (not (.contains (str ns-sym) "chrondb.api.sql"))))

(defn -main [& args]
  (helpers/run-test-main
   :args args
   :namespace-filter non-external-protocol?
   :description "Running all tests except Redis tests..."))

;; Run the tests when this script is executed directly
(when (= *file* (System/getProperty "babashka.file"))
  (-main))
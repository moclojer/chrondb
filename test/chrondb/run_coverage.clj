(ns chrondb.run-coverage
  (:require [cloverage.coverage :as cloverage]))

(defn -main [& args]
  (println "Running code coverage excluding Redis tests...")

  ;; Configuração para o Cloverage
  (let [opts {:ns-regex "chrondb.*"
              :test-ns-regex "chrondb.*-test"
              :exclude-namespaces ["chrondb.api.redis.*"]
              :src-ns-path ["src"]
              :test-ns-path ["test"]
              :fail-threshold 65
              :codecov true
              :html true
              :output "target/coverage"}]

    ;; Executa o Cloverage com as opções
    (cloverage/run-main opts))

  (println "Coverage completed!"))

;; Run when this script is executed directly
(when (= *file* (System/getProperty "babashka.file"))
  (-main))
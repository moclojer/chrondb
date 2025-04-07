(ns chrondb.api.sql.test-util
  "Utilities for testing SQL functionality"
  (:require [chrondb.storage.protocol :as storage]))

(defn get-writer-output-util
  "Gets the output from a writer map for testing"
  [writer-map]
  (.flush (:writer writer-map))
  (str (.getBuffer (:string-writer writer-map))))

(defn create-test-data-with-plain-ids
  "Prepares test data with plain IDs and _table field"
  [storage]
  (let [table-name "test"]
    (doseq [id ["1" "2" "3"]]
      (let [num (Integer/parseInt id)
            doc {:id id
                 :_table table-name
                 :nome (str "Item " num)
                 :valor (* num 10)
                 :ativo (odd? num)}]
        (storage/save-document storage doc)))))
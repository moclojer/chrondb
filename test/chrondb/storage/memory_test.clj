(ns chrondb.storage.memory-test
  (:require [chrondb.storage.memory :as memory]
            [chrondb.storage.protocol :as protocol]
            [clojure.test :refer [deftest is testing]]))

(deftest test-memory-storage
  (testing "Memory storage operations"
    (let [storage (memory/create-memory-storage)]
      (testing "Save document"
        (let [doc {:id "1" :name "Test"}
              saved (protocol/save-document storage doc)]
          (is (= doc saved))))

      (testing "Get document"
        (let [doc {:id "1" :name "Test"}]
          (protocol/save-document storage doc)
          (is (= doc (protocol/get-document storage "1")))))

      (testing "Delete document"
        (let [doc {:id "1" :name "Test"}]
          (protocol/save-document storage doc)
          (is (protocol/delete-document storage "1"))
          (is (nil? (protocol/get-document storage "1")))))

      (.close storage))))
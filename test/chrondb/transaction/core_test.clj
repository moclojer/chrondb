(ns chrondb.transaction.core-test
  (:require [chrondb.transaction.core :as tx]
            [clojure.test :refer [deftest is testing]]))

(deftest with-transaction-success
  (testing "updates context and triggers completion handler"
    (let [captured (atom nil)
          result (tx/with-transaction [::storage {:origin "rest"
                                                  :user "user-123"
                                                  :flags ["migration"]
                                                  :on-complete (fn [ctx res err]
                                                                 (reset! captured {:ctx ctx :res res :err err}))}]
                   (tx/add-flags! "bulk-load")
                   (tx/merge-metadata! {:request-id "req-1"})
                   :ok)
          ctx (:ctx @captured)]
      (is (= :ok result))
      (is (= :ok (:res @captured)))
      (is (nil? (:err @captured)))
      (is (= :committed (:status ctx)))
      (is (= "rest" (:origin ctx)))
      (is (= "user-123" (:user ctx)))
      (is (contains? (:flags ctx) "bulk-load"))
      (is (contains? (:flags ctx) "migration"))
      (is (= "req-1" (get-in ctx [:metadata :request-id]))))))

(deftest with-transaction-rollback
  (testing "marks rollback flag on failure"
    (let [captured (atom nil)]
      (is (thrown-with-msg? Exception #"boom"
                            (tx/with-transaction [::storage {:origin "rest"
                                                             :on-complete (fn [ctx _ err]
                                                                            (reset! captured {:ctx ctx :err err}))}]
                              (tx/add-flags! "bulk-load")
                              (throw (Exception. "boom")))))
      (let [ctx (:ctx @captured)]
        (is (= :rolled-back (:status ctx)))
        (is (some #(= "rollback" %) (:flags ctx)))
        (is (= "rest" (:origin ctx)))
        (is (instance? Exception (:err @captured)))))))

(deftest context-for-commit-inside-transaction
  (testing "produces note payload with commit overrides"
    (let [note (tx/with-transaction [::storage {:origin "rest" :user "alice"}]
                 (tx/context-for-commit {:commit-id "abc123"
                                         :branch "main"
                                         :operation "save"}))]
      (is (= "abc123" (:commit_id note)))
      (is (= "rest" (:origin note)))
      (is (= "alice" (:user note)))
      (is (= "save" (:operation note)))
      (is (= "main" (:branch note)))
      (is (contains? note :tx_id)))))

(deftest context-for-commit-no-transaction
  (testing "creates ephemeral context when none bound"
    (let [note (tx/context-for-commit {:origin "cli" :commit-id "def456"})]
      (is (= "cli" (:origin note)))
      (is (= "def456" (:commit_id note)))
      (is (contains? note :tx_id)))))
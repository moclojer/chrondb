(ns chrondb.api.sql.execution.transaction-test
  (:require [clojure.test :refer [deftest is testing]]
            [chrondb.api.sql.execution.query :as query]
            [chrondb.api.sql.parser.statements :as statements]
            [chrondb.storage.protocol :as storage-protocol]
            [chrondb.transaction.core :as tx])
  (:import [java.io ByteArrayOutputStream]
           [java.util.concurrent CountDownLatch]))

(defn- make-capturing-storage []
  (let [docs (atom {})
        contexts (atom [])]
    {:storage (reify storage-protocol/Storage
                (save-document [_ doc]
                  (storage-protocol/save-document _ doc nil))
                (save-document [_ doc branch]
                  (swap! docs assoc (:id doc) doc)
                  (let [payload (tx/context-for-commit {:operation "save-document"
                                                        :document-id (:id doc)
                                                        :branch branch
                                                        :metadata (cond-> {}
                                                                    (:_table doc) (assoc :table (:_table doc)))})]
                    (swap! contexts conj payload)
                    doc))

                (get-document [_ id]
                  (@docs id))
                (get-document [_ id _branch]
                  (@docs id))

                (delete-document [_ id]
                  (storage-protocol/delete-document _ id nil))
                (delete-document [_ id branch]
                  (let [exists (contains? @docs id)]
                    (when exists
                      (swap! docs dissoc id)
                      (let [payload (tx/context-for-commit {:operation "delete-document"
                                                            :document-id id
                                                            :branch branch
                                                            :flags ["delete"]})]
                        (swap! contexts conj payload)))
                    exists))

                (get-documents-by-prefix [_ prefix]
                  (storage-protocol/get-documents-by-prefix _ prefix nil))
                (get-documents-by-prefix [_ prefix _branch]
                  (->> @docs
                       (filter (fn [[doc-id _]] (.startsWith (str doc-id) prefix)))
                       (map second)
                       vec))

                (get-documents-by-table [_ table]
                  (storage-protocol/get-documents-by-table _ table nil))
                (get-documents-by-table [_ table _branch]
                  (->> @docs
                       vals
                       (filter #(= (:_table %) table))
                       vec))

                (get-document-history [_ _]
                  [])
                (get-document-history [_ _ _]
                  [])

                (close [_] nil))
     :docs docs
     :contexts contexts}))

(defn- parse-sql [sql]
  (statements/parse-sql-query sql))

(defn- await-latch [^CountDownLatch latch]
  (.await latch)
  latch)

(deftest sql-insert-concurrency-records-distinct-transactions
  (testing "Concurrent INSERT statements capture isolated transaction contexts"
    (let [{:keys [storage contexts]} (make-capturing-storage)
          thread-count 5
          ready-latch (CountDownLatch. thread-count)
          start-latch (CountDownLatch. 1)
          futures (mapv
                   (fn [i]
                     (future
                       (.countDown ready-latch)
                       (await-latch start-latch)
                       (let [doc-id (format "user:%d" i)
                             sql (format "INSERT INTO public.user (id, name, age) VALUES ('%s', 'User %d', %d)"
                                         doc-id i (+ 20 i))
                             parsed (parse-sql sql)]
                         (query/handle-insert-case storage nil (ByteArrayOutputStream.) parsed))))
                   (range thread-count))]
      (await-latch ready-latch)
      (.countDown start-latch)
      (doseq [f futures] @f)

      (is (= thread-count (count @contexts)) "Each insert should record a transaction context")
      (is (= thread-count (count (distinct (map :tx_id @contexts))))
          "Transaction IDs must be unique across concurrent inserts")
      (is (every? #(= "sql" (:origin %)) @contexts) "Origin should be sql for all contexts")
      (is (every? #(= 1 (get-in % [:metadata :document-count])) @contexts)
          "Each insert metadata should record a single document"))))

(deftest sql-update-transactions-include-flags-and-metadata
  (let [{:keys [storage contexts docs]} (make-capturing-storage)
        doc-id "user:42"
        _ (swap! docs assoc doc-id {:id doc-id :_table "user" :name "Alice" :age 30})
        sql "UPDATE public.user SET name='Alice Updated', age=31 WHERE id='user:42'"
        parsed (parse-sql sql)]
    (query/handle-update-case storage nil (ByteArrayOutputStream.) parsed)

    (is (= 1 (count @contexts)) "A single update should produce one context entry")
    (let [context (first @contexts)]
      (is (= "sql" (:origin context)))
      (is (= #{"update"} (set (:flags context))) "Update flag should be present")
      (is (= [doc-id] (get-in context [:metadata :document-ids]))
          "Metadata should list the updated document ID")
      (is (= 1 (get-in context [:metadata :document-count]))))))

(deftest sql-delete-transactions-set-delete-flag
  (let [{:keys [storage contexts docs]} (make-capturing-storage)
        doc-id "user:5"
        _ (swap! docs assoc doc-id {:id doc-id :_table "user" :name "Bob"})
        sql "DELETE FROM public.user WHERE id='user:5'"
        parsed (parse-sql sql)]
    (testing "DELETE statements wrap storage mutations in transactions"
      (query/handle-delete-case storage nil (ByteArrayOutputStream.) parsed)

      (is (= 1 (count @contexts)) "Delete should generate exactly one context entry")
      (let [context (first @contexts)]
        (is (= "sql" (:origin context)))
        (is (= #{"delete"} (set (:flags context))) "Delete flag should be present")
        (is (= doc-id (:document_id context)) "Document ID should be recorded"))

      (is (nil? (@docs doc-id)) "Document should be removed from storage"))))


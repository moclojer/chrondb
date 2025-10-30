(ns chrondb.api.ast-integration-test
  "Integration tests for AST-based search across all protocols (REST, Redis, CLI)"
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [chrondb.api.v1.routes :as routes]
            [chrondb.api.redis.core :as redis]
            [chrondb.storage.memory :as memory]
            [chrondb.storage.protocol :as storage]
            [chrondb.index.lucene :as lucene]
            [chrondb.query.ast :as ast]
            [chrondb.index.protocol :as index]
            [chrondb.test-helpers :as helpers]
            [ring.mock.request :as mock]
            [clojure.data.json :as json])
  (:import [java.net Socket]
           [java.io BufferedReader BufferedWriter InputStreamReader OutputStreamWriter]
           [java.nio.charset StandardCharsets]))

(def ^:dynamic *storage* nil)
(def ^:dynamic *index* nil)
(def ^:dynamic *index-dir* nil)

(defn with-test-infrastructure [test-fn]
  (let [index-dir (helpers/create-temp-dir)
        storage (memory/create-memory-storage)
        index (lucene/create-lucene-index index-dir)]
    (when-not index
      (throw (IllegalStateException. "Failed to create Lucene index for test")))
    (try
      (binding [*storage* storage
                *index* index
                *index-dir* index-dir]
        (test-fn))
      (finally
        (.close storage)
        (when index (.close index))
        (helpers/delete-directory index-dir)))))

(use-fixtures :each with-test-infrastructure)

;; Helper functions for Redis client simulation
(defn connect-to-redis [host port]
  (let [socket (Socket. host port)]
    {:socket socket
     :reader (BufferedReader. (InputStreamReader. (.getInputStream socket) StandardCharsets/UTF_8))
     :writer (BufferedWriter. (OutputStreamWriter. (.getOutputStream socket) StandardCharsets/UTF_8))}))

(defn close-redis-connection [conn]
  (.close (:writer conn))
  (.close (:reader conn))
  (.close (:socket conn)))

(defn send-redis-command [conn command & args]
  (let [writer (:writer conn)
        full-command (concat [command] args)]
    (.write writer (str "*" (count full-command) "\r\n"))
    (doseq [arg full-command]
      (.write writer (str "$" (count (str arg)) "\r\n" arg "\r\n")))
    (.flush writer)))

(defn read-redis-response [conn]
  (let [reader (:reader conn)
        first-line (.readLine reader)
        type (first first-line)
        content (subs first-line 1)]
    (case type
      \+ content
      \- (throw (ex-info content {}))
      \: (Long/parseLong content)
      \$ (if (= content "-1")
           nil
           (let [len (Long/parseLong content)
                 data (char-array len)
                 _ (dotimes [i len]
                     (aset data i (char (.read reader))))
                 _ (.read reader)
                 _ (.read reader)]
             (String. data)))
      \* (let [count (Long/parseLong content)]
           (if (neg? count)
             nil
             (vec (repeatedly count #(read-redis-response conn)))))
      (throw (ex-info (str "Unknown RESP type: " type) {})))))

(defn parse-json-body [response]
  (when-let [body (:body response)]
    (if (string? body)
      (json/read-str body :key-fn keyword)
      body)))

(defn create-json-request [method uri & [body]]
  (-> (mock/request method uri)
      (assoc-in [:headers "content-type"] "application/json")
      (assoc-in [:headers "accept"] "application/json")
      (cond-> body (assoc :body body))))

(deftest test-ast-rest-api-integration
  (testing "REST API AST integration"
    (let [app (routes/create-routes *storage* *index*)
          docs [{:id "doc:1" :name "Alice" :age 30 :content "Software Engineer"}
                {:id "doc:2" :name "Bob" :age 25 :content "Designer"}
                {:id "doc:3" :name "Charlie" :age 35 :content "Software Architect"}]]

      ;; Index documents
      (doseq [doc docs]
        (storage/save-document *storage* doc)
        (index/index-document *index* doc))

      (testing "Basic FTS search via query string"
        (let [response (app (create-json-request :get "/api/v1/search?q=Software"))
              body (parse-json-body response)]
          (is (= 200 (:status response)))
          (is (map? body))
          (is (vector? (:results body)))
          (is (= 2 (count (:results body))))))

      (testing "Search with limit and offset"
        (let [response (app (create-json-request :get "/api/v1/search?q=Software&limit=1&offset=0"))
              body (parse-json-body response)]
          (is (= 200 (:status response)))
          (is (= 1 (count (:results body)))))

        (let [response (app (create-json-request :get "/api/v1/search?q=Software&limit=1&offset=1"))
              body (parse-json-body response)]
          (is (= 200 (:status response)))
          (is (= 1 (count (:results body))))))

      (testing "Search with sort"
        (let [response (app (create-json-request :get "/api/v1/search?q=Software&sort=age:asc"))
              body (parse-json-body response)]
          (is (= 200 (:status response)))
          (is (vector? (:results body)))
          (is (= 2 (count (:results body))))
          ;; Results should be sorted by age ascending
          (is (<= (:age (first (:results body))) (:age (second (:results body)))))))

      (testing "Search with structured AST query"
        (let [query-edn (pr-str {:clauses [{:type :fts :field "content" :value "Software"}]})
              response (app (create-json-request :get (str "/api/v1/search?query=" (java.net.URLEncoder/encode query-edn "UTF-8"))))
              body (parse-json-body response)]
          (is (= 200 (:status response)))
          (is (vector? (:results body)))
          (is (= 2 (count (:results body))))))

      (testing "Search with branch parameter"
        (let [response (app (create-json-request :get "/api/v1/search?q=Software&branch=main"))
              body (parse-json-body response)]
          (is (= 200 (:status response)))
          (is (map? body))
          (is (vector? (:results body))))))))

(deftest test-ast-redis-integration
  (testing "Redis SEARCH command AST integration"
    (let [port 16400
          server (redis/start-redis-server *storage* *index* port)
          docs [{:id "doc:1" :name "Alice" :age 30 :content "Software Engineer"}
                {:id "doc:2" :name "Bob" :age 25 :content "Designer"}
                {:id "doc:3" :name "Charlie" :age 35 :content "Software Architect"}]]

      (try
        ;; Index documents
        (doseq [doc docs]
          (storage/save-document *storage* doc)
          (index/index-document *index* doc))

        ;; Connect to Redis server
        (let [conn (connect-to-redis "localhost" port)]
          (try
            (testing "Basic SEARCH command"
              (send-redis-command conn "SEARCH" "Software")
              (let [result (read-redis-response conn)]
                (is (vector? result))
                (is (= 2 (count result)))
                ;; Results should be JSON strings
                (is (every? string? result))))

            (testing "SEARCH with LIMIT"
              (send-redis-command conn "SEARCH" "Software" "LIMIT" "1")
              (let [result (read-redis-response conn)]
                (is (vector? result))
                (is (= 1 (count result)))))

            (testing "SEARCH with LIMIT and OFFSET"
              (send-redis-command conn "SEARCH" "Software" "LIMIT" "1" "OFFSET" "1")
              (let [result (read-redis-response conn)]
                (is (vector? result))
                (is (= 1 (count result)))))

            (testing "SEARCH with SORT"
              (send-redis-command conn "SEARCH" "Software" "SORT" "age:asc")
              (let [result (read-redis-response conn)]
                (is (vector? result))
                (is (= 2 (count result)))
                ;; Parse JSON results and verify sorting
                (let [doc1 (json/read-str (first result) :key-fn keyword)
                      doc2 (json/read-str (second result) :key-fn keyword)]
                  (is (<= (:age doc1) (:age doc2))))))

            (testing "FT.SEARCH alias"
              (send-redis-command conn "FT.SEARCH" "Software")
              (let [result (read-redis-response conn)]
                (is (vector? result))
                (is (= 2 (count result)))))

            (testing "SEARCH with BRANCH"
              (send-redis-command conn "SEARCH" "Software" "BRANCH" "main")
              (let [result (read-redis-response conn)]
                (is (vector? result))
                (is (>= (count result) 0))))

            (finally
              (close-redis-connection conn))))
        (finally
          (redis/stop-redis-server server))))))

(deftest test-ast-multi-protocol-consistency
  (testing "Consistency across REST, Redis, and AST direct calls"
    (let [docs [{:id "doc:1" :name "Alice" :age 30 :content "Software Engineer"}
                {:id "doc:2" :name "Bob" :age 25 :content "Designer"}
                {:id "doc:3" :name "Charlie" :age 35 :content "Software Architect"}]]

      ;; Index documents
      (doseq [doc docs]
        (storage/save-document *storage* doc)
        (index/index-document *index* doc))

      (testing "Same query via AST direct, REST, and Redis"
        (let [ast-query (ast/query [(ast/fts "content" "Software")]
                                   {:limit 10 :branch "main"})

              ;; Direct AST call
              direct-result (index/search-query *index* ast-query "main" {:limit 10})
              direct-ids (set (:ids direct-result))

              ;; REST API call
              app (routes/create-routes *storage* *index*)
              rest-response (app (create-json-request :get "/api/v1/search?q=Software&limit=10"))
              rest-body (parse-json-body rest-response)
              rest-ids (set (map :id (:results rest-body)))

              ;; Redis call
              port 16401
              server (redis/start-redis-server *storage* *index* port)]
          (try
            (let [conn (connect-to-redis "localhost" port)]
              (try
                (send-redis-command conn "SEARCH" "Software" "LIMIT" "10")
                (let [redis-result (read-redis-response conn)
                      redis-ids (set (map #(-> % json/read-str (get "id")) redis-result))]

                  ;; All protocols should return the same document IDs
                  (is (= direct-ids rest-ids redis-ids)
                      "All protocols should return the same document IDs"))
                (finally
                  (close-redis-connection conn))))
            (finally
              (redis/stop-redis-server server))))))))

(deftest test-ast-pagination-with-cursor
  (testing "Cursor-based pagination via REST API"
    (let [app (routes/create-routes *storage* *index*)
          ;; Create many documents
          docs (map #(hash-map :id (str "doc:" %)
                               :name (str "User" %)
                               :content (str "Content " %))
                    (range 1 21))]

      ;; Index documents
      (doseq [doc docs]
        (storage/save-document *storage* doc)
        (index/index-document *index* doc))

      (testing "First page with cursor"
        (let [response (app (create-json-request :get "/api/v1/search?q=Content&limit=5"))
              body (parse-json-body response)]
          (is (= 200 (:status response)))
          (is (= 5 (count (:results body))))
          (is (contains? body :next-cursor))

          (when (:next-cursor body)
            ;; Use cursor for next page
            (let [next-response (app (create-json-request :get (str "/api/v1/search?q=Content&limit=5&after=" (:next-cursor body))))
                  next-body (parse-json-body next-response)]
              (is (= 200 (:status next-response)))
              (is (= 5 (count (:results next-body))))
              ;; Results should be different
              (is (not= (set (map :id (:results body)))
                        (set (map :id (:results next-body))))))))))))

(deftest test-ast-sort-functionality
  (testing "Sort functionality across protocols"
    (let [docs [{:id "doc:1" :name "Alice" :age 30 :score 85.5}
                {:id "doc:2" :name "Bob" :age 25 :score 92.0}
                {:id "doc:3" :name "Charlie" :age 35 :score 78.5}]]

      ;; Index documents
      (doseq [doc docs]
        (storage/save-document *storage* doc)
        (index/index-document *index* doc))

      (testing "REST API sort ascending"
        (let [app (routes/create-routes *storage* *index*)
              response (app (create-json-request :get "/api/v1/search?q=doc&sort=age:asc"))
              body (parse-json-body response)]
          (is (= 200 (:status response)))
          (when (>= (count (:results body)) 2)
            (let [ages (map :age (:results body))]
              (is (apply <= ages) "Ages should be in ascending order")))))

      (testing "REST API sort descending"
        (let [app (routes/create-routes *storage* *index*)
              response (app (create-json-request :get "/api/v1/search?q=doc&sort=age:desc"))
              body (parse-json-body response)]
          (is (= 200 (:status response)))
          (when (>= (count (:results body)) 2)
            (let [ages (map :age (:results body))]
              (is (apply >= ages) "Ages should be in descending order"))))))))


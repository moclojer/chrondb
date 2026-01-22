(ns chrondb.api.server-test
  (:require [clojure.test :refer [deftest is testing]]
            [chrondb.api.server :as server]
            [chrondb.storage.memory :as memory]
            [chrondb.index.lucene :as lucene]
            [chrondb.test-helpers :refer [with-test-data delete-directory]]
            [clojure.data.json :as json]
            [ring.mock.request :as mock]))

(defn parse-json-body [response]
  (when-let [body (:body response)]
    (if (string? body)
      (json/read-str body :key-fn keyword)
      body)))

(deftest test-server
  (testing "Server creation"
    (let [storage (memory/create-memory-storage)
          index (lucene/create-lucene-index "test-index")
          app (server/create-app storage index)]
      (is (fn? app))
      (.close storage)
      (.close index)
      (delete-directory "test-index"))))

(deftest test-json-middleware
  (testing "JSON middleware"
    #_{:clj-kondo/ignore [:unresolved-symbol]}
    (with-test-data [storage index]
      (let [app (server/create-app storage index)
            doc {:id "test:1" :name "Test Doc" :value 42}]

        (testing "Valid JSON request"
          (let [request (-> (mock/request :post "/api/v1/save")
                            (assoc :body (json/write-str doc))
                            (assoc-in [:headers "content-type"] "application/json"))
                response (app request)]
            (is (= 200 (:status response)))
            (is (= doc (parse-json-body response)))))

        (testing "Invalid JSON request"
          (let [request (-> (mock/request :post "/api/v1/save")
                            (assoc :body "{invalid json}")
                            (assoc-in [:headers "content-type"] "application/json"))
                response (app request)]
            (is (= 400 (:status response)))
            (is (:error (parse-json-body response)))))

        (testing "Empty body request"
          (let [request (mock/request :get "/api/v1/get/nonexistent")
                response (app request)]
            (is (= 404 (:status response)))))

        (testing "JSON response"
          (let [request (mock/request :get "/")
                response (app request)]
            (is (= "application/json" (get-in response [:headers "Content-Type"])))
            (is (string? (:body response)))))))))

(deftest test-server-startup
  (testing "Server startup"
    (let [storage (memory/create-memory-storage)
          index (lucene/create-lucene-index "test-index")
          server (server/start-server storage index 0)]
      (is (not (nil? server)))
      (.stop server)
      (.close storage)
      (.close index)
      (delete-directory "test-index"))))
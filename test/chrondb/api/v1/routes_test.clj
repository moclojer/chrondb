(ns chrondb.api.v1.routes-test
  (:require [clojure.test :refer [deftest is testing]]
            [chrondb.api.v1.routes :as routes]
            [chrondb.storage.memory :as memory]
            [chrondb.storage.protocol :as storage]
            [chrondb.index.lucene :as lucene]
            [chrondb.test-helpers :as helpers]
            [ring.mock.request :as mock]
            [clojure.data.json :as json]))

(defn json-request [request]
  (-> request
      (assoc-in [:headers "content-type"] "application/json")
      (assoc-in [:headers "accept"] "application/json")))

(defn parse-json-body [response]
  (when-let [body (:body response)]
    (if (string? body)
      (json/read-str body :key-fn keyword)
      body)))

(defn create-json-request [method uri & [body]]
  (let [request (cond-> (mock/request method uri)
                  true json-request
                  body (assoc :body body)
                  body (assoc :content-type "application/json"))]
    (println "Created request:" request)
    (println "Request body type:" (type (:body request)))
    (println "Request body:" (:body request))
    request))

(deftest test-routes
  (testing "Routes"
    (let [index-dir (helpers/create-temp-dir)
          storage (memory/create-memory-storage)
          index (lucene/create-lucene-index index-dir)
          app (routes/create-routes storage index)
          doc {:id "test:1" :name "Test Doc" :value 42}]

      (testing "Save document"
        (let [request (create-json-request :post "/api/v1/save" doc)
              response (app request)]
          (println "Response status:" (:status response))
          (println "Response body:" (:body response))
          (is (= 200 (:status response)) "Response status should be 200")
          (is (= doc (parse-json-body response)) "Response body should match the saved document")))

      (testing "Get document"
        (let [_ (storage/save-document storage doc)
              response (app (create-json-request :get "/api/v1/get/test:1"))]
          (is (= 200 (:status response)) "Response status should be 200")
          (is (= doc (parse-json-body response)) "Response body should match the requested document")))

      (testing "Search documents"
        (let [response (app (create-json-request :get "/api/v1/search?q=Test"))
              body (parse-json-body response)]
          (is (= 200 (:status response)) "Response status should be 200")
          (is (map? body) "Response body should be a map")
          (is (vector? (:results body)) "Results should be a vector")))

      (testing "Delete document"
        (let [response (app (create-json-request :delete "/api/v1/delete/test:1"))]
          (is (= 200 (:status response)) "Response status should be 200")
          (is (= {:message "Document deleted"} (parse-json-body response)) "Response should confirm deletion")))

      (.close storage)
      (.close index)
      (helpers/delete-directory index-dir))))
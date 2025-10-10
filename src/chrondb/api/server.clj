(ns chrondb.api.server
  "HTTP server implementation for ChronDB.
   Provides middleware for JSON request/response handling and server startup functionality."
  (:require [chrondb.api.v1.routes :as routes]
            [ring.adapter.jetty :as jetty]
            [clojure.data.json :as json]))

(defn wrap-json-body-custom
  "Custom middleware for parsing JSON request bodies.
   Handles both string and input stream bodies.
   Parameters:
   - handler: The Ring handler to wrap
   Returns: A new handler that parses JSON request bodies"
  [handler]
  (fn [request]
    (if-let [body (:body request)]
      (try
        (let [body-str (if (string? body) body (slurp body))
              json-body (when (not-empty body-str)
                          (json/read-str body-str :key-fn keyword))]
          (handler (assoc request :body json-body)))
        (catch Exception e
          {:status 400
           :headers {"Content-Type" "application/json"}
           :body (json/write-str {:error (str "Invalid JSON: " (.getMessage e))})}))
      (handler (assoc request :body nil)))))

(defn wrap-json-response
  "Middleware for converting response bodies to JSON.
   Parameters:
   - handler: The Ring handler to wrap
   Returns: A handler that converts response bodies to JSON"
  [handler]
  (fn [request]
    (let [response (handler request)]
      (if (coll? (:body response))
        (-> response
            (update :body json/write-str)
            (assoc-in [:headers "Content-Type"] "application/json"))
        response))))

(defn create-app
  "Creates the main Ring application with all middleware.
   Parameters:
   - storage: The storage implementation
   - index: The index implementation
   Returns: A Ring handler with all middleware applied"
  [storage index]
  (-> (routes/create-routes storage index)
      wrap-json-body-custom
      wrap-json-response))

(defn start-server
  "Starts the HTTP server for ChronDB.
   Parameters:
   - storage: The storage implementation
   - index: The index implementation
   - port: The port number to listen on
   Returns: The Jetty server instance"
  [storage index port]
  (println "Starting server on port" port)
  (jetty/run-jetty (create-app storage index) {:port port :join? false}))
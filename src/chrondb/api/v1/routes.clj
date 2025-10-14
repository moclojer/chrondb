(ns chrondb.api.v1.routes
  "API routes for ChronDB v1.
   Defines the HTTP endpoints and their handlers for the REST API."
  (:require [clojure.string :as str]
            [compojure.core :refer [GET POST DELETE routes context]]
            [compojure.route :as route]
            [chrondb.storage.protocol :as storage]
            [chrondb.index.protocol :as index]
            [chrondb.api.v1 :as handlers]
            [ring.util.response :as response]))

(defn handle-save
  "Handles document save requests (default branch)."
  [storage index doc params]
  (let [result (handlers/handle-save storage index doc params)]
    (-> (response/response (:body result))
        (response/status (:status result)))))

(defn handle-get
  "Handles document retrieval requests."
  [storage id params]
  (let [result (handlers/handle-get storage id params)]
    (-> (response/response (:body result))
        (response/status (:status result)))))

(defn handle-delete
  "Handles document deletion requests."
  [storage index id params]
  (let [result (handlers/handle-delete storage index id params)]
    (-> (response/response (:body result))
        (response/status (:status result)))))

(defn handle-search
  "Handles document search requests.
   Parameters:
   - index: The index implementation
   - query: The search query string
   Returns: HTTP response with search results"
  [index query]
  ;; TODO: Allow specifying the search field via query parameter?
  ;; TODO: Allow specifying the branch via query parameter?
  (let [default-field "content"
        default-branch "main"]
    (response/response
     {:results (index/search index default-field query default-branch)})))

(defn create-routes
  "Creates the API routes for ChronDB.
   Parameters:
   - storage: The storage implementation
   - index: The index implementation
   Returns: Ring handler with all defined routes"
  [storage index]
  (routes
   (GET "/api/v1/info" []
     (let [result (handlers/handle-info storage)]
       (-> (response/response (:body result))
           (response/status (:status result)))))
   (POST "/api/v1/init" []
     (let [result (handlers/handle-init storage)]
       (-> (response/response (:body result))
           (response/status (:status result)))))
   (GET "/" [] (response/response {:message "Welcome to ChronDB"}))
   (context "/api/v1" []
     (POST "/save" {body :body}
       (handle-save storage index body nil))
     (POST "/put" {body :body params :query-params}
       (let [payload (if (map? body)
                       (cond-> body
                         (contains? params :id) (assoc :id (:id params)))
                       body)]
         (handle-save storage index payload params)))
     (GET "/get/:id" [id :as {params :query-params}]
       (handle-get storage id params))
     (GET "/get/:id/history" [id :as {params :query-params}]
       (let [result (handlers/handle-history storage id params)]
         (-> (response/response (:body result))
             (response/status (:status result)))))
     (DELETE "/delete/:id" [id :as {params :query-params}]
       (handle-delete storage index id params))
     (GET "/search" [q] (handle-search index q))
     (GET "/documents" {params :query-params}
       (let [result (handlers/handle-export-documents storage params)]
         (-> (response/response (:body result))
             (response/status (:status result)))))
     (POST "/documents/import" {body :body params :query-params}
       (let [result (handlers/handle-import-documents storage index body params)]
         (-> (response/response (:body result))
             (response/status (:status result)))))
     (POST "/verify" []
       (let [result (handlers/handle-verify storage)]
         (-> (response/response (:body result))
             (response/status (:status result)))))
     (GET "/history/:id" [id :as {params :query-params}]
       (let [result (handlers/handle-history storage id params)]
         (-> (response/response (:body result))
             (response/status (:status result)))))
     (GET "/history" {params :query-params}
       (let [{:strs [id]} params]
         (if (str/blank? id)
           (-> (response/response {:error "Query parameter 'id' is required"})
               (response/status 400))
           (let [result (handlers/handle-history storage id params)]
             (-> (response/response (:body result))
                 (response/status (:status result)))))))
     (GET "/export" {params :query-params}
       (let [result (handlers/handle-export-documents storage params)]
         (-> (response/response (:body result))
             (response/status (:status result))))))
   (POST "/api/v1/backup" {body :body}
     (let [result (handlers/handle-backup storage body)]
       (-> (response/response (:body result))
           (response/status (:status result)))))
   (POST "/api/v1/restore" {{backup-file :tempfile :as params} :params}
     (let [result (handlers/handle-restore storage backup-file params)]
       (-> (response/response (:body result))
           (response/status (:status result)))))
   (POST "/api/v1/export" {body :body}
     (let [result (handlers/handle-export storage body)]
       (-> (response/response (:body result))
           (response/status (:status result)))))
   (route/not-found (response/not-found {:error "Not Found"}))))
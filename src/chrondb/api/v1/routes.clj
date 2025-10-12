(ns chrondb.api.v1.routes
  "API routes for ChronDB v1.
   Defines the HTTP endpoints and their handlers for the REST API."
  (:require [compojure.core :refer [GET POST DELETE routes]]
            [compojure.route :as route]
            [chrondb.storage.protocol :as storage]
            [chrondb.index.protocol :as index]
            [chrondb.api.v1 :as handlers]
            [ring.util.response :as response]))

(defn handle-save
  "Handles document save requests.
   Parameters:
   - storage: The storage implementation
   - index: The index implementation
   - doc: The document to save (must be a map with an :id field)
   Returns: HTTP response with saved document or error details"
  [storage index doc]
  (try
    (if (and (map? doc) (:id doc))
      (let [saved (storage/save-document storage doc)]
        (index/index-document index saved)
        (response/response saved))
      (response/status
       (response/response {:error "Invalid document format. Must be a map with an :id field"})
       400))
    (catch Exception e
      (response/status
       (response/response {:error (.getMessage e)})
       500))))

(defn handle-get
  "Handles document retrieval requests.
   Parameters:
   - storage: The storage implementation
   - id: The document ID to retrieve
   Returns: HTTP response with document or 404 if not found"
  [storage id]
  (if-let [doc (storage/get-document storage id)]
    (response/response doc)
    (response/status
     (response/response {:error "Document not found"})
     404)))

(defn handle-delete
  "Handles document deletion requests.
   Parameters:
   - storage: The storage implementation
   - index: The index implementation
   - id: The document ID to delete
   Returns: HTTP response with success message or 404 if not found"
  [storage index id]
  (if-let [_ (storage/get-document storage id)]
    (do
      (storage/delete-document storage id)
      (index/delete-document index id)
      (response/response {:message "Document deleted"}))
    (response/status
     (response/response {:error "Document not found"})
     404)))

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
   (GET "/" [] (response/response {:message "Welcome to ChronDB"}))
   (POST "/api/v1/save" {body :body} (handle-save storage index body))
   (GET "/api/v1/get/:id" [id] (handle-get storage id))
   (DELETE "/api/v1/delete/:id" [id] (handle-delete storage index id))
   (GET "/api/v1/search" [q] (handle-search index q))
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
   (POST "/api/v1/import" {{bundle :tempfile :as params} :params}
     (let [result (handlers/handle-import storage bundle params)]
       (-> (response/response (:body result))
           (response/status (:status result)))))
   (route/not-found (response/not-found {:error "Not Found"}))))
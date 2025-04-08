(ns chrondb.api.v1
  (:require [chrondb.storage.protocol :as storage]
            [chrondb.index.protocol :as index]))

(defn handle-get [storage id]
  (if-let [doc (storage/get-document storage id)]
    {:status 200
     :body doc}
    {:status 404
     :body {:error "Document not found"}}))

(defn handle-save [storage index doc]
  (let [saved (storage/save-document storage doc)]
    (index/index-document index saved)
    {:status 200
     :body saved}))

(defn handle-delete [storage id]
  (if (storage/delete-document storage id)
    {:status 200
     :body {:message "Document deleted"}}
    {:status 404
     :body {:error "Document not found"}}))

(defn handle-search [index query]
  {:status 200
   :body (index/search index "name" query "main")})
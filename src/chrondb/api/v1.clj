(ns chrondb.api.v1
  (:require [chrondb.storage.protocol :as storage]
            [chrondb.index.protocol :as index]
            [chrondb.backup.core :as backup]
            [clojure.java.io :as io])
  (:import [java.io File]))

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

(defn handle-backup [storage {:keys [output format refs]}]
  (try
    (if output
      {:status 200
       :body (backup/create-full-backup storage {:output-path output
                                                 :format (keyword (or format "tar.gz"))
                                                 :refs refs})}
      {:status 400 :body {:error "output is required"}})
    (catch Exception e
      {:status 500 :body {:error (.getMessage e)}})))

(defn- temp-file
  [prefix suffix]
  (doto (File/createTempFile prefix suffix)
    (.deleteOnExit)))

(defn handle-restore [storage temp-upload {:keys [filename format]}]
  (try
    (if temp-upload
      (let [tmp (temp-file "chrondb-restore" (str "-" (or filename "backup")))
            _ (io/copy temp-upload tmp)
            result (backup/restore-backup storage {:input-path (.getAbsolutePath tmp)
                                                   :format (keyword (or format "tar.gz"))})]
        {:status 200 :body result})
      {:status 400 :body {:error "file upload is required"}})
    (catch Exception e
      {:status 500 :body {:error (.getMessage e)}})))

(defn handle-export [storage {:keys [output refs format] :or {format "bundle"}}]
  (try
    (if output
      {:status 200
       :body (backup/export-snapshot storage {:output output
                                              :refs refs
                                              :format (keyword format)})}
      {:status 400 :body {:error "output is required"}})
    (catch Exception e
      {:status 500 :body {:error (.getMessage e)}})))

(defn handle-import [storage temp-upload {:keys [filename verify]}]
  (try
    (if temp-upload
      (let [tmp (temp-file "chrondb-import" (str "-" (or filename "bundle")))
            _ (io/copy temp-upload tmp)
            result (backup/import-snapshot storage {:input (.getAbsolutePath tmp)
                                                    :verify (not= "false" verify)})]
        {:status 200 :body result})
      {:status 400 :body {:error "file upload is required"}})
    (catch Exception e
      {:status 500 :body {:error (.getMessage e)}})))
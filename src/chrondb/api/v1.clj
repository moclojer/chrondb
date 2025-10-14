(ns chrondb.api.v1
  (:require [chrondb.config :as config]
            [chrondb.storage.protocol :as storage]
            [chrondb.index.protocol :as index]
            [chrondb.backup.core :as backup]
            [clojure.java.io :as io]
            [clojure.string :as str])
  (:import [java.io File]))

(defn handle-get
  "Retrieve a document, optionally from a specific branch."
  [storage id {:keys [branch]}]
  (let [branch-name (when (and branch (not (str/blank? branch))) branch)
        doc (if branch-name
              (storage/get-document storage id branch-name)
              (storage/get-document storage id))]
    (if doc
      {:status 200 :body doc}
      {:status 404
       :body {:error "Document not found"
              :id id
              :branch branch-name}})))

(defn handle-save
  "Persist a document optionally targeting a branch."
  ([storage index doc]
   (handle-save storage index doc nil))
  ([storage index doc {:keys [branch]}]
   (let [branch-name (when (and branch (not (str/blank? branch))) branch)
         saved (if branch-name
                 (storage/save-document storage doc branch-name)
                 (storage/save-document storage doc))]
     (index/index-document index saved)
     {:status 200
      :body saved})))

(defn handle-delete
  "Delete document optionally from a specific branch."
  ([storage index id]
   (handle-delete storage index id nil))
  ([storage index id {:keys [branch]}]
   (let [branch-name (when (and branch (not (str/blank? branch))) branch)
         doc (if branch-name
               (storage/get-document storage id branch-name)
               (storage/get-document storage id))]
     (if doc
       (do
         (if branch-name
           (storage/delete-document storage id branch-name)
           (storage/delete-document storage id))
         (index/delete-document index id)
         {:status 200 :body (cond-> {:message "Document deleted"}
                              branch-name (assoc :id id :branch branch-name))})
       {:status 404 :body {:error "Document not found"
                           :id id
                           :branch branch-name}}))))

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

(defn handle-info
  "Return basic information about the running ChronDB instance."
  [storage]
  (let [cfg (config/load-config)
        default-branch (get-in cfg [:git :default-branch])
        data-dir (get-in cfg [:storage :data-dir])]
    {:status 200
     :body {:default-branch default-branch
            :data-directory data-dir
            :repository-open? (boolean (:repository storage))}}))

(defn handle-init
  "Initialize client connection by returning repository metadata."
  [storage]
  (handle-info storage))

(defn- parse-int-safe [value]
  (when (and value (not (str/blank? value)))
    (try
      (Integer/parseInt (str value))
      (catch Exception _ nil))))

(defn handle-history
  "Return the history for a given document id.
   Accepts optional :branch, :limit and :since parameters."
  [storage id {:keys [branch limit since]}]
  (try
    (let [branch-name (when (and branch (not (str/blank? branch))) branch)
          limit-val (parse-int-safe limit)
          history (vec (or (storage/get-document-history storage id branch-name) []))
          filtered (if (and since (not (str/blank? since)))
                     (->> history
                          (drop-while #(not= (str (:commit-id %)) (str since)))
                          rest
                          vec)
                     history)
          limited (if (and limit-val (pos? limit-val))
                    (vec (take limit-val filtered))
                    filtered)
          status (if (seq history) 200 404)]
      {:status status
       :body {:id id
              :branch branch-name
              :since since
              :count (count limited)
              :truncated? (and limit-val (> (count filtered) (count limited)))
              :history limited}})
    (catch Exception e
      {:status 500 :body {:error (.getMessage e)}})))

(defn handle-export-documents
  "Return documents matching prefix for export purposes."
  [storage {:keys [prefix branch limit]}]
  (let [branch-name (when (and branch (not (str/blank? branch))) branch)
        docs (vec (or (storage/get-documents-by-prefix storage (or prefix "") branch-name) []))
        limit-val (parse-int-safe limit)
        selected (if (and limit-val (pos? limit-val))
                   (vec (take limit-val docs))
                   docs)]
    {:status 200
     :body {:prefix prefix
            :branch branch-name
            :count (count selected)
            :documents selected}}))

(defn handle-import-documents
  "Import documents into the repository."
  [storage index body {:keys [branch]}]
  (let [docs (cond
               (map? body)
               (or (:documents body)
                   (when (:id body)
                     [body]))

               (sequential? body) body
               :else nil)]
    (cond
      (not (seq docs))
      {:status 400 :body {:error "Request body must contain documents"}}

      (some #(not (contains? % :id)) docs)
      {:status 400 :body {:error "All documents must include :id"}}

      :else
      (try
        (let [branch-name (when (and branch (not (str/blank? branch))) branch)
              saved (mapv (fn [doc]
                            (let [result (if branch-name
                                           (storage/save-document storage doc branch-name)
                                           (storage/save-document storage doc))]
                              (index/index-document index result)
                              result))
                          docs)]
          {:status 200
           :body {:imported (count saved)
                  :documents saved}})
        (catch Exception e
          {:status 500 :body {:error (.getMessage e)}})))))

(defn handle-verify
  "Perform repository verification by exporting a temporary bundle."
  [storage]
  (let [tmp (temp-file "chrondb-verify" ".bundle")]
    (try
      (let [result (backup/export-snapshot storage {:output (.getAbsolutePath tmp)
                                                    :verify true
                                                    :include-manifest false})]
        {:status 200
         :body {:status "ok"
                :checksum (:checksum result)
                :refs (:refs result)}})
      (catch Exception e
        {:status 500
         :body {:error (.getMessage e)}})
      (finally
        (.delete tmp)))))
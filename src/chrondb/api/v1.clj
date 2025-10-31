(ns chrondb.api.v1
  (:require [chrondb.config :as config]
            [chrondb.storage.protocol :as storage]
            [chrondb.index.protocol :as index]
            [chrondb.backup.core :as backup]
            [chrondb.transaction.core :as tx]
            [clojure.java.io :as io]
            [clojure.string :as str])
  (:import [java.io File]))

(defn- request-header
  "Fetch a header value from a Ring request, ignoring case."
  [request header-name]
  (let [headers (:headers request)
        lowercase (some-> header-name (str/lower-case))
        uppercase (some-> header-name (str/upper-case))]
    (when headers
      (or (get headers header-name)
          (get headers lowercase)
          (get headers uppercase)))))

(defn- sanitize-branch [branch]
  (when (and branch (not (str/blank? branch))) branch))

(defn- normalize-flags [flags]
  (letfn [(spread [value]
            (cond
              (nil? value) []
              (and (coll? value) (not (string? value))) (mapcat spread value)
              :else [value]))]
    (->> (spread flags)
         (keep identity)
         (map str)
         (remove str/blank?)
         distinct
         vec
         not-empty)))

(defn- build-transaction-options
  "Constructs transaction options based on request context and overrides."
  [{:keys [request branch flags metadata origin user]}]
  (let [origin-value (or origin
                         (some-> (request-header request "x-chrondb-origin") str/lower-case)
                         "rest")
        user-value (or user (request-header request "x-chrondb-user"))
        request-id (or (request-header request "x-request-id")
                       (request-header request "x-correlation-id"))
        remote-addr (:remote-addr request)
        meta-base (cond-> {}
                    (sanitize-branch branch) (assoc :branch (sanitize-branch branch))
                    request-id (assoc :request-id request-id)
                    remote-addr (assoc :remote-addr remote-addr))
        merged-meta (merge meta-base (or metadata {}))
        normalized-flags (normalize-flags flags)]
    (cond-> {:origin origin-value}
      (and user-value (not (str/blank? user-value))) (assoc :user user-value)
      (seq merged-meta) (assoc :metadata merged-meta)
      normalized-flags (assoc :flags normalized-flags))))

(defn handle-get
  "Retrieve a document, optionally from a specific branch."
  ([storage id]
   (handle-get storage id nil))
  ([storage id {:keys [branch params]}]
   (let [params (or params {})
         branch-name (sanitize-branch (or branch (:branch params)))
         doc (if branch-name
               (storage/get-document storage id branch-name)
               (storage/get-document storage id))]
     (if doc
       {:status 200 :body doc}
       {:status 404
        :body {:error "Document not found"
               :id id
               :branch branch-name}}))))

(defn handle-save
  "Persist a document optionally targeting a branch."
  ([storage index doc]
   (handle-save storage index doc nil))
  ([storage index doc {:keys [branch params request flags metadata origin user] :as ctx}]
   (let [params (or params {})
         branch-name (sanitize-branch (or branch (:branch params)))
         meta-base (or metadata {})
         meta-extra (cond-> meta-base
                      (:id doc) (assoc :document-id (:id doc)))
         tx-opts (build-transaction-options {:request request
                                             :branch branch-name
                                             :flags flags
                                             :metadata meta-extra
                                             :origin origin
                                             :user user})
         saved (tx/with-transaction [storage tx-opts]
                 (let [result (if branch-name
                                (storage/save-document storage doc branch-name)
                                (storage/save-document storage doc))]
                   (index/index-document index result)
                   result))]
     {:status 200
      :body saved})))

(defn handle-delete
  "Delete document optionally from a specific branch."
  ([storage index id]
   (handle-delete storage index id nil))
  ([storage index id {:keys [branch params request flags metadata origin user] :as ctx}]
   (let [params (or params {})
         branch-name (sanitize-branch (or branch (:branch params)))
         meta-base (or metadata {})
         meta-extra (assoc meta-base :document-id id)
         operation-flags (concat ["delete"] (or flags []))
         tx-opts (build-transaction-options {:request request
                                             :branch branch-name
                                             :flags operation-flags
                                             :metadata meta-extra
                                             :origin origin
                                             :user user})]
     (tx/with-transaction [storage tx-opts]
       (let [doc (if branch-name
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
                               :branch branch-name}}))))))

(defn handle-search
  "Unified search handler. Accepts either a raw query string (`:q`) or a structured query map (`:query`)."
  [index params]
  (let [{:keys [q query branch limit offset sort]} params
        branch-name (or (not-empty branch) "main")
        limit (some-> limit str Integer/parseInt)
        offset (some-> offset str Integer/parseInt)
        sort (cond
               (string? sort) (map (fn [spec]
                                     (let [[field dir] (-> spec str/trim (str/split #":"))]
                                       {:field (str/trim field)
                                        :direction (if (= "desc" (str/lower-case (or dir ""))) :desc :asc)}))
                                   (str/split sort #","))
               (sequential? sort) sort
               :else nil)
        base-query (cond
                     (map? query) query
                     (string? q) {:clauses [{:type :fts :field "content" :value q :analyzer :fts}]}
                     :else {:clauses []})
        opts (cond-> {}
               limit (assoc :limit limit)
               offset (assoc :offset offset)
               sort (assoc :sort sort))
        result (index/search-query index base-query branch-name opts)]
    {:status 200
     :body result}))

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

(defn handle-restore
  [storage temp-upload {:keys [filename format request flags metadata origin user branch params] :as ctx}]
  (try
    (if temp-upload
      (let [tmp (temp-file "chrondb-restore" (str "-" (or filename "backup")))
            _ (io/copy temp-upload tmp)
            operation-flags (concat ["rollback" "bulk-load"] (or flags []))
            params-map (or params {})
            branch-name (sanitize-branch (or branch (:branch params-map)))
            meta-base (merge {:filename filename
                              :format (or format "tar.gz")}
                             (or metadata {}))
            tx-opts (build-transaction-options {:request request
                                                :branch branch-name
                                                :flags operation-flags
                                                :metadata meta-base
                                                :origin origin
                                                :user user})]
        (tx/with-transaction [storage tx-opts]
          (let [result (backup/restore-backup storage {:input-path (.getAbsolutePath tmp)
                                                       :format (keyword (or format "tar.gz"))})]
            {:status 200 :body result})))
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

(defn handle-import
  [storage temp-upload {:keys [filename verify request flags metadata origin user branch params] :as ctx}]
  (try
    (if temp-upload
      (let [tmp (temp-file "chrondb-import" (str "-" (or filename "bundle")))
            _ (io/copy temp-upload tmp)
            operation-flags (concat ["bulk-load" "migration"] (or flags []))
            params-map (or params {})
            branch-name (sanitize-branch (or branch (:branch params-map)))
            meta-base (merge {:filename filename
                              :verify (not= "false" verify)}
                             (or metadata {}))
            tx-opts (build-transaction-options {:request request
                                                :branch branch-name
                                                :flags operation-flags
                                                :metadata meta-base
                                                :origin origin
                                                :user user})]
        (tx/with-transaction [storage tx-opts]
          (let [result (backup/import-snapshot storage {:input (.getAbsolutePath tmp)
                                                        :verify (not= "false" verify)})]
            {:status 200 :body result})))
      {:status 400 :body {:error "file upload is required"}})
    (catch Exception e
      {:status 500 :body {:error (.getMessage e)}})))

(defn handle-info
  "Return basic information about the running ChronDB instance."
  ([storage]
   (handle-info storage nil))
  ([storage _]
   (let [cfg (config/load-config)
         default-branch (get-in cfg [:git :default-branch])
         data-dir (get-in cfg [:storage :data-dir])]
     {:status 200
      :body {:default-branch default-branch
             :data-directory data-dir
             :repository-open? (boolean (:repository storage))}})))

(defn handle-init
  "Initialize client connection by returning repository metadata."
  ([storage]
   (handle-init storage nil))
  ([storage context]
   (handle-info storage context)))

(defn- parse-int-safe [value]
  (when (and value (not (str/blank? value)))
    (try
      (Integer/parseInt (str value))
      (catch Exception _ nil))))

(defn handle-history
  "Return the history for a given document id.
   Accepts optional :branch, :limit and :since parameters."
  ([storage id]
   (handle-history storage id nil))
  ([storage id {:keys [branch limit since params]}]
   (try
     (let [params (or params {})
           branch-name (sanitize-branch (or branch (:branch params)))
           limit-val (parse-int-safe (or limit (:limit params)))
           since-val (or since (:since params))
           history (vec (or (storage/get-document-history storage id branch-name) []))
           filtered (if (and since-val (not (str/blank? since-val)))
                      (->> history
                           (drop-while #(not= (str (:commit-id %)) (str since-val)))
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
              :since since-val
              :count (count limited)
              :truncated? (and limit-val (> (count filtered) (count limited)))
              :history limited}})
     (catch Exception e
       {:status 500 :body {:error (.getMessage e)}}))))

(defn handle-export-documents
  "Return documents matching prefix for export purposes."
  [storage {:keys [prefix branch limit params]}]
  (let [params (or params {})
        prefix-value (or prefix (:prefix params) "")
        branch-name (sanitize-branch (or branch (:branch params)))
        docs (vec (or (storage/get-documents-by-prefix storage prefix-value branch-name) []))
        limit-val (parse-int-safe (or limit (:limit params)))
        selected (if (and limit-val (pos? limit-val))
                   (vec (take limit-val docs))
                   docs)]
    {:status 200
     :body {:prefix prefix-value
            :branch branch-name
            :count (count selected)
            :documents selected}}))

(defn handle-import-documents
  "Import documents into the repository."
  ([storage index body]
   (handle-import-documents storage index body nil))
  ([storage index body {:keys [branch params request flags metadata origin user]}]
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
         (let [params (or params {})
               branch-name (sanitize-branch (or branch (:branch params)))
               doc-ids (->> docs (map :id) (remove nil?) (take 20) vec)
               meta-base (merge {:document-count (count docs)} (or metadata {}))
               meta-extra (cond-> meta-base
                             (seq doc-ids) (assoc :document-ids doc-ids))
               operation-flags (concat ["bulk-load"] (or flags []))
               tx-opts (build-transaction-options {:request request
                                                   :branch branch-name
                                                   :flags operation-flags
                                                   :metadata meta-extra
                                                   :origin origin
                                                   :user user})]
           (tx/with-transaction [storage tx-opts]
             (let [saved (mapv (fn [doc]
                                 (let [result (if branch-name
                                                (storage/save-document storage doc branch-name)
                                                (storage/save-document storage doc))]
                                   (index/index-document index result)
                                   result))
                               docs)]
               {:status 200
                :body {:imported (count saved)
                       :documents saved}})))
         (catch Exception e
           {:status 500 :body {:error (.getMessage e)}}))))))

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
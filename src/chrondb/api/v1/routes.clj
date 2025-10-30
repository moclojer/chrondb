(ns chrondb.api.v1.routes
  "API routes for ChronDB v1.
   Defines the HTTP endpoints and their handlers for the REST API."
  (:require [clojure.string :as str]
            [clojure.edn :as edn]
            [clojure.walk :as walk]
            [compojure.core :refer [GET POST DELETE routes context]]
            [compojure.route :as route]
            [chrondb.index.protocol :as index]
            [chrondb.storage.protocol :as storage]
            [chrondb.api.v1 :as handlers]
            [chrondb.query.ast :as ast]
            [ring.util.codec :as codec]
            [ring.util.response :as response]
            [chrondb.util.logging :as log]))

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

(defn parse-sort-param
  "Parses sort parameter from string format 'field:asc,field2:desc' to AST sort descriptors."
  [sort-param]
  (when sort-param
    (->> (str/split sort-param #",")
         (map (fn [entry]
                (let [[field dir] (-> entry str/trim (str/split #":"))
                      direction (if (= "desc" (str/lower-case (or dir ""))) :desc :asc)]
                  (ast/sort-by field direction))))
         (vec))))

(defn parse-structured-query
  "Parses structured query from EDN string or map."
  [query-param]
  (when query-param
    (try
      (cond
        (map? query-param) query-param
        (string? query-param) (edn/read-string query-param)
        :else nil)
      (catch Exception _
        nil))))

(defn parse-search-after
  "Parses searchAfter cursor from base64 encoded string.
   Returns a map with :doc and :score that can be converted to ScoreDoc later."
  [after-param]
  (when after-param
    (try
      (let [decoder (java.util.Base64/getDecoder)
            decoded (String. (.decode decoder (.getBytes after-param "UTF-8")) "UTF-8")
            parsed (edn/read-string decoded)]
        (when (and (map? parsed)
                   (contains? parsed :doc)
                   (contains? parsed :score))
          parsed))
      (catch Exception _
        nil))))

(defn handle-search
  "Handles document search requests via the Lucene-backed query engine.
   Supports:
   - Query string (`q`) - converted to FTS clause
   - Structured query (`query`) - AST query map
   - Branch selection (`branch`)
   - Sorting (`sort=field:asc,field2:desc`)
   - Pagination (`limit`, `offset`)
   - Cursor-based pagination (`after` or `cursor`) - base64 encoded ScoreDoc
   - Feature flag (`use_ast=true`) - enables AST query processing"
  [storage index request]
  (let [params-map (or (:query-params request)
                       (some-> (:query-string request)
                               codec/form-decode
                               (walk/keywordize-keys)))
        params (walk/keywordize-keys (or params-map {}))
        {:keys [q query branch limit offset sort after cursor]} params
        branch-name (or (not-empty branch) "main")
        ;; Parse sort descriptors
        sort-descriptors (parse-sort-param sort)

        ;; Parse searchAfter cursor
        search-after-cursor (or (parse-search-after after)
                               (parse-search-after cursor))

        ;; Parse limit and offset
        limit-int (when limit (try (Integer/parseInt limit) (catch Exception _ nil)))
        offset-int (when offset (try (Integer/parseInt offset) (catch Exception _ nil)))

        ;; Build AST query
        ast-query (cond
                   ;; Use structured query if provided
                   (parse-structured-query query)
                   (let [structured (parse-structured-query query)]
                     (ast/query (:clauses structured)
                               {:sort sort-descriptors
                                :limit limit-int
                                :offset offset-int
                                :branch branch-name
                                :after search-after-cursor}))

                   ;; Convert simple query string to AST
                   (some? q)
                   (let [fts-clause (ast/fts "content" q)]
                     (ast/query [fts-clause]
                               {:sort sort-descriptors
                                :limit limit-int
                                :offset offset-int
                                :branch branch-name
                                :after search-after-cursor}))

                   ;; Empty query
                   :else
                   (ast/query []
                             {:sort sort-descriptors
                              :limit limit-int
                              :offset offset-int
                              :branch branch-name
                              :after search-after-cursor}))

        _ (log/log-info (str "AST query built: " ast-query))

        ;; Build options map
        opts (cond-> {}
               limit-int (assoc :limit limit-int)
               offset-int (assoc :offset offset-int)
               (seq sort-descriptors) (assoc :sort sort-descriptors)
               search-after-cursor (assoc :after search-after-cursor))

        ;; Execute query
        result (if (seq (:clauses ast-query))
                (index/search-query index ast-query branch-name opts)
                {:ids [] :total 0 :limit (or limit-int 100) :offset (or offset-int 0)})

        ;; Fetch full documents from storage using IDs
        results (if (seq (:ids result))
                 (filter some? (map #(storage/get-document storage % branch-name) (:ids result)))
                 [])

        ;; Prepare response with next cursor if available
        response-body (cond-> {:results (vec results)
                               :total (:total result)
                               :limit (:limit result)
                               :offset (:offset result)}
                        (:next-cursor result)
                        (assoc :next-cursor
                               (let [encoder (java.util.Base64/getEncoder)
                                     cursor-data {:doc (.-doc ^org.apache.lucene.search.ScoreDoc (:next-cursor result))
                                                  :score (.-score ^org.apache.lucene.search.ScoreDoc (:next-cursor result))}
                                     encoded (String. (.encode encoder (.getBytes (pr-str cursor-data) "UTF-8")) "UTF-8")]
                                 encoded)))]
    (response/response response-body)))

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
     (GET "/search" request
       (handle-search storage index request))
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
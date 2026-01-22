(ns chrondb.api.v1.routes
  "API routes for ChronDB v1.
   Defines the HTTP endpoints and their handlers for the REST API."
  (:require [clojure.string :as str]
            [clojure.edn :as edn]
            [clojure.walk :as walk]
            [compojure.core :refer [GET POST PUT DELETE routes context]]
            [compojure.route :as route]
            [chrondb.index.protocol :as index]
            [chrondb.storage.protocol :as storage]
            [chrondb.api.v1 :as handlers]
            [chrondb.query.ast :as ast]
            [ring.util.codec :as codec]
            [ring.util.response :as response]
            [chrondb.util.logging :as log]
            [chrondb.observability.metrics :as metrics]
            [chrondb.observability.health :as health]))

(defn- keywordize-params [params]
  (-> params (or {}) (walk/keywordize-keys)))

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

(defn- parse-flags
  [value]
  (cond
    (nil? value) []
    (vector? value) (->> value (map str) (map str/trim) (remove str/blank?) vec)
    (and (coll? value) (not (string? value))) (->> value (mapcat parse-flags) vec)
    (string? value) (->> (str/split value #",") (map str/trim) (remove str/blank?) vec)
    :else [(str value)]))

(defn- build-request-context
  ([request]
   (build-request-context request {}))
  ([{:keys [headers query-params request-method uri remote-addr] :as request} overrides]
   (let [params (keywordize-params query-params)
         override-params (keywordize-params (:params overrides))
         merged-params (merge params override-params)
         branch-override (some-> (:branch overrides) str/trim not-empty)
         branch-param (some-> (:branch merged-params) str/trim not-empty)
         branch (or branch-override branch-param)
         origin-override (some-> (:origin overrides) str/trim str/lower-case not-empty)
         origin-header (some-> (get headers "x-chrondb-origin") str/trim str/lower-case not-empty)
         origin-param (some-> (:origin merged-params) str/trim str/lower-case not-empty)
         origin (or origin-override origin-header origin-param "rest")
         user-override (some-> (:user overrides) str/trim not-empty)
         user-header (some-> (get headers "x-chrondb-user") str/trim not-empty)
         user-param (some-> (:user merged-params) str/trim not-empty)
         user (or user-override user-header user-param)
         flags-header (parse-flags (get headers "x-chrondb-flags"))
         flags-param (parse-flags (:flags merged-params))
         flags-override (parse-flags (:flags overrides))
         flags (->> [flags-header flags-param flags-override]
                    (mapcat identity)
                    (remove str/blank?)
                    distinct
                    vec)
         metadata-param (let [m (:metadata merged-params)]
                          (cond
                            (map? m) m
                            (string? m) (try
                                          (edn/read-string m)
                                          (catch Exception _ nil))
                            :else nil))
         metadata-override (when (map? (:metadata overrides))
                             (:metadata overrides))
         request-id (some-> (or (get headers "x-request-id")
                                (:request-id merged-params)
                                (:request-id overrides))
                            str/trim not-empty)
         base-metadata {:http {:method (some-> request-method name)
                               :path uri
                               :remote-addr remote-addr}}
         combined-metadata (-> base-metadata
                               (merge (or metadata-param {}))
                               (merge (or metadata-override {}))
                               (cond-> request-id (assoc :request-id request-id)))
         params* (-> merged-params
                     (cond-> branch (assoc :branch branch))
                     (cond-> user (assoc :user user))
                     (cond-> (seq flags) (assoc :flags flags))
                     (cond-> (seq combined-metadata) (assoc :metadata combined-metadata)))
         context (-> overrides
                     (assoc :request request
                            :params params*)
                     (cond-> branch (assoc :branch branch))
                     (assoc :origin origin)
                     (cond-> user (assoc :user user))
                     (cond-> (seq flags) (assoc :flags flags))
                     (cond-> (seq combined-metadata) (assoc :metadata combined-metadata)))]
     context)))

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
   - opts: Optional map with :health-checker and :wal
   Returns: Ring handler with all defined routes"
  [storage index & [{:keys [health-checker]}]]
  (routes
   ;; Health and observability endpoints (outside /api/v1 for standard locations)
   (GET "/health" []
     (if health-checker
       (let [result (health/run-all-checks health-checker)]
         (-> (response/response result)
             (response/status (health/status->http-code (:status result)))))
       (response/response {:status :healthy :message "Health checks not configured"})))

   (GET "/healthz" []
     ;; Liveness probe - just check if process is alive
     (response/response {:status :healthy :timestamp (.toString (java.time.Instant/now))}))

   (GET "/readyz" []
     ;; Readiness probe - use health checker if available
     (if health-checker
       (let [result (health/run-all-checks health-checker)
             ready? (#{:healthy :degraded} (:status result))]
         (-> (response/response {:status (if ready? :ready :not-ready) :checks (:checks result)})
             (response/status (if ready? 200 503))))
       (response/response {:status :ready})))

   (GET "/startupz" []
     ;; Startup probe - same as readiness for now
     (if health-checker
       (let [result (health/run-all-checks health-checker)
             started? (#{:healthy :degraded} (:status result))]
         (-> (response/response {:status (if started? :started :starting) :checks (:checks result)})
             (response/status (if started? 200 503))))
       (response/response {:status :started})))

   (GET "/metrics" []
     {:status 200
      :headers {"Content-Type" "text/plain; charset=utf-8"}
      :body (metrics/export-all-metrics)})

   (GET "/api/v1/info" []
     (let [result (handlers/handle-info storage)]
       (-> (response/response (:body result))
           (response/status (:status result)))))
   (POST "/api/v1/init" []
     (let [result (handlers/handle-init storage)]
       (-> (response/response (:body result))
           (response/status (:status result)))))
   (GET "/" [] (response/response {:message "Welcome to ChronDB"}))
   (context "/api/v1/schemas/validation" []
     (GET "/" {:as request}
       (let [result (handlers/handle-list-validation-schemas storage (build-request-context request {:metadata {:endpoint "/api/v1/schemas/validation"}}))]
         (-> (response/response (:body result))
             (response/status (:status result)))))
     (GET "/:namespace" [namespace :as request]
       (let [result (handlers/handle-get-validation-schema storage namespace (build-request-context request {:metadata {:endpoint "/api/v1/schemas/validation/:namespace" :namespace namespace}}))]
         (-> (response/response (:body result))
             (response/status (:status result)))))
     (PUT "/:namespace" [namespace :as {:keys [body] :as request}]
       (let [result (handlers/handle-save-validation-schema storage namespace body (build-request-context request {:metadata {:endpoint "/api/v1/schemas/validation/:namespace" :namespace namespace}}))]
         (-> (response/response (:body result))
             (response/status (:status result)))))
     (DELETE "/:namespace" [namespace :as request]
       (let [result (handlers/handle-delete-validation-schema storage namespace (build-request-context request {:metadata {:endpoint "/api/v1/schemas/validation/:namespace" :namespace namespace}}))]
         (-> (response/response (:body result))
             (response/status (:status result)))))
     (GET "/:namespace/history" [namespace :as request]
       (let [result (handlers/handle-validation-schema-history storage namespace (build-request-context request {:metadata {:endpoint "/api/v1/schemas/validation/:namespace/history" :namespace namespace}}))]
         (-> (response/response (:body result))
             (response/status (:status result)))))
     (POST "/:namespace/validate" [namespace :as {:keys [body] :as request}]
       (let [result (handlers/handle-validate-document storage namespace body (build-request-context request {:metadata {:endpoint "/api/v1/schemas/validation/:namespace/validate" :namespace namespace}}))]
         (-> (response/response (:body result))
             (response/status (:status result))))))
   (context "/api/v1" []
     (POST "/save" {:keys [body] :as request}
       (handle-save storage index body (build-request-context request {:metadata {:endpoint "/api/v1/save"}})))
     (POST "/put" {:keys [body] :as request}
       (let [context (build-request-context request {:metadata {:endpoint "/api/v1/put"}})
             params (:params context)
             payload (if (map? body)
                       (cond-> body
                         (:id params) (assoc :id (:id params)))
                       body)]
         (handle-save storage index payload context)))
     (GET "/get/:id" [id :as request]
       (handle-get storage id (build-request-context request {:metadata {:endpoint "/api/v1/get" :document-id id}})))
     (GET "/get/:id/history" [id :as request]
       (let [result (handlers/handle-history storage id (build-request-context request {:metadata {:endpoint "/api/v1/get/:id/history"}}))]
         (-> (response/response (:body result))
             (response/status (:status result)))))
     (DELETE "/delete/:id" [id :as request]
       (handle-delete storage index id (build-request-context request {:metadata {:endpoint "/api/v1/delete" :document-id id}})))
     (GET "/search" request
       (handle-search storage index request))
     (GET "/documents" {:as request}
       (let [result (handlers/handle-export-documents storage (build-request-context request {:metadata {:endpoint "/api/v1/documents"}}))]
         (-> (response/response (:body result))
             (response/status (:status result)))))
     (POST "/documents/import" {:keys [body] :as request}
       (let [result (handlers/handle-import-documents storage index body (build-request-context request {:metadata {:endpoint "/api/v1/documents/import"}}))]
         (-> (response/response (:body result))
             (response/status (:status result)))))
     (POST "/verify" []
       (let [result (handlers/handle-verify storage)]
         (-> (response/response (:body result))
             (response/status (:status result)))))
     (GET "/history/:id" [id :as request]
       (let [result (handlers/handle-history storage id (build-request-context request {:metadata {:endpoint "/api/v1/history/:id"}}))]
         (-> (response/response (:body result))
             (response/status (:status result)))))
     (GET "/history" {:as request}
       (let [context (build-request-context request {:metadata {:endpoint "/api/v1/history"}})
             params (:params context)
             id (or (:id params) (get params :id) (get-in request [:query-params "id"]))]
         (if (str/blank? id)
           (-> (response/response {:error "Query parameter 'id' is required"})
               (response/status 400))
           (let [result (handlers/handle-history storage id context)]
             (-> (response/response (:body result))
                 (response/status (:status result)))))))
     (GET "/export" {:as request}
       (let [result (handlers/handle-export-documents storage (build-request-context request {:metadata {:endpoint "/api/v1/export"}}))]
         (-> (response/response (:body result))
             (response/status (:status result))))))
   (POST "/api/v1/backup" {:keys [body]}
     (let [result (handlers/handle-backup storage body)]
       (-> (response/response (:body result))
           (response/status (:status result)))))
   (POST "/api/v1/restore" {:keys [params] :as request}
     (let [context (build-request-context request {:metadata {:endpoint "/api/v1/restore"}})
           params* (keywordize-params params)
           backup-file (:tempfile params)
           result (handlers/handle-restore storage backup-file (merge params* context))]
       (-> (response/response (:body result))
           (response/status (:status result)))))
   (POST "/api/v1/export" {:keys [body]}
     (let [result (handlers/handle-export storage body)]
       (-> (response/response (:body result))
           (response/status (:status result)))))
   (route/not-found (response/not-found {:error "Not Found"}))))
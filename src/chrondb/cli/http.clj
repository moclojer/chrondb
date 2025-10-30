(ns chrondb.cli.http
  "HTTP client helpers used by the ChronDB CLI."
  (:require [clj-http.client :as http]
            [clojure.data.json :as json]
            [clojure.string :as str]))

(def default-config
  {:scheme "http"
   :host "localhost"
   :port 3000
   :timeout 5000})

(defn- sanitize-base-url [base]
  (when (seq base)
    (-> base
        (str/replace #"/+$" ""))))

(defn- build-base-url [{:keys [base-url scheme host port]}]
  (if base-url
    (sanitize-base-url base-url)
    (let [scheme (or scheme (:scheme default-config))
          host (or host (:host default-config))
          port (or port (:port default-config))]
      (str scheme "://" host ":" port))))

(defn merge-config
  "Merge user supplied options with defaults."
  [cfg]
  (merge default-config (select-keys cfg [:scheme :host :port :timeout :token :base-url])))

(defn- encode-body [body]
  (cond
    (nil? body) nil
    (string? body) body
    :else (json/write-str body)))

(defn request
  "Perform an HTTP request returning {:status .. :body .. :headers ..} without throwing."
  [config {:keys [method path query body headers]}]
  (let [cfg (merge-config config)
        base-url (build-base-url cfg)
        url (if (and path (str/starts-with? path "http"))
              path
              (str base-url path))
        request-headers (cond-> {"Accept" "application/json"}
                          (:token cfg) (assoc "Authorization" (str "Bearer " (:token cfg)))
                          headers (merge headers))
        opts {:method method
              :url url
              :throw-exceptions false
              :headers request-headers
              :query-params query
              :socket-timeout (:timeout cfg)
              :conn-timeout (:timeout cfg)
              :accept :json
              :as :json-string-keys}
        opts (if body
               (assoc opts :body (encode-body body) :content-type :json)
               opts)]
    (try
      (http/request opts)
      (catch Exception ex
        {:status :error
         :error ex
         :message (.getMessage ex)}))))

(defn get-info [cfg]
  (request cfg {:method :get :path "/api/v1/info"}))

(defn init! [cfg]
  (request cfg {:method :post :path "/api/v1/init"}))

(defn put-document [cfg doc {:keys [branch]}]
  (request cfg {:method :post
                :path "/api/v1/put"
                :query (when branch {:branch branch})
                :body doc}))

(defn save-document [cfg doc]
  (request cfg {:method :post
                :path "/api/v1/save"
                :body doc}))

(defn get-document [cfg id {:keys [branch]}]
  (request cfg {:method :get
                :path (str "/api/v1/get/" id)
                :query (when branch {:branch branch})}))

(defn delete-document [cfg id {:keys [branch]}]
  (request cfg {:method :delete
                :path (str "/api/v1/delete/" id)
                :query (when branch {:branch branch})}))

(defn get-history [cfg id {:keys [branch limit since]}]
  (request cfg {:method :get
                :path (str "/api/v1/history/" id)
                :query (cond-> {}
                         branch (assoc :branch branch)
                         limit (assoc :limit limit)
                         since (assoc :since since))}))

(defn export-documents [cfg {:keys [prefix branch limit]}]
  (request cfg {:method :get
                :path "/api/v1/documents"
                :query (cond-> {}
                         prefix (assoc :prefix prefix)
                         branch (assoc :branch branch)
                         limit (assoc :limit limit))}))

(defn import-documents [cfg documents {:keys [branch]}]
  (request cfg {:method :post
                :path "/api/v1/documents/import"
                :query (when branch {:branch branch})
                :body {:documents documents}}))

(defn verify [cfg]
  (request cfg {:method :post
                :path "/api/v1/verify"}))

(defn search-documents
  "Search documents using AST queries via REST API"
  [cfg {:keys [q query branch limit offset sort after cursor]}]
  (request cfg {:method :get
                :path "/api/v1/search"
                :query (cond-> {}
                         q (assoc :q q)
                         query (assoc :query query)
                         branch (assoc :branch branch)
                         limit (assoc :limit limit)
                         offset (assoc :offset offset)
                         sort (assoc :sort sort)
                         after (assoc :after after)
                         cursor (assoc :cursor cursor))}))


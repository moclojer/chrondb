(ns chrondb.cli.core
  (:require [chrondb.cli.http :as http]
            [clojure.data.json :as json]
            [clojure.edn :as edn]
            [clojure.string :as str]
            [clojure.tools.cli :as cli]))

(def global-options
  [[nil "--host HOST" "ChronDB REST host" :default "localhost"]
   [nil "--port PORT" "ChronDB REST port" :default 3000 :parse-fn #(Integer/parseInt %)]
   [nil "--scheme SCHEME" "HTTP scheme" :default "http"]
   [nil "--base-url URL" "Full base URL overriding host/port"]
   [nil "--timeout MS" "Request timeout in milliseconds" :default 5000 :parse-fn #(Integer/parseInt %)]
   [nil "--token TOKEN" "Bearer token for authentication"]
   ["-h" "--help" "Show help"]])

(def command-spec
  {"init" {:doc "Fetch basic information from a ChronDB instance"
           :usage "chrondb init"}
   "info" {:doc "Alias for init" :usage "chrondb info"}
   "get" {:doc "Fetch a document by ID"
          :usage "chrondb get <id> [--branch main]"
          :options [[nil "--branch BRANCH" "Branch name"]]
          :args 1}
   "put" {:doc "Save a document"
          :usage "chrondb put <id> --data '{\"key\":\"value\"}' [--branch main]"
          :options [[nil "--branch BRANCH" "Branch name"]
                    [nil "--file FILE" "Path to JSON/EDN file with document" :default nil]
                    [nil "--data JSON" "Inline JSON document" :default nil]]
          :args 1}
   "history" {:doc "Show document history"
              :usage "chrondb history <id> [--branch main] [--limit N] [--since COMMIT]"
              :options [[nil "--branch BRANCH"]
                        [nil "--limit N" :parse-fn #(Integer/parseInt %)]
                        [nil "--since COMMIT"]]
              :args 1}
   "delete" {:doc "Delete a document"
             :usage "chrondb delete <id> [--branch main]"
             :options [[nil "--branch BRANCH"]]
             :args 1}
   "export" {:doc "Export documents matching a prefix"
             :usage "chrondb export [--prefix foo:] [--branch main] [--limit 10] [--output file.json]"
             :options [[nil "--prefix PREFIX"]
                       [nil "--branch BRANCH"]
                       [nil "--limit N" :parse-fn #(Integer/parseInt %)]
                       [nil "--output FILE"]]
             :args 0}
   "import" {:doc "Import documents from STDIN or file"
             :usage "chrondb import [--file file.json] [--branch main]"
             :options [[nil "--file FILE"]
                       [nil "--branch BRANCH"]]
             :args 0}
   "verify" {:doc "Run repository verification"
             :usage "chrondb verify"}
   "tail-history" {:doc "Continually poll document history"
                   :usage "chrondb tail-history <id> [--branch main] [--interval MS] [--since COMMIT]"
                   :options [[nil "--branch BRANCH"]
                             [nil "--interval MS" :default 2000 :parse-fn #(Integer/parseInt %)]
                             [nil "--since COMMIT"]]
                   :args 1}})

(defn usage
  ([]
   (str "ChronDB CLI\n\nUsage: chrondb [global-options] <command> [command-options]\n\n"
        "Commands:\n"
        (->> command-spec
             (map (fn [[cmd {:keys [doc usage]}]]
                    (str "  " cmd (when usage (str "\n      " usage))
                         (when doc (str "\n      " doc)))))
             (str/join "\n"))
        "\n\nGlobal options:\n"
        (->> global-options
             (map (fn [[short long desc & _]]
                    (format "  %s %-20s %s"
                            (or short "  ")
                            long
                            desc)))
             (str/join "\n"))))
  ([command]
   (if-let [{:keys [usage doc]} (command-spec command)]
     (str "Usage: " usage (when doc (str "\n" doc)))
     (str "Unknown command: " command))))

(defn parse-command-args [command args]
  (let [{:keys [options args errors]} (cli/parse-opts args (:options (command-spec command)))]
    {:options options :args args :errors errors}))

(defn- read-json [s]
  (json/read-str s :key-fn keyword))

(defn- read-json-file [path]
  (-> path slurp read-json))

(defn- read-edn-file [path]
  (-> path slurp edn/read-string))

(defn- load-documents-from-file [path]
  (let [ext (some-> path (str/lower-case) (str/split #"\.") last)]
    (case ext
      "json" (let [data (read-json-file path)]
               (if (vector? data) data [data]))
      "edn" (let [data (read-edn-file path)]
              (if (vector? data) data [data]))
      (throw (ex-info "Unsupported file extension" {:file path :extension ext})))))

(defn- normalize-documents [data]
  (cond
    (vector? data) data
    (map? data) [data]
    (nil? data) []
    :else (throw (ex-info "Invalid document payload" {:value data}))))

(defn- read-import-docs [{:keys [file stdin]}]
  (cond
    file (load-documents-from-file file)
    (string? stdin)
    (let [content (str/trim stdin)]
      (if (str/blank? content)
        []
        (normalize-documents (read-json content))))
    (some? stdin)
    []
    :else
    (let [reader *in*
          ready? (try
                   (.ready ^java.io.Reader reader)
                   (catch Exception _ false))]
      (if-not ready?
        []
        (let [content (slurp reader)
              trimmed (str/trim content)]
          (if (str/blank? trimmed)
            []
            (normalize-documents (read-json trimmed))))))))

(defn- pretty-json [data]
  (json/write-str data :indent true :escape-unicode false))

(defn exit! [status msg]
  (when msg
    (binding [*out* (if (zero? status) *out* *err*)]
      (println msg)))
  (System/exit status))

(defn- handle-response [resp]
  (cond
    (nil? resp) (exit! 1 "No response received")
    (= :error (:status resp)) (exit! 1 (str "Request error: " (:message resp)))
    (<= 200 (:status resp) 299) (println (pretty-json (:body resp)))
    (= 404 (:status resp)) (exit! 1 (str "Not found: " (pretty-json (:body resp))))
    :else (exit! 1 (str "HTTP " (:status resp) ": " (pretty-json (:body resp))))))

(defn- cfg-from-options [opts]
  (select-keys opts [:host :port :scheme :base-url :token :timeout]))

(defn run-init [cfg _ _]
  (handle-response (http/init! cfg)))

(defn run-info [cfg args opts]
  (run-init cfg args opts))

(defn run-get [cfg [id] opts]
  (handle-response (http/get-document cfg id opts)))

(defn run-put [cfg [id] opts]
  (let [{:keys [data file branch]} opts
        doc (cond
              data (read-json data)
              file (first (load-documents-from-file file))
              :else (throw (ex-info "Either --data or --file is required" {})))
        payload (assoc doc :id id)]
    (handle-response (http/put-document cfg payload {:branch branch}))))

(defn run-history [cfg [id] opts]
  (handle-response (http/get-history cfg id opts)))

(defn run-delete [cfg [id] opts]
  (handle-response (http/delete-document cfg id opts)))

(defn run-export [cfg _ opts]
  (let [resp (http/export-documents cfg opts)]
    (if-let [path (:output opts)]
      (do
        (spit path (pretty-json (:body resp)))
        (println "Export written to" path))
      (handle-response resp))))

(defn run-import [cfg _ opts]
  (let [docs (read-import-docs opts)]
    (if (seq docs)
      (handle-response (http/import-documents cfg docs {:branch (:branch opts)}))
      (exit! 1 "No documents provided for import"))))

(defn run-verify [cfg _ _]
  (handle-response (http/verify cfg)))

(defn run-tail-history [cfg [id] {:keys [interval since branch] :or {interval 2000}}]
  (loop [cursor since]
    (let [resp (http/get-history cfg id {:branch branch :since cursor :limit 100})]
      (cond
        (= :error (:status resp)) (exit! 1 (str "Request error: " (:message resp)))
        (= 404 (:status resp)) (do
                                 (Thread/sleep interval)
                                 (recur cursor))
        :else (let [entries (get-in resp [:body :history])
                    new-cursor (some-> entries first (get "commit-id"))]
                (doseq [entry (reverse entries)]
                  (println (pretty-json entry)))
                (Thread/sleep interval)
                (recur (or new-cursor cursor)))))))

(def handlers
  {"init" run-init
   "info" run-info
   "get" run-get
   "put" run-put
   "history" run-history
   "delete" run-delete
   "export" run-export
   "import" run-import
   "verify" run-verify
   "tail-history" run-tail-history})

(defn dispatch! [command cfg args opts]
  (if-let [handler (handlers command)]
    (handler cfg args opts)
    (exit! 1 (str "Unknown command: " command))))

(defn -main
  [& argv]
  (let [{:keys [options arguments errors]} (cli/parse-opts argv global-options :in-order true)]
    (when (:help options)
      (println (usage))
      (System/exit 0))
    (when errors
      (exit! 1 (str "Argument error:\n" (str/join "\n" errors))))
    (when (empty? arguments)
      (exit! 1 (usage)))
    (let [[command & cmd-args] arguments]
      (if-not (command-spec command)
        (exit! 1 (usage command))
        (let [{:keys [options args errors]} (parse-command-args command cmd-args)]
          (when errors
            (exit! 1 (str "Command error:\n" (str/join "\n" errors))))
          (let [required (:args (command-spec command))]
            (when (and required (< (count args) required))
              (exit! 1 (usage command))))
          (let [cfg (merge (cfg-from-options options) (cfg-from-options (:options (command-spec command))))]
            (dispatch! command cfg args options)))))))

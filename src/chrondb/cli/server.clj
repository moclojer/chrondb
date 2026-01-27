(ns chrondb.cli.server
  (:require [chrondb.api.server :as server]
            [chrondb.api.redis.core :as redis-core]
            [chrondb.api.sql.server :as sql-server]
            [chrondb.backup.core :as backup]
            [chrondb.storage.git.core :as git-core]
            [chrondb.storage.durable :as durable]
            [chrondb.storage.protocol :as storage]
            [chrondb.index.lucene :as lucene]
            [chrondb.index.lucene-nrt :as lucene-nrt]
            [chrondb.observability.health :as health]
            [chrondb.util.logging :as log]
            [chrondb.util.locks :as locks]
            [clojure.java.io :as io]
            [clojure.string :as str]))

(def server-command-map
  {"server" :server
   "backup" :backup
   "restore" :restore
   "export-snapshot" :export
   "import-snapshot" :import
   "schedule" :schedule
   "cancel-schedule" :cancel-schedule
   "list-schedules" :list-schedules})

(defn ensure-data-directories
  []
  (let [data-dir "data"
        index-dir (str data-dir "/index")
        wal-dir (str data-dir "/wal")]
    (when-not (.exists (io/file data-dir))
      (.mkdirs (io/file data-dir)))
    (when-not (.exists (io/file index-dir))
      (.mkdirs (io/file index-dir)))
    (when-not (.exists (io/file wal-dir))
      (.mkdirs (io/file wal-dir)))
    ;; Clean stale locks from Git and Lucene directories
    ;; Use :force? true on server startup since no other process should be running
    (locks/clean-stale-locks data-dir {:force? true :verbose? true})
    (locks/clean-lucene-lock index-dir {:force? true :verbose? true})))

(defn parse-kv-args
  [args]
  (loop [remaining args
         acc {}]
    (if (empty? remaining)
      acc
      (let [[flag value & more] remaining]
        (if (str/starts-with? flag "--")
          (recur more (assoc acc (keyword (subs flag 2)) value))
          (throw (ex-info "Invalid option format" {:option flag})))))))

(defn ensure-output [opts]
  (if-let [_ (:output opts)]
    opts
    (throw (ex-info "--output is required" {:options opts}))))

(defn ensure-input [opts]
  (if-let [_ (:input opts)]
    opts
    (throw (ex-info "--input is required" {:options opts}))))

(defn parse-server-options
  [args]
  (loop [remaining args
         options {:http-port "3000"
                  :redis-port "6379"
                  :sql-port "5432"
                  :disable-redis false
                  :disable-rest false
                  :disable-sql false}]
    (if (empty? remaining)
      options
      (let [arg (first remaining)]
        (cond
          (= arg "--disable-redis")
          (recur (rest remaining) (assoc options :disable-redis true))

          (= arg "--disable-rest")
          (recur (rest remaining) (assoc options :disable-rest true))

          (= arg "--disable-sql")
          (recur (rest remaining) (assoc options :disable-sql true))

          (and (not (str/starts-with? arg "--"))
               (nil? (get options :http-port-set)))
          (recur (rest remaining) (assoc options :http-port arg :http-port-set true))

          (and (not (str/starts-with? arg "--"))
               (get options :http-port-set)
               (nil? (get options :redis-port-set)))
          (recur (rest remaining) (assoc options :redis-port arg :redis-port-set true))

          (and (not (str/starts-with? arg "--"))
               (get options :http-port-set)
               (get options :redis-port-set)
               (nil? (get options :sql-port-set)))
          (recur (rest remaining) (assoc options :sql-port arg :sql-port-set true))

          :else
          (recur (rest remaining) options))))))

(defn start-servers
  [storage index {:keys [http-port redis-port sql-port disable-rest disable-redis disable-sql
                         health-checker wal]}]
  (when-not disable-rest
    (println "Starting REST API server on port" http-port)
    (server/start-server storage index http-port {:health-checker health-checker :wal wal}))
  (when-not disable-redis
    (println "Starting Redis protocol server on port" redis-port)
    (redis-core/start-redis-server storage index redis-port))
  (when-not disable-sql
    (println "Starting SQL protocol server on port" sql-port)
    (sql-server/start-server storage index sql-port))
  ;; Start index maintenance task (runs every 60 minutes by default)
  (lucene/start-index-maintenance-task index 60))

(defn create-health-checker
  "Create health checker with all standard checks."
  [storage index data-dir]
  (health/create-health-checker
   [(health/create-health-check
     :storage "Storage layer"
     (fn []
       (try
         (storage/get-document storage "health-check-probe" "main")
         {:status :healthy :message "Storage accessible"}
         (catch Exception _e
           {:status :healthy :message "Storage accessible (no test doc)"}))))
    (health/create-health-check
     :index "Lucene index"
     (fn []
       (try
         (let [stats (when (satisfies? chrondb.index.lucene-nrt/NRTIndex index)
                       (chrondb.index.lucene-nrt/get-stats index))]
           {:status :healthy
            :message "Index operational"
            :details stats})
         (catch Exception e
           {:status :degraded :message (str "Index warning: " (.getMessage e))}))))
    (health/create-health-check
     :disk "Disk space"
     (health/disk-space-check data-dir))
    (health/create-health-check
     :memory "Memory"
     (health/memory-check))]))

(defn run-server-command
  [args]
  (let [options (parse-server-options args)
        data-dir "data"
        ;; Create base Git storage
        base-storage (git-core/create-git-storage data-dir)
        _ (println "Creating Lucene index in data/index directory")
        ;; Create index (use NRT for better performance if available)
        index (try
                (log/log-info "Attempting to create NRT Lucene index...")
                (lucene-nrt/create-nrt-index (str data-dir "/index"))
                (catch Exception e
                  (log/log-warn (str "NRT index creation failed, falling back to standard: " (.getMessage e)))
                  (lucene/create-lucene-index (str data-dir "/index"))))
        _ (println "Lucene index created successfully: " (type index))
        ;; Create durable storage with WAL and OCC
        _ (println "Creating durable storage with WAL and OCC...")
        durable-storage (durable/create-durable-storage
                         base-storage
                         index
                         :wal-dir (str data-dir "/wal")
                         :enable-wal true
                         :enable-occ true
                         :recover-on-start true)
        _ (println "Durable storage created successfully")
        ;; Create health checker
        health-checker (create-health-checker durable-storage index data-dir)
        _ (println "Health checker initialized")
        ;; Ensure index is populated with existing documents (in background)
        _ (lucene/ensure-index-populated index base-storage "main")]
    (start-servers durable-storage index
                   {:http-port (Integer/parseInt (:http-port options))
                    :redis-port (Integer/parseInt (:redis-port options))
                    :sql-port (Integer/parseInt (:sql-port options))
                    :disable-rest (:disable-rest options)
                    :disable-redis (:disable-redis options)
                    :disable-sql (:disable-sql options)
                    :health-checker health-checker})))

(defn usage
  []
  (str/join "\n"
            ["ChronDB - Chronological database"
             ""
             "Usage:"
             "  chrondb server [options]               Start ChronDB services (default)"
             "  chrondb backup --output PATH [--format tar.gz|bundle]"
             "  chrondb restore --input PATH [--format tar.gz|bundle]"
             "  chrondb export-snapshot --output PATH [--refs ref1,ref2]"
             "  chrondb import-snapshot --input PATH"
             "  chrondb schedule --mode full|incremental --interval MINUTES --output-dir DIR"
             "  chrondb cancel-schedule --id JOB_ID"
             "  chrondb list-schedules"
             ""
             "Examples:"
             "  chrondb backup --output backups/full-$(date +%F).tar.gz"
             "  chrondb export-snapshot --output backups/main.bundle --refs refs/heads/main"
             "  chrondb restore --input backups/full.tar.gz"]))

(defn parse-command
  [args]
  (let [[cmd & rest-args] args]
    (case cmd
      nil {:command :server :args args}
      "server" {:command :server :args rest-args}
      "backup" {:command :backup :args rest-args}
      "restore" {:command :restore :args rest-args}
      "export-snapshot" {:command :export :args rest-args}
      "import-snapshot" {:command :import :args rest-args}
      "schedule" {:command :schedule :args rest-args}
      "cancel-schedule" {:command :cancel-schedule :args rest-args}
      "list-schedules" {:command :list-schedules :args rest-args}
      {:command :unknown :value cmd :args rest-args})))

(defn backup-command
  [storage args]
  (let [opts (-> args parse-kv-args ensure-output)
        format (keyword (or (:format opts) "tar.gz"))]
    (backup/create-full-backup storage {:output-path (:output opts)
                                        :format format
                                        :refs (when-let [refs (:refs opts)]
                                                (str/split refs #","))})))

(defn restore-command
  [storage args]
  (let [opts (-> args parse-kv-args ensure-input)
        format (keyword (or (:format opts) "tar.gz"))]
    (backup/restore-backup storage {:input-path (:input opts)
                                    :format format})))

(defn export-command
  [storage args]
  (let [opts (-> args parse-kv-args ensure-output)
        refs (when-let [refs (:refs opts)]
               (str/split refs #","))]
    (backup/export-snapshot storage {:output (:output opts)
                                     :refs refs})))

(defn import-command
  [storage args]
  (let [opts (-> args parse-kv-args ensure-input)]
    (backup/import-snapshot storage {:input (:input opts)})))

(defn schedule-command
  [storage args]
  (let [opts (parse-kv-args args)
        interval (some-> (:interval opts) Integer/parseInt)]
    (backup/schedule-backup storage {:mode (keyword (or (:mode opts) "full"))
                                     :format (keyword (or (:format opts) "tar.gz"))
                                     :interval-minutes interval
                                     :output-dir (:output-dir opts)
                                     :base-commit (:base-commit opts)
                                     :retention (some-> (:retention opts) Integer/parseInt)})))

(defn cancel-schedule-command
  [_storage args]
  (let [opts (parse-kv-args args)]
    (backup/cancel-scheduled-backup (:id opts))))

(defn list-schedules-command
  [_storage _args]
  (backup/list-scheduled-backups))

(defn dispatch!
  [command args]
  (case command
    :server (do
              (ensure-data-directories)
              (run-server-command args))
    (let [storage (git-core/create-git-storage "data")]
      (case command
        :backup (backup-command storage args)
        :restore (restore-command storage args)
        :export (export-command storage args)
        :import (import-command storage args)
        :schedule (schedule-command storage args)
        :cancel-schedule (cancel-schedule-command storage args)
        :list-schedules (list-schedules-command storage args)
        :unknown (do
                   (println "Unknown command" (first args))
                   (println (usage)))))))

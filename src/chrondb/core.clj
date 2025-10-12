;; This file is part of ChronDB.
;;
;; ChronDB is free software: you can redistribute it and/or modify
;; it under the terms of the GNU Affero General Public License as published
;; by the Free Software Foundation, either version 3 of the License,
;; or (at your option) any later version.
;;
;; ChronDB is distributed in the hope that it will be useful,
;; but WITHOUT ANY WARRANTY; without even the implied warranty of
;; MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
;; GNU Affero General Public License for more details.
;;
;; You should have received a copy of the GNU Affero General Public License
;; along with this program. If not, see <https://www.gnu.org/licenses/>.
(ns chrondb.core
  "Core namespace for ChronDB - A chronological database with Git-like versioning"
  (:gen-class)
  (:require [chrondb.api.server :as server]
            [chrondb.api.redis.core :as redis-core]
            [chrondb.api.sql.server :as sql-server]
            [chrondb.backup.core :as backup]
            [chrondb.storage.git.core :as git-core]
            [chrondb.index.lucene :as lucene]
            [clojure.java.io :as io]
            [clojure.string :as str]))

(defn ensure-data-directories
  "Creates necessary data directories for ChronDB if they don't exist.
   This includes the main data directory and the index subdirectory."
  []
  (let [data-dir "data"
        index-dir (str data-dir "/index")
        lock-file (io/file (str index-dir "/write.lock"))]
    (when-not (.exists (io/file data-dir))
      (.mkdirs (io/file data-dir)))
    (when-not (.exists (io/file index-dir))
      (.mkdirs (io/file index-dir)))
    ;; Remove any stale lock files that might exist
    (when (.exists lock-file)
      (println "Removing stale Lucene lock file")
      (.delete lock-file))))

(defn parse-server-options
  "Parse arguments for server mode (default)."
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
      "--help" {:command :help}
      "-h" {:command :help}
      {:command :unknown :value cmd})))

(defn parse-kv-args
  "Parse args like --key value into a map."
  [args]
  (loop [remaining args
         acc {}]
    (if (empty? remaining)
      acc
      (let [[flag value & more] remaining]
        (if (str/starts-with? flag "--")
          (recur more (assoc acc (keyword (subs flag 2)) value))
          (throw (ex-info "Invalid option format" {:option flag})))))))


(defn ensure-output
  [opts]
  (if-let [_ (:output opts)]
    opts
    (throw (ex-info "--output is required" {:options opts}))))

(defn ensure-input
  [opts]
  (if-let [_ (:input opts)]
    opts
    (throw (ex-info "--input is required" {:options opts}))))

(defn start-servers
  [storage index {:keys [http-port redis-port sql-port disable-rest disable-redis disable-sql]}]
  (when-not disable-rest
    (println "Starting REST API server on port" http-port)
    (server/start-server storage index http-port))
  (when-not disable-redis
    (println "Starting Redis protocol server on port" redis-port)
    (redis-core/start-redis-server storage index redis-port))
  (when-not disable-sql
    (println "Starting SQL protocol server on port" sql-port)
    (sql-server/start-server storage index sql-port)))

(defn run-server-command
  [args]
  (let [options (parse-server-options args)
        storage (git-core/create-git-storage "data")
        _ (println "Creating Lucene index in data/index directory")
        index (lucene/create-lucene-index "data/index")
        _ (println "Lucene index created successfully: " (type index))]
    (start-servers storage index
                   {:http-port (Integer/parseInt (:http-port options))
                    :redis-port (Integer/parseInt (:redis-port options))
                    :sql-port (Integer/parseInt (:sql-port options))
                    :disable-rest (:disable-rest options)
                    :disable-redis (:disable-redis options)
                    :disable-sql (:disable-sql options)})))

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

(defn -main
  [& args]
  (ensure-data-directories)
  (let [{:keys [command args value]} (parse-command args)]
    (case command
      :help (println (usage))
      :unknown (do
                 (println "Unknown command" value)
                 (println (usage)))
      :server (run-server-command args)
      (let [storage (git-core/create-git-storage "data")]
        (case command
          :backup (backup-command storage args)
          :restore (restore-command storage args)
          :export (export-command storage args)
          :import (import-command storage args)
          :schedule (schedule-command storage args)
          :cancel-schedule (cancel-schedule-command storage args)
          :list-schedules (list-schedules-command storage args))))))
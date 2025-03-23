(ns chrondb.core
  "Core namespace for ChronDB - A chronological database with Git-like versioning"
  (:require [chrondb.api.server :as server]
            [chrondb.api.redis.core :as redis-core]
            [chrondb.api.sql.server :as sql-server]
            [chrondb.storage.git :as git]
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

(defn parse-args
  "Parse command line arguments into a map of options.

   Supported options:
   - --disable-redis: Disables the Redis server
   - --disable-rest: Disables the REST API server
   - --disable-sql: Disables the SQL server

   Returns a map with:
   - :http-port - HTTP port number (default: 3000)
   - :redis-port - Redis port number (default: 6379)
   - :sql-port - SQL port number (default: 5432)
   - :disable-redis - Boolean flag to disable Redis server
   - :disable-rest - Boolean flag to disable REST API server
   - :disable-sql - Boolean flag to disable SQL server"
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
          (recur (rest remaining) (assoc options
                                         :http-port arg
                                         :http-port-set true))

          (and (not (str/starts-with? arg "--"))
               (get options :http-port-set)
               (nil? (get options :redis-port-set)))
          (recur (rest remaining) (assoc options
                                         :redis-port arg
                                         :redis-port-set true))

          (and (not (str/starts-with? arg "--"))
               (get options :http-port-set)
               (get options :redis-port-set)
               (nil? (get options :sql-port-set)))
          (recur (rest remaining) (assoc options
                                         :sql-port arg
                                         :sql-port-set true))

          :else
          (recur (rest remaining) options))))))

(defn -main
  "Entry point for the ChronDB application.
   Initializes the storage and index components and starts the HTTP server.

   Parameters:
   - args: Command line arguments
     - First non-flag argument: Optional port number for the HTTP server (default: 3000)
     - Second non-flag argument: Optional port number for the Redis server (default: 6379)
     - Third non-flag argument: Optional port number for the SQL server (default: 5432)
     - --disable-redis: Flag to disable the Redis server
     - --disable-rest: Flag to disable the REST API server
     - --disable-sql: Flag to disable the SQL server"
  [& args]
  (ensure-data-directories)
  (let [options (parse-args args)
        storage (git/create-git-storage "data")
        index (lucene/create-lucene-index "data/index")
        http-port (Integer/parseInt (:http-port options))
        redis-port (Integer/parseInt (:redis-port options))
        sql-port (Integer/parseInt (:sql-port options))
        disable-redis (:disable-redis options)
        disable-rest (:disable-rest options)
        disable-sql (:disable-sql options)]

    (when-not disable-rest
      (println "Starting REST API server on port" http-port)
      (server/start-server storage index http-port))

    (when-not disable-redis
      (println "Starting Redis protocol server on port" redis-port)
      (redis-core/start-redis-server storage index redis-port))

    (when-not disable-sql
      (println "Starting SQL protocol server on port" sql-port)
      (sql-server/start-server storage sql-port))))
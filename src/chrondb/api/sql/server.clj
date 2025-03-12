(ns chrondb.api.sql.server
  "Functions for starting and stopping the PostgreSQL-compatible SQL server for ChronDB"
  (:require [chrondb.api.sql.core :as sql]
            [chrondb.index.memory :as memory-index]))

(defn start-server
  "Starts a PostgreSQL-compatible SQL server for ChronDB.
   Parameters:
   - storage: The storage implementation
   - port: The port number to listen on (default: 5432)
   Returns: The server socket"
  ([storage]
   (start-server storage 5432))
  ([storage port]
   (let [index (memory-index/create-memory-index)]
     (sql/start-sql-server storage index port))))

(defn stop-server
  "Stops the SQL server.
   Parameters:
   - server-socket: The server socket to close
   Returns: nil"
  [server-socket]
  (sql/stop-sql-server server-socket))
(ns chrondb.api.sql.core
  "SQL server implementation for ChronDB"
  (:require [chrondb.storage.protocol :as storage]
            [chrondb.index.protocol :as index]
            [chrondb.util.logging :as log]
            [chrondb.api.sql.connection.server :as server]))

(defn start-sql-server
  "Starts a SQL server for ChronDB.
   Parameters:
   - storage: The storage implementation
   - index: The index implementation (optional)
   - port: The port number to listen on (optional, default: 5432)
   Returns: The server socket"
  ([storage]
   (start-sql-server storage nil 5432))
  ([storage index]
   (start-sql-server storage index 5432))
  ([storage index port]
   (log/log-info (str "Starting ChronDB SQL server on port " port))
   (server/start-sql-server storage index port)))

(defn stop-sql-server
  "Stops the SQL server.
   Parameters:
   - server-socket: The server socket to close
   Returns: nil"
  [server-socket]
  (log/log-info "Stopping ChronDB SQL server")
  (server/stop-sql-server server-socket))
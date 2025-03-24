(ns chrondb.api.sql.core
  "SQL server implementation for ChronDB"
  (:require [chrondb.util.logging :as log]
            [chrondb.api.sql.connection.server :as server]))

(defn- try-start-server
  "Attempts to start a server on the given port.
   If the port is in use, returns nil.
   Returns: The server socket or nil if port is in use"
  [storage index port]
  (try
    (server/start-sql-server storage index port)
    (catch java.net.BindException _e
      (log/log-warn (str "Port " port " is already in use. Try a different port."))
      nil)))

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
   (if (zero? port)
     ;; If port is 0, let the system choose an available port
     (server/start-sql-server storage index port)
     ;; Otherwise, try the specified port, or fall back to a random port if that fails
     (if-let [server (try-start-server storage index port)]
       server
       ;; If the specified port is in use, try with port 0 (system-assigned)
       (do
         (log/log-info "Falling back to system-assigned port")
         (server/start-sql-server storage index 0))))))

(defn stop-sql-server
  "Stops the SQL server.
   Parameters:
   - server-socket: The server socket to close
   Returns: nil"
  [server-socket]
  (log/log-info "Stopping ChronDB SQL server")
  (server/stop-sql-server server-socket))
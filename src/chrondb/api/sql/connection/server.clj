(ns chrondb.api.sql.connection.server
  "SQL server management"
  (:require [clojure.core.async :as async]
            [chrondb.api.sql.connection.client :as client]
            [chrondb.util.logging :as log])
  (:import [java.net ServerSocket]))

(defn start-sql-server
  "Starts a SQL server for ChronDB.
   Parameters:
   - storage: The storage implementation
   - index: The index implementation
   - port: The port number to listen on
   Returns: The server socket"
  [storage index port]
  (let [server-socket (ServerSocket. port)]
    (log/log-info (str "Starting SQL server on port " port))
    (async/go
      (try
        (while (not (.isClosed server-socket))
          (try
            (let [client-socket (.accept server-socket)]
              (async/go
                (client/handle-client storage index client-socket)))
            (catch Exception e
              (when-not (.isClosed server-socket)
                (log/log-error (str "Error accepting SQL connection: " (.getMessage e)))))))
        (catch Exception e
          (log/log-error (str "Error in SQL server: " (.getMessage e))))))
    server-socket))

(defn stop-sql-server
  "Stops the SQL server.
   Parameters:
   - server-socket: The server socket to close
   Returns: nil"
  [^ServerSocket server-socket]
  (log/log-info "Stopping SQL server")
  (when (and server-socket (not (.isClosed server-socket)))
    (.close server-socket)))
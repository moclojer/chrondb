(ns chrondb.api.sql.server
  "Functions for starting and stopping the PostgreSQL-compatible SQL server for ChronDB"
  (:require [chrondb.api.sql.core :as sql]
            [chrondb.index.lucene :as lucene-index]))

(defn start-server
  "Starts a PostgreSQL-compatible SQL server for ChronDB.
   Parameters:
   - storage: The storage implementation
   - index-or-port: Either an index implementation OR a port number (default: 5432)
   - port: The port number to listen on when index is provided
   Returns: The server socket"
  ([storage]
   (start-server storage nil 5432))
  ([storage index-or-port]
   (if (number? index-or-port)
     ;; Se for um número, é o port
     (start-server storage nil index-or-port)
     ;; Se não for um número, é o index
     (start-server storage index-or-port 5432)))
  ([storage index port]
   (let [actual-index (or index (lucene-index/create-lucene-index "data/index"))]
     (sql/start-sql-server storage actual-index port))))

(defn stop-server
  "Stops the SQL server.
   Parameters:
   - server-socket: The server socket to close
   Returns: nil"
  [server-socket]
  (sql/stop-sql-server server-socket))
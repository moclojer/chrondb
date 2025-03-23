(ns chrondb.api.sql.connection.client
  "Client connection management"
  (:require [chrondb.api.sql.protocol.handlers :as handlers]
            [chrondb.util.logging :as log])
  (:import [java.net Socket]))

(defn handle-client
  "Handles a PostgreSQL client connection.
   Parameters:
   - storage: The storage implementation
   - index: The index implementation
   - client-socket: The client socket
   Returns: nil"
  [storage index ^Socket client-socket]
  (log/log-info (str "SQL client connected: " (.getRemoteSocketAddress client-socket)))
  (let [closed-socket? (atom false)]
    (try
      (let [in (.getInputStream client-socket)
            out (.getOutputStream client-socket)]
        ;; Initialize the connection and process messages
        (handlers/handle-client-connection storage index in out))
      (catch Exception e
        (let [sw (java.io.StringWriter.)
              pw (java.io.PrintWriter. sw)]
          (.printStackTrace e pw)
          (log/log-error (str "Error initializing SQL client: " (.getMessage e) "\n" (.toString sw)))))
      (finally
        ;; Always ensure the socket is closed when we're done
        (when (and client-socket (not @closed-socket?) (not (.isClosed client-socket)))
          (try
            (.close client-socket)
            (log/log-info (str "Client socket closed: " (.getRemoteSocketAddress client-socket)))
            (catch Exception e
              (log/log-error (str "Error closing client socket: " (.getMessage e))))))))))
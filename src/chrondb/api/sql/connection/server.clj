(ns chrondb.api.sql.connection.server
  "Servidor de conexão SQL para o ChronDB"
  (:require [chrondb.util.logging :as log]
            [chrondb.api.sql.connection.handler :as handler])
  (:import [java.net ServerSocket InetAddress]
           [java.util.concurrent Executors]))

(defn- create-server-socket
  "Cria um socket de servidor na porta especificada"
  [port]
  (let [addr (InetAddress/getByName "localhost")
        server-socket (ServerSocket. port 50 addr)]
    server-socket))

(defn- accept-clients
  "Aceita conexões de clientes e processa usando o thread pool"
  [^ServerSocket server-socket thread-pool client-handler]
  (loop []
    (when-not (.isClosed server-socket)
      (try
        (let [socket (.accept server-socket)]
          ;; Submit the client handling task to the thread pool
          (.submit thread-pool ^Runnable #(client-handler socket)))
        (catch java.net.SocketException e
          (if (.isClosed server-socket)
            (log/log-info "Server socket closed.")
            (log/log-error (str "Socket error: " (.getMessage e)))))
        (catch Exception e
          (log/log-error (str "Error accepting client: " (.getMessage e)))))
      ;; Continue accepting connections (fora do try)
      (recur))))

(defn start-sql-server
  "Inicia um servidor SQL"
  [storage index port]
  (let [^ServerSocket server-socket (create-server-socket port)
        actual-port (.getLocalPort server-socket)
        thread-pool (Executors/newCachedThreadPool)
        client-handler (handler/create-connection-handler storage index)]

    (log/log-info (str "SQL server started on port " actual-port))

    ;; Start accepting connections in a separate thread
    (let [accept-thread (Thread. #(accept-clients server-socket thread-pool client-handler))]
      (.start accept-thread)

      ;; Retornar o objeto ServerSocket para manter compatibilidade com testes
      server-socket)))

(defn stop-sql-server
  "Para o servidor SQL"
  [server]
  (when server
    (log/log-info "Stopping SQL server...")

    ;; Se o objeto for um servidor-socket (compatibilidade com testes antigos)
    (if (instance? java.net.ServerSocket server)
      (do
        (.close server)
        (log/log-info "Server socket closed."))

      ;; Se for um mapa com informações do servidor (nova implementação)
      (let [{:keys [server-socket thread-pool]} server]
        ;; Close the server socket to stop accepting new connections
        (when server-socket
          (.close server-socket)
          (log/log-info "Server socket closed."))

        ;; Shutdown the thread pool
        (when thread-pool
          (.shutdown thread-pool)
          ;; Wait a bit for tasks to complete
          (try
            (.awaitTermination thread-pool 5 java.util.concurrent.TimeUnit/SECONDS)
            (catch InterruptedException _
              (log/log-warn "Interrupted while waiting for thread pool shutdown")))

          ;; Force shutdown if tasks are still running
          (when-not (.isTerminated thread-pool)
            (.shutdownNow thread-pool)))))

    (log/log-info "SQL server stopped")))
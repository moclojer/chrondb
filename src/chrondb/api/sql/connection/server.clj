(ns chrondb.api.sql.connection.server
  "Gerenciamento do servidor SQL"
  (:require [clojure.core.async :as async]
            [chrondb.api.sql.connection.client :as client]
            [chrondb.util.logging :as log])
  (:import [java.net ServerSocket]))

(defn start-sql-server
  "Inicia um servidor SQL para ChronDB.
   Parâmetros:
   - storage: A implementação de armazenamento
   - index: A implementação de índice
   - port: O número da porta para escutar
   Retorna: O socket do servidor"
  [storage index port]
  (let [server-socket (ServerSocket. port)]
    (log/log-info (str "Iniciando servidor SQL na porta " port))
    (async/go
      (try
        (while (not (.isClosed server-socket))
          (try
            (let [client-socket (.accept server-socket)]
              (async/go
                (client/handle-client storage index client-socket)))
            (catch Exception e
              (when-not (.isClosed server-socket)
                (log/log-error (str "Erro ao aceitar conexão SQL: " (.getMessage e)))))))
        (catch Exception e
          (log/log-error (str "Erro no servidor SQL: " (.getMessage e))))))
    server-socket))

(defn stop-sql-server
  "Para o servidor SQL.
   Parâmetros:
   - server-socket: O socket do servidor para fechar
   Retorna: nil"
  [^ServerSocket server-socket]
  (log/log-info "Parando servidor SQL")
  (when (and server-socket (not (.isClosed server-socket)))
    (.close server-socket)))
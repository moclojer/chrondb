(ns chrondb.api.sql.core
  "Core API for SQL protocol implementation"
  (:require [chrondb.util.logging :as log]
            [chrondb.api.sql.connection.server :as server]))

(defn- try-start-server
  "Tenta iniciar um servidor na porta especificada.
   Retorna nil se a porta estiver em uso."
  [storage index port]
  (try
    (server/start-sql-server storage index port)
    (catch java.net.BindException _e
      (log/log-warn (str "Port " port " is already in use. Try a different port."))
      nil)))

(defn start-sql-server
  "Inicia um servidor SQL para ChronDB.
   Parâmetros:
   - storage: A implementação de armazenamento
   - index: A implementação de índice (opcional)
   - port: O número da porta para escutar (opcional, padrão: 5432)
   Retorna: O objeto do servidor"
  ([storage]
   (start-sql-server storage nil 5432))
  ([storage index]
   (start-sql-server storage index 5432))
  ([storage index port]
   (log/log-info (str "Starting ChronDB SQL server on port " port))
   (if (zero? port)
     ;; Se a porta for 0, deixe o sistema escolher uma porta disponível
     (server/start-sql-server storage index port)
     ;; Caso contrário, tente a porta especificada ou recorra a uma porta aleatória
     (if-let [server (try-start-server storage index port)]
       server
       ;; Se a porta especificada estiver em uso, tente com a porta 0 (atribuída pelo sistema)
       (do
         (log/log-info "Falling back to system-assigned port")
         (server/start-sql-server storage index 0))))))

(defn stop-sql-server
  "Para o servidor SQL.
   Parâmetros:
   - server: O objeto do servidor a ser fechado
   Retorna: nil"
  [server]
  (log/log-info "Stopping ChronDB SQL server")
  (server/stop-sql-server server))
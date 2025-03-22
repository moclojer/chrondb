(ns chrondb.api.sql.core
  "Implementação do servidor SQL para ChronDB"
  (:require [chrondb.storage.protocol :as storage]
            [chrondb.index.protocol :as index]
            [chrondb.util.logging :as log]
            [chrondb.api.sql.connection.server :as server]))

(defn start-sql-server
  "Inicia um servidor SQL para ChronDB.
   Parâmetros:
   - storage: A implementação de armazenamento
   - index: A implementação de índice (opcional)
   - port: O número da porta para escutar (opcional, padrão: 5432)
   Retorna: O socket do servidor"
  ([storage]
   (start-sql-server storage nil 5432))
  ([storage index]
   (start-sql-server storage index 5432))
  ([storage index port]
   (log/log-info (str "Iniciando servidor SQL do ChronDB na porta " port))
   (server/start-sql-server storage index port)))

(defn stop-sql-server
  "Para o servidor SQL.
   Parâmetros:
   - server-socket: O socket do servidor para fechar
   Retorna: nil"
  [server-socket]
  (log/log-info "Parando servidor SQL do ChronDB")
  (server/stop-sql-server server-socket))
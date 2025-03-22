(ns chrondb.api.sql.connection.client
  "Gerenciamento de conexões de clientes"
  (:require [chrondb.api.sql.protocol.handlers :as handlers]
            [chrondb.util.logging :as log])
  (:import [java.net Socket]
           [java.io InputStream OutputStream]))

(defn handle-client
  "Manipula uma conexão de cliente PostgreSQL.
   Parâmetros:
   - storage: A implementação de armazenamento
   - index: A implementação de índice
   - client-socket: O socket do cliente
   Retorna: nil"
  [storage index ^Socket client-socket]
  (log/log-info (str "Cliente SQL conectado: " (.getRemoteSocketAddress client-socket)))
  (let [closed-socket? (atom false)]
    (try
      (let [in (.getInputStream client-socket)
            out (.getOutputStream client-socket)]
        ;; Inicializa a conexão e processa mensagens
        (handlers/handle-client-connection storage index in out))
      (catch Exception e
        (let [sw (java.io.StringWriter.)
              pw (java.io.PrintWriter. sw)]
          (.printStackTrace e pw)
          (log/log-error (str "Erro ao inicializar cliente SQL: " (.getMessage e) "\n" (.toString sw)))))
      (finally
        ;; Sempre garante que o socket seja fechado quando terminarmos
        (when (and client-socket (not @closed-socket?) (not (.isClosed client-socket)))
          (try
            (.close client-socket)
            (log/log-info (str "Socket do cliente fechado: " (.getRemoteSocketAddress client-socket)))
            (catch Exception e
              (log/log-error (str "Erro ao fechar socket do cliente: " (.getMessage e))))))))))
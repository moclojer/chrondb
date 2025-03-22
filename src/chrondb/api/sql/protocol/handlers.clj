(ns chrondb.api.sql.protocol.handlers
  "Manipuladores de mensagens do protocolo PostgreSQL"
  (:require [chrondb.api.sql.protocol.messages :as messages]
            [chrondb.util.logging :as log]
            [chrondb.api.sql.execution.query :as query])
  (:import [java.io InputStream OutputStream DataInputStream]
           [java.nio.charset StandardCharsets]))

(defn handle-message
  "Manipula uma mensagem do protocolo PostgreSQL.
   Parâmetros:
   - storage: A implementação de armazenamento
   - index: A implementação de índice
   - out: O stream de saída para escrever respostas
   - message-type: O tipo da mensagem
   - buffer: O conteúdo da mensagem como um array de bytes
   - content-length: O tamanho do conteúdo da mensagem
   Retorna: true para continuar lendo mensagens, false para encerrar a conexão"
  [storage index ^OutputStream out message-type buffer content-length]
  (log/log-debug (str "Mensagem recebida do tipo: " (char message-type)))
  (try
    (case (char message-type)
      \Q (let [query-text (String. buffer 0 (dec content-length) StandardCharsets/UTF_8)]
           (log/log-debug (str "Consulta SQL: " query-text))
           (try
             (query/handle-query storage index out query-text)
             (catch Exception e
               (let [sw (java.io.StringWriter.)
                     pw (java.io.PrintWriter. sw)]
                 (.printStackTrace e pw)
                 (log/log-error (str "Erro ao executar consulta: " (.getMessage e) "\n" (.toString sw))))
               (messages/send-error-response out (str "Erro ao executar consulta: " (.getMessage e)))
               (messages/send-command-complete out "UNKNOWN" 0)))
           (messages/send-ready-for-query out)
           true)  ;; Continua lendo
      \X (do      ;; Mensagem de terminação recebida
           (log/log-info "Cliente solicitou terminação")
           false) ;; Sinaliza para fechar a conexão
      (do         ;; Outro comando não suportado
        (log/log-debug (str "Comando não suportado: " (char message-type)))
        (messages/send-error-response out (str "Comando não suportado: " (char message-type)))
        (messages/send-ready-for-query out)
        true))    ;; Continua lendo
    (catch Exception e
      (let [sw (java.io.StringWriter.)
            pw (java.io.PrintWriter. sw)]
        (.printStackTrace e pw)
        (log/log-error (str "Erro ao manipular mensagem: " (.getMessage e) "\n" (.toString sw))))
      (try
        (messages/send-error-response out (str "Erro interno do servidor: " (.getMessage e)))
        (messages/send-ready-for-query out)
        (catch Exception e2
          (log/log-error (str "Erro ao enviar resposta de erro: " (.getMessage e2)))))
      true)))

(defn read-client-messages
  "Lê mensagens de um cliente PostgreSQL.
   Parâmetros:
   - storage: A implementação de armazenamento
   - index: A implementação de índice
   - in: O stream de entrada para ler
   - out: O stream de saída para escrever respostas
   Retorna: nil"
  [storage index ^InputStream in ^OutputStream out]
  (let [dis (DataInputStream. in)]
    (try
      (loop []
        (log/log-debug "Aguardando mensagem do cliente")
        (let [continue?
              (try
                (let [message-type (.readByte dis)]
                  (log/log-debug (str "Mensagem recebida do tipo: " (char message-type)))
                  (when (pos? message-type)
                    (let [message-length (.readInt dis)
                          content-length (- message-length 4)
                          buffer (byte-array content-length)]
                      (.readFully dis buffer 0 content-length)
                      (handle-message storage index out message-type buffer content-length))))
                (catch java.io.EOFException _e
                  (log/log-info "Cliente desconectado")
                  false) ;; Termina
                (catch java.net.SocketException e
                  (log/log-info (str "Socket fechado: " (.getMessage e)))
                  false) ;; Termina
                (catch Exception e
                  (let [sw (java.io.StringWriter.)
                        pw (java.io.PrintWriter. sw)]
                    (.printStackTrace e pw)
                    (log/log-error (str "Erro ao ler mensagem do cliente: " (.getMessage e) "\n" (.toString sw))))
                  (try
                    (messages/send-error-response out (str "Erro interno do servidor: Erro ao ler mensagem"))
                    (messages/send-ready-for-query out)
                    (catch Exception e2
                      (log/log-error (str "Erro ao enviar resposta de erro: " (.getMessage e2)))))
                  true))] ;; Continua lendo a menos que o socket tenha sido fechado
          (when continue?
            (recur))))
      (catch Exception e
        (let [sw (java.io.StringWriter.)
              pw (java.io.PrintWriter. sw)]
          (.printStackTrace e pw)
          (log/log-error (str "Erro no loop de processamento do cliente: " (.getMessage e) "\n" (.toString sw))))))))

(defn handle-client-connection
  "Inicializa a conexão com um cliente PostgreSQL.
   Parâmetros:
   - storage: A implementação de armazenamento
   - index: A implementação de índice
   - in: O stream de entrada do cliente
   - out: O stream de saída para o cliente
   Retorna: nil"
  [storage index ^InputStream in ^OutputStream out]
  (when-let [_startup-message (messages/read-startup-message in)]
    (log/log-debug "Enviando autenticação OK")
    (messages/send-authentication-ok out)
    (messages/send-parameter-status out "server_version" "14.0")
    (messages/send-parameter-status out "client_encoding" "UTF8")
    (messages/send-parameter-status out "DateStyle" "ISO, MDY")
    (messages/send-backend-key-data out)
    (messages/send-ready-for-query out)

    ;; Loop principal para ler consultas
    (read-client-messages storage index in out)))
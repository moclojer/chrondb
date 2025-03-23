(ns chrondb.api.sql.protocol.handlers
  "Request handlers for SQL protocol"
  (:require [chrondb.util.logging :as log]
            [chrondb.api.sql.protocol.messages :as messages]
            [chrondb.api.sql.execution.query :as query])
  (:import [java.io InputStream OutputStream]))

(defn handle-query-message
  "Handles a query message from a client.
   Parameters:
   - storage: The storage implementation
   - index: The index implementation (optional)
   - in: The input stream
   - out: The output stream
   Returns: nil"
  [storage index ^InputStream in ^OutputStream out _]
  (try
    (let [;; Ler o comprimento da mensagem (4 bytes como um int)
          length-bytes (byte-array 4)
          _ (.read in length-bytes)
          length (-> (bit-and (aget length-bytes 0) 0xFF) (bit-shift-left 24)
                     (bit-or (-> (bit-and (aget length-bytes 1) 0xFF) (bit-shift-left 16)))
                     (bit-or (-> (bit-and (aget length-bytes 2) 0xFF) (bit-shift-left 8)))
                     (bit-or (bit-and (aget length-bytes 3) 0xFF)))
          ;; O comprimento inclui os próprios 4 bytes do campo de comprimento
          content-length (- length 4)
          ;; Ler o restante da mensagem
          message-bytes (byte-array content-length)
          bytes-read (.read in message-bytes 0 content-length)

          ;; Obter o texto da consulta SQL (ignorando o byte nulo final)
          query-text (String. message-bytes 0 (dec bytes-read) "UTF-8")]

      (log/log-info (str "Executando consulta SQL: " query-text))

      ;; Executar a consulta
      (query/handle-query storage index out query-text)

      ;; Indicar que estamos prontos para a próxima consulta
      (messages/send-ready-for-query out \I))
    (catch Exception e
      (let [sw (java.io.StringWriter.)
            pw (java.io.PrintWriter. sw)]
        (.printStackTrace e pw)
        (log/log-error (str "Erro ao executar consulta: " (.getMessage e) "\n" (.toString sw))))
      (messages/send-error-response out (str "Erro ao executar consulta: " (.getMessage e)))
      (messages/send-ready-for-query out \E))))

(defn handle-message
  "Handles a message from a client.
   Parameters:
   - storage: The storage implementation
   - index: The index implementation (optional)
   - in: The input stream
   - out: The output stream
   - message-type: The message type
   Returns: nil"
  [storage index ^InputStream in ^OutputStream out message-type]
  (try
    (case message-type
      ;; 'Q' = Simple Query
      81 (handle-query-message storage index in out message-type)
      ;; 'X' = Terminate
      88 (do
           (log/log-info "Cliente solicitou encerramento")
           ;; Não envie uma resposta aqui, apenas retorne
           nil)
      ;; Outros tipos de mensagem não suportados
      (do
        (log/log-info (str "Comando não suportado: " (char message-type)))
        ;; Leia e descarte o resto da mensagem
        (let [length-bytes (byte-array 4)
              _ (.read in length-bytes)
              length (-> (bit-and (aget length-bytes 0) 0xFF) (bit-shift-left 24)
                         (bit-or (-> (bit-and (aget length-bytes 1) 0xFF) (bit-shift-left 16)))
                         (bit-or (-> (bit-and (aget length-bytes 2) 0xFF) (bit-shift-left 8)))
                         (bit-or (bit-and (aget length-bytes 3) 0xFF)))
              content-length (- length 4)
              discard-buffer (byte-array content-length)]
          (.read in discard-buffer))
        (messages/send-error-response out (str "Comando não suportado: " (char message-type)))
        (messages/send-ready-for-query out \I)))
    (catch Exception e
      (let [sw (java.io.StringWriter.)
            pw (java.io.PrintWriter. sw)]
        (.printStackTrace e pw)
        (log/log-error (str "Erro ao processar mensagem: " (.getMessage e) "\n" (.toString sw))))
      (try
        (messages/send-error-response out (str "Erro interno do servidor: " (.getMessage e)))
        (messages/send-ready-for-query out \E)
        (catch Exception e2
          (log/log-error (str "Erro ao enviar resposta de erro: " (.getMessage e2))))))))

(defn handle-client
  "Handles a client socket.
   Parameters:
   - storage: The storage implementation
   - index: The index implementation (optional)
   - in: The input stream
   - out: The output stream
   Returns: nil"
  [storage index ^InputStream in ^OutputStream out]
  (try
    ;; Read initial startup message
    (let [startup-message (messages/read-startup-message in)]
      (log/log-info (str "Client connected: " startup-message))

      ;; Send authentication OK
      (messages/send-authentication-ok out)

      ;; Send parameter status messages
      (messages/send-parameter-status out "server_version" "9.5.0")
      (messages/send-parameter-status out "client_encoding" "UTF8")

      ;; Indicate that we're ready to process queries
      (messages/send-ready-for-query out \I)

      ;; Process client messages until EOF
      (loop []
        ;; Read message type
        (let [message-type (.read in)]
          (if (not= message-type -1)  ;; EOF check
            (do
              (log/log-info (str "Message type received: " (char message-type)))
              (handle-message storage index in out message-type)
              (recur))
            (log/log-info "Client disconnected")))))
    (catch Exception e
      (let [sw (java.io.StringWriter.)
            pw (java.io.PrintWriter. sw)]
        (.printStackTrace e pw)
        (log/log-error (str "Error reading client message: " (.getMessage e) "\n" (.toString sw))))
      (try
        (messages/send-error-response out "Internal server error: Error reading message")
        (messages/send-ready-for-query out \E)
        (catch Exception e2
          (log/log-error (str "Error sending error response: " (.getMessage e2))))))))

(defn create-client-handler
  "Creates a handler for a client socket.
   Parameters:
   - storage: The storage implementation
   - index: The index implementation (optional)
   Returns: A function that takes a socket and handles it"
  [storage index]
  (fn [socket]
    (try
      (with-open [in (.getInputStream socket)
                  out (.getOutputStream socket)]
        (handle-client storage index in out))
      (catch Exception e
        (let [sw (java.io.StringWriter.)
              pw (java.io.PrintWriter. sw)]
          (.printStackTrace e pw)
          (log/log-error (str "Error in client processing loop: " (.getMessage e) "\n" (.toString sw))))))))
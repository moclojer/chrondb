(ns chrondb.api.sql.connection.handler
  "Functions for handling SQL client connections"
  (:require [chrondb.util.logging :as log]
            [chrondb.api.sql.protocol.core :as protocol]
            [chrondb.api.sql.protocol.messages.reader :as reader]
            [chrondb.api.sql.execution.query :as query])
  (:import [java.io InputStream OutputStream]))

(defn handle-query-message
  "Handles a query message"
  [storage index protocol-impl ^InputStream in ^OutputStream out session-context]
  (let [query-data (reader/read-query-message in)]
    (if (:error query-data)
      (do
        (log/log-error (str "Error reading query: " (:error query-data)))
        (-> protocol-impl
            (.write-query-result out {:command "ERROR" :rows [] :data [] :columns []})))

      (let [query-text (:query query-data)]
        (log/log-info (str "Executing SQL query: " query-text))

        (try
          ;; Execute query and get results
          (query/handle-query storage index out query-text session-context)

          ;; Retornamos true porque handle-query já envia as respostas diretamente para o output stream
          true

          (catch Exception e
            (log/log-error (str "Error executing query: " (.getMessage e)))
            (-> protocol-impl
                (.write-query-result out {:command "ERROR"
                                          :rows []
                                          :error (.getMessage e)}))))))))

(defn handle-terminate-message
  "Handles a terminate message"
  []
  (log/log-info "Client requested termination")
  {:terminate true})

(defn handle-message
  "Handles a client message based on its type"
  [storage index protocol-impl ^InputStream in ^OutputStream out message session-context]
  (let [type (:type message)]
    (try
      (case type
        ;; 'Q' = Simple Query
        81 (handle-query-message storage index protocol-impl in out session-context)
        ;; 'X' = Terminate
        88 (handle-terminate-message)
        ;; Unsupported message type
        (do
          (log/log-info (str "Unsupported command: " (char type)))
          ;; Discard message content
          (let [length (reader/read-int in)
                content-length (- length 4)
                discard-buffer (byte-array content-length)]
            (.read in discard-buffer))
          ;; Send error response
          (-> protocol-impl
              (.write-query-result out
                                   {:command "ERROR"
                                    :rows []
                                    :error (str "Unsupported command: " (char type))}))))
      (catch Exception e
        (log/log-error (str "Error processing message: " (.getMessage e)))
        (-> protocol-impl
            (.write-query-result out
                                 {:command "ERROR"
                                  :rows []
                                  :error (str "Internal server error: " (.getMessage e))}))))))

(defn handle-client-connection
  "Handles a new client connection"
  [storage index ^InputStream in ^OutputStream out]
  (try
    (let [protocol-impl (protocol/create-protocol)
          ;; Create session context with mutable state for branch tracking
          session-context (atom {:current-branch nil})]
      ;; Handle startup sequence
      (doseq [action (-> protocol-impl (.handle-startup in out))]
        (action))

      ;; Process client messages until terminated or EOF
      (loop []
        (let [message (-> protocol-impl (.read-message in))]
          (if message
            (let [result (handle-message storage index protocol-impl in out message session-context)]
              (if-not (:terminate result)
                (recur)
                (log/log-info "Client session terminated normally")))
            (log/log-info "Client disconnected")))))

    (catch Exception e
      (log/log-error (str "Error in client connection handler: " (.getMessage e))))))

(defn create-connection-handler
  "Creates a handler function for new client connections"
  [storage index]
  (fn [client-socket]
    (try
      (with-open [in (.getInputStream client-socket)
                  out (.getOutputStream client-socket)]
        (handle-client-connection storage index in out))
      (catch Exception e
        (log/log-error (str "Error handling client socket: " (.getMessage e)))))))
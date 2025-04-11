(ns chrondb.api.sql.protocol.core
  "Core functionality for the PostgreSQL protocol implementation"
  (:require [chrondb.api.sql.protocol.messages.reader :as reader]
            [chrondb.api.sql.protocol.messages.writer :as writer]
            [chrondb.util.logging :as log]))

;; A principal interface do protocolo - transforma entre dados do protocolo e dados da aplicação
(defprotocol PostgresProtocol
  "Protocol defining the main operations for PostgreSQL communication"
  (handle-startup [this in out] "Handle startup sequence")
  (read-message [this in] "Read a message from the input stream")
  (write-query-result [this out result] "Write query results to the output stream"))

;; Implementação concreta do protocolo
(defrecord PostgresProtocolImpl []
  PostgresProtocol

  (handle-startup [_this in out]
    (let [startup-message (reader/read-startup-message in)]
      (log/log-info (str "Client connected: " startup-message))

      ;; Return a sequence of actions to perform
      [(fn [] (writer/send-authentication-ok out))
       (fn [] (writer/send-parameter-status out "server_version" "9.5.0"))
       (fn [] (writer/send-parameter-status out "client_encoding" "UTF8"))
       (fn [] (writer/send-notice-response out "Welcome to ChronDB - Git-backed Versioned Database System"))
       (fn [] (writer/send-ready-for-query out \I))]))

  (read-message [_this in]
    (let [message-type (.read in)]
      (when (not= message-type -1) ;; EOF check
        {:type message-type
         :type-char (char message-type)})))

  (write-query-result [_this out {:keys [command rows data columns] :as result}]
    (let [write-actions
          (cond-> []
            ;; If there are columns in the result, send row description
            columns
            (conj #(writer/send-row-description out columns))

            ;; If there's data, send each row
            (seq data)
            (into (map (fn [row] #(writer/send-data-row out row)) data))

            ;; Always send command complete
            :always
            (conj #(writer/send-command-complete out command (count rows)))

            ;; Always send ready for query at the end
            :always
            (conj #(writer/send-ready-for-query out \I)))]

      ;; Execute all write actions in sequence
      (doseq [action write-actions]
        (action))

      result)))

(defn create-protocol
  "Creates a new instance of the PostgreSQL protocol implementation"
  []
  (->PostgresProtocolImpl))
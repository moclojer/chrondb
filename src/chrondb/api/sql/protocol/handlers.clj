(ns chrondb.api.sql.protocol.handlers
  "PostgreSQL protocol message handlers"
  (:require [chrondb.api.sql.protocol.messages :as messages]
            [chrondb.util.logging :as log]
            [chrondb.api.sql.execution.query :as query])
  (:import [java.io InputStream OutputStream DataInputStream]
           [java.nio.charset StandardCharsets]))

(defn handle-message
  "Handles a PostgreSQL protocol message.
   Parameters:
   - storage: The storage implementation
   - index: The index implementation
   - out: The output stream to write responses
   - message-type: The message type
   - buffer: The message content as a byte array
   - content-length: The content length of the message
   Returns: true to continue reading messages, false to terminate the connection"
  [storage index ^OutputStream out message-type buffer content-length]
  (log/log-debug (str "Message received of type: " (char message-type)))
  (try
    (case (char message-type)
      \Q (let [query-text (String. buffer 0 (dec content-length) StandardCharsets/UTF_8)]
           (log/log-debug (str "SQL Query: " query-text))
           (try
             (query/handle-query storage index out query-text)
             (catch Exception e
               (let [sw (java.io.StringWriter.)
                     pw (java.io.PrintWriter. sw)]
                 (.printStackTrace e pw)
                 (log/log-error (str "Error executing query: " (.getMessage e) "\n" (.toString sw))))
               (messages/send-error-response out (str "Error executing query: " (.getMessage e)))
               (messages/send-command-complete out "UNKNOWN" 0)))
           (messages/send-ready-for-query out)
           true)  ;; Continue reading
      \X (do      ;; Termination message received
           (log/log-info "Client requested termination")
           false) ;; Signal to close the connection
      (do         ;; Other unsupported command
        (log/log-debug (str "Unsupported command: " (char message-type)))
        (messages/send-error-response out (str "Unsupported command: " (char message-type)))
        (messages/send-ready-for-query out)
        true))    ;; Continue reading
    (catch Exception e
      (let [sw (java.io.StringWriter.)
            pw (java.io.PrintWriter. sw)]
        (.printStackTrace e pw)
        (log/log-error (str "Error handling message: " (.getMessage e) "\n" (.toString sw))))
      (try
        (messages/send-error-response out (str "Internal server error: " (.getMessage e)))
        (messages/send-ready-for-query out)
        (catch Exception e2
          (log/log-error (str "Error sending error response: " (.getMessage e2)))))
      true)))

(defn read-client-messages
  "Reads messages from a PostgreSQL client.
   Parameters:
   - storage: The storage implementation
   - index: The index implementation
   - in: The input stream to read from
   - out: The output stream to write responses
   Returns: nil"
  [storage index ^InputStream in ^OutputStream out]
  (let [dis (DataInputStream. in)]
    (try
      (loop []
        (log/log-debug "Waiting for client message")
        (let [continue?
              (try
                (let [message-type (.readByte dis)]
                  (log/log-debug (str "Message received of type: " (char message-type)))
                  (when (pos? message-type)
                    (let [message-length (.readInt dis)
                          content-length (- message-length 4)
                          buffer (byte-array content-length)]
                      (.readFully dis buffer 0 content-length)
                      (handle-message storage index out message-type buffer content-length))))
                (catch java.io.EOFException _e
                  (log/log-info "Client disconnected")
                  false) ;; Terminate
                (catch java.net.SocketException e
                  (log/log-info (str "Socket closed: " (.getMessage e)))
                  false) ;; Terminate
                (catch Exception e
                  (let [sw (java.io.StringWriter.)
                        pw (java.io.PrintWriter. sw)]
                    (.printStackTrace e pw)
                    (log/log-error (str "Error reading client message: " (.getMessage e) "\n" (.toString sw))))
                  (try
                    (messages/send-error-response out (str "Internal server error: Error reading message"))
                    (messages/send-ready-for-query out)
                    (catch Exception e2
                      (log/log-error (str "Error sending error response: " (.getMessage e2)))))
                  true))] ;; Continue reading unless socket has been closed
          (when continue?
            (recur))))
      (catch Exception e
        (let [sw (java.io.StringWriter.)
              pw (java.io.PrintWriter. sw)]
          (.printStackTrace e pw)
          (log/log-error (str "Error in client processing loop: " (.getMessage e) "\n" (.toString sw))))))))

(defn handle-client-connection
  "Initializes connection with a PostgreSQL client.
   Parameters:
   - storage: The storage implementation
   - index: The index implementation
   - in: The client's input stream
   - out: The client's output stream
   Returns: nil"
  [storage index ^InputStream in ^OutputStream out]
  (when-let [_startup-message (messages/read-startup-message in)]
    (log/log-debug "Sending authentication OK")
    (messages/send-authentication-ok out)
    (messages/send-parameter-status out "server_version" "14.0")
    (messages/send-parameter-status out "client_encoding" "UTF8")
    (messages/send-parameter-status out "DateStyle" "ISO, MDY")
    (messages/send-backend-key-data out)
    (messages/send-ready-for-query out)

    ;; Main loop to read queries
    (read-client-messages storage index in out)))
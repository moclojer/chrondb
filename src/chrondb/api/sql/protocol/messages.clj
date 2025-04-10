(ns chrondb.api.sql.protocol.messages
  "Functions for handling PostgreSQL protocol messages.
   Defines data structures and encoding/decoding for the protocol messages."
  (:require [chrondb.api.sql.protocol.constants :as constants]
            [chrondb.util.logging :as log])
  (:import [java.io InputStream OutputStream]
           [java.nio ByteBuffer]
           [java.nio.charset StandardCharsets]))

;; Helper for writing messages to the stream
(defn write-message
  "Write a message to the output stream.
   Parameters:
   - out: The output stream
   - type: The message type character
   - body: The message body (string or byte array)
   Returns: nil"
  [^OutputStream out type body]
  (try
    (let [is-byte-array (instance? (Class/forName "[B") body)
          body-bytes (if is-byte-array
                       ^bytes body
                       (.getBytes (str body) StandardCharsets/UTF_8))
          length-int (+ 4 (if is-byte-array
                            (alength ^bytes body-bytes)
                            (count body-bytes)))]
      ;; Message type
      (.write out (int type))
      ;; Message length (int32)
      (if (not= type constants/PG_STARTUP_MESSAGE)
        (do
          ;; Normal message: 1 byte type + 4 bytes length + body
          (.write out (bit-and (bit-shift-right length-int 24) 0xFF))
          (.write out (bit-and (bit-shift-right length-int 16) 0xFF))
          (.write out (bit-and (bit-shift-right length-int 8) 0xFF))
          (.write out (bit-and length-int 0xFF)))
        (do
          ;; Startup message: only 4 bytes length + body (no type byte)
          (.write out (bit-and (bit-shift-right length-int 24) 0xFF))
          (.write out (bit-and (bit-shift-right length-int 16) 0xFF))
          (.write out (bit-and (bit-shift-right length-int 8) 0xFF))
          (.write out (bit-and length-int 0xFF))))
      ;; Message body
      (if is-byte-array
        (.write out ^bytes body-bytes)
        (.write out body-bytes))
      (.flush out))
    (catch java.net.SocketException e
      (log/log-warn (str "Socket error while writing message of type: " (when type (char type)) " - " (.getMessage e)))
      nil)
    (catch Exception e
      (log/log-error (str "Error writing message: " (.getMessage e) " for type: " (when type (char type))))
      nil)))

;; Reading functions
(defn read-byte
  "Read a single byte from the input stream"
  [^InputStream in]
  (.read in))

(defn read-int
  "Read a 4-byte integer from the input stream"
  [^InputStream in]
  (-> (read-byte in)
      (bit-shift-left 24)
      (bit-or (bit-shift-left (read-byte in) 16))
      (bit-or (bit-shift-left (read-byte in) 8))
      (bit-or (read-byte in))))

(defn read-short
  "Read a 2-byte short from the input stream"
  [^InputStream in]
  (-> (bit-shift-left (read-byte in) 8)
      (bit-or (read-byte in))))

(defn read-null-terminated-string
  "Read a null-terminated string from the input stream"
  [^InputStream in]
  (let [sb (StringBuilder.)]
    (loop []
      (let [b (read-byte in)]
        (if (zero? b)
          (.toString sb)
          (do
            (.append sb (char b))
            (recur)))))))

(defn read-startup-message
  "Read a startup message from the client"
  [^InputStream in]
  (try
    (let [length (read-int in)
          protocol-version (read-int in)
          parameter-bytes (byte-array (- length 8))]
      (.read in parameter-bytes)
      (let [parameters (ByteBuffer/wrap parameter-bytes)
            result {:protocol protocol-version
                    :protocol-version protocol-version
                    :parameters {}}]
        (loop [r result]
          (let [key-start (.position parameters)
                key-length (loop [pos key-start]
                             (if (zero? (.get parameters pos))
                               (- pos key-start)
                               (recur (inc pos))))
                key-bytes (byte-array key-length)
                _ (.position parameters key-start)
                _ (.get parameters key-bytes 0 key-length)
                _ (.position parameters (+ key-start key-length 1))
                key (when (pos? key-length) (String. key-bytes StandardCharsets/UTF_8))]
            (if (or (nil? key) (zero? (count key)))
              r
              (let [value-start (.position parameters)
                    value-length (loop [pos value-start]
                                   (if (zero? (.get parameters pos))
                                     (- pos value-start)
                                     (recur (inc pos))))
                    value-bytes (byte-array value-length)
                    _ (.position parameters value-start)
                    _ (.get parameters value-bytes 0 value-length)
                    _ (.position parameters (+ value-start value-length 1))
                    value (String. value-bytes StandardCharsets/UTF_8)
                    updated-params (assoc (:parameters r) key value)]
                (recur (assoc r :parameters updated-params))))))))
    (catch Exception e
      (log/log-error (str "Error reading startup message: " (.getMessage e)))
      nil)))

;; Message sending functions
(defn send-authentication-ok
  "Sends an authentication OK message to the client.
   Parameters:
   - out: The output stream
   Returns: nil"
  [^OutputStream out]
  (let [buffer (ByteBuffer/allocate 4)]
    (.putInt buffer 0)  ;; Auth code 0 = OK

    (let [pos (.position buffer)
          final-data (byte-array pos)]
      (.flip buffer)
      (.get buffer final-data)
      (write-message out constants/PG_AUTHENTICATION_OK final-data))))

(defn send-error-response
  "Sends an error response message to the client.
   Parameters:
   - out: The output stream
   - message: The error message
   Returns: nil"
  [^OutputStream out message]
  (let [buffer (ByteBuffer/allocate 1024)  ;; Aumentar para 1KB, suficiente para a maioria das mensagens de erro
        ;; Add error fields
        _ (.put buffer (byte (int \S)))  ;; Severity field
        severity-bytes (.getBytes "ERROR" StandardCharsets/UTF_8)
        _ (.put buffer severity-bytes)
        _ (.put buffer (byte 0))  ;; Null terminator

        ;; Message field
        _ (.put buffer (byte (int \M)))  ;; Message field
        message-bytes (.getBytes message StandardCharsets/UTF_8)
        _ (.put buffer message-bytes)
        _ (.put buffer (byte 0))  ;; Null terminator

        ;; Final null terminator for the entire message
        _ (.put buffer (byte 0))

        pos (.position buffer)
        final-data (byte-array pos)]
    (.flip buffer)
    (.get buffer final-data)
    (write-message out constants/PG_ERROR_RESPONSE final-data)))

(defn send-notice-response
  "Sends a notice response message to the client.
   Parameters:
   - out: The output stream
   - message: The notice message
   Returns: nil"
  [^OutputStream out message]
  (let [buffer (ByteBuffer/allocate 1024)
        ;; Add notice fields
        _ (.put buffer (byte (int \S)))  ;; Severity field
        severity-bytes (.getBytes "NOTICE" StandardCharsets/UTF_8)
        _ (.put buffer severity-bytes)
        _ (.put buffer (byte 0))  ;; Null terminator

        ;; Message field
        _ (.put buffer (byte (int \M)))  ;; Message field
        message-bytes (.getBytes message StandardCharsets/UTF_8)
        _ (.put buffer message-bytes)
        _ (.put buffer (byte 0))  ;; Null terminator

        ;; Final null terminator for the entire message
        _ (.put buffer (byte 0))

        pos (.position buffer)
        final-data (byte-array pos)]
    (.flip buffer)
    (.get buffer final-data)
    (write-message out constants/PG_NOTICE_RESPONSE final-data)))

(defn send-parameter-status
  "Sends a parameter status message to inform client of settings
   Parameters:
   - out: The output stream
   - name: The parameter name
   - value: The parameter value
   Returns: nil"
  [^OutputStream out name value]
  (let [name-bytes (.getBytes name StandardCharsets/UTF_8)
        value-bytes (.getBytes value StandardCharsets/UTF_8)
        buffer (ByteBuffer/allocate (+ (count name-bytes) (count value-bytes) 2))]
    (.put buffer name-bytes)
    (.put buffer (byte 0))  ;; Null terminator
    (.put buffer value-bytes)
    (.put buffer (byte 0))  ;; Null terminator

    (let [pos (.position buffer)
          final-data (byte-array pos)]
      (.flip buffer)
      (.get buffer final-data)
      (write-message out constants/PG_PARAMETER_STATUS final-data))))

(defn send-command-complete
  "Sends a command complete message to the client.
   Parameters:
   - out: The output stream
   - command: The command that was executed (e.g., 'SELECT', 'INSERT')
   - rows: The number of rows affected
   Returns: nil"
  [^OutputStream out command rows]
  (let [command-str (cond
                      (= command "INSERT") "INSERT 0 1"  ;; For INSERT, the format should be "INSERT 0 1" where 0 is the OID and 1 is the row count
                      (and (= command "SELECT") (zero? rows)) "SELECT 0 0"  ;; Explicit format for zero results (0 rows)
                      :else (str command " " rows))
        command-bytes (.getBytes command-str StandardCharsets/UTF_8)
        buffer (ByteBuffer/allocate (+ (count command-bytes) 1))] ;; +1 for null terminator
    (.put buffer command-bytes)
    (.put buffer (byte 0))  ;; Null terminator for the string

    (let [pos (.position buffer)
          final-data (byte-array pos)]
      (.flip buffer)
      (.get buffer final-data)
      (write-message out constants/PG_COMMAND_COMPLETE final-data))))

(defn send-row-description
  "Sends a row description message.
   Parameters:
   - out: The output stream
   - columns: A list of column names
   Returns: nil"
  [^OutputStream out columns]
  (let [buffer (ByteBuffer/allocate (+ 2 (* (count columns) 128)))]  ;; Estimated size, 2 for field count + space for each field

    ;; Number of fields
    (.putShort buffer (short (count columns)))

    ;; For each column
    (doseq [column columns]
      ;; Column name
      (let [column-bytes (.getBytes (or column "") StandardCharsets/UTF_8)]
        (.put buffer column-bytes)
        (.put buffer (byte 0)))  ;; Null terminator

      ;; Table OID (0 = not part of a specific table)
      (.putInt buffer 0)

      ;; Column attribute number within the table (0 = not from a table)
      (.putShort buffer (short 0))

      ;; Data type OID (25 = TEXT)
      (.putInt buffer 25)  ;; 25 = TEXT

      ;; Data type size (4 bytes for INT4)
      (.putShort buffer (short -1))  ;; -1 = variable length

      ;; Type modifier (usually -1 = no specific modifier)
      (.putInt buffer -1)

      ;; Format code (0 = text, 1 = binary)
      (.putShort buffer (short 0)))

    ;; Reset buffer position for reading
    (.flip buffer)

    ;; Create byte array from buffer
    (let [size (.remaining buffer)
          message-body (byte-array size)]
      (.get buffer message-body)

      ;; Send the row description
      (write-message out constants/PG_ROW_DESCRIPTION message-body))))

(defn send-data-row
  "Send a message of type DataRow.
   Parameters:
   - out: The output stream
   - values: A list of column values
   Returns: nil"
  [^OutputStream out values]
  (let [buf (ByteBuffer/allocate constants/MAX_BUFFER_SIZE)]
    ;; Number of values
    (.putShort buf (short (count values)))
    ;; For each value
    (doseq [value values]
      (if (nil? value)
        ;; NULL value
        (.putInt buf -1)
        ;; Non-NULL value
        (let [bytes (try
                      (cond
                        ;; Handle byte arrays directly
                        (instance? (Class/forName "[B") value) value
                        ;; Numbers should be converted to string first
                        (instance? Number value) (.getBytes (str value) StandardCharsets/UTF_8)
                        ;; Handle strings
                        (string? value) (.getBytes (str value) StandardCharsets/UTF_8)
                        ;; Handle other types by converting to string first
                        :else (.getBytes (str value) StandardCharsets/UTF_8))
                      (catch Throwable _
                        (.getBytes (str value) StandardCharsets/UTF_8)))
              length (if (instance? (Class/forName "[B") bytes)
                       (alength ^bytes bytes)
                       (count bytes))]
          (.putInt buf length)
          (if (instance? (Class/forName "[B") bytes)
            (.put buf ^bytes bytes)
            ;; If bytes is not a byte array (which shouldn't happen but just in case)
            (let [stringBytes (.getBytes (str bytes) StandardCharsets/UTF_8)]
              (.put buf stringBytes))))))
    ;; Reset position for reading
    (.flip buf)
    ;; Create byte array from buffer
    (let [body-size (.remaining buf)
          body (byte-array body-size)]
      (.get buf body 0 body-size)
      ;; Send message
      (write-message out constants/PG_DATA_ROW body))))

(defn send-ready-for-query
  "Sends a ready for query message.
   Parameters:
   - out: The output stream
   - status: Transaction status (I=idle, T=in transaction, E=error)
   Returns: nil"
  [^OutputStream out status]
  (let [buffer (ByteBuffer/allocate 1)]
    (.put buffer (byte (int (or status \I))))  ;; Default to Idle status

    (let [pos (.position buffer)
          final-data (byte-array pos)]
      (.flip buffer)
      (.get buffer final-data)
      (write-message out constants/PG_READY_FOR_QUERY final-data))))
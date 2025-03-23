(ns chrondb.api.sql.protocol.messages
  "Protocol message handling for PostgreSQL wire protocol"
  (:require [chrondb.api.sql.protocol.constants :as constants]
            [chrondb.util.logging :as log])
  (:import [java.io OutputStream InputStream DataOutputStream DataInputStream ByteArrayOutputStream]
           [java.nio.charset StandardCharsets]
           [java.nio ByteBuffer]))

(defn write-bytes
  "Writes a byte array to an output stream and flushes it.
   Parameters:
   - out: The output stream
   - data: The byte array to write
   Returns: nil"
  [^OutputStream out ^bytes data]
  (.write out data 0 (alength data))
  (.flush out))

(defn write-message
  "Writes a PostgreSQL protocol message to an output stream.
   Parameters:
   - out: The output stream
   - type: The message type (a byte)
   - data: The message content as a byte array
   Returns: nil"
  [^OutputStream out type ^bytes data]
  (try
    (let [msg-length (+ 4 (alength data))
          dos (DataOutputStream. out)]
      ;; Message type (1 byte)
      (.writeByte dos (int type))

      ;; Message length (int - 4 bytes) in Big Endian format (Java default)
      (.writeInt dos msg-length)

      ;; Write the message content
      (.write dos data 0 (alength data))
      (.flush dos))
    (catch Exception e
      (log/log-error (str "Error writing message: " (.getMessage e))))))

(defn string-to-bytes
  "Converts a string to a null-terminated byte array.
   Parameters:
   - s: The string to convert
   Returns: A byte array containing the string bytes followed by a null terminator"
  [s]
  (let [s-bytes (.getBytes s StandardCharsets/UTF_8)
        result (byte-array (inc (alength s-bytes)))]
    ;; Copy the original string
    (System/arraycopy s-bytes 0 result 0 (alength s-bytes))
    ;; Add the null terminator
    (aset-byte result (alength s-bytes) (byte 0))
    result))

(defn read-null-terminated-string
  "Lê uma string terminada em nulo a partir de um array de bytes, começando em uma posição específica.
   Retorna um mapa com a string lida e a nova posição no array."
  [^bytes buffer pos]
  (let [start-pos pos]
    (loop [curr-pos pos]
      (if (or (>= curr-pos (alength buffer))
              (zero? (aget buffer curr-pos)))
        ;; Encontrou o terminador NULL ou fim do buffer
        (let [length (- curr-pos start-pos)
              string-bytes (byte-array length)]
          (System/arraycopy buffer start-pos string-bytes 0 length)
          {:string (String. string-bytes StandardCharsets/UTF_8)
           :new-pos (inc curr-pos)}) ; Pular o terminador NULL
        ;; Continue procurando pelo terminador
        (recur (inc curr-pos))))))

(defn read-startup-message
  "Reads the startup message from a PostgreSQL client.
   Parameters:
   - in: The input stream to read from
   Returns: A map containing protocol version, database name, and username"
  [^InputStream in]
  (try
    (let [dis (DataInputStream. in)
          ;; Read the message length (first field - 4 bytes)
          msg-length (.readInt dis)]
      (when (> msg-length 0)
        (let [content-length (- msg-length 4)
              buffer (byte-array content-length)]
          ;; Read the rest of the message
          (.readFully dis buffer 0 content-length)

          ;; Parse the protocol version (int - 4 bytes)
          (let [protocol-bytes (byte-array 4)]
            (System/arraycopy buffer 0 protocol-bytes 0 4)
            (let [protocol (.getInt (ByteBuffer/wrap protocol-bytes))
                  parameters (atom {})
                  pos (atom 4)]  ;; Start after the protocol version

              ;; Parse parameters (null-terminated strings)
              (loop []
                (when (< @pos (alength buffer))
                  (if (zero? (aget buffer @pos))
                    ;; End of parameters
                    (swap! pos inc)
                    (let [param-result (read-null-terminated-string buffer @pos)
                          param-name (:string param-result)]
                      (reset! pos (:new-pos param-result))

                      (when (< @pos (alength buffer))
                        (let [value-result (read-null-terminated-string buffer @pos)
                              param-value (:string value-result)]
                          (reset! pos (:new-pos value-result))
                          (swap! parameters assoc param-name param-value)
                          (recur)))))))

              ;; Create and return the result map
              {:protocol protocol
               :parameters @parameters})))))
    (catch Exception e
      (log/log-error (str "Error reading startup message: " (.getMessage e)))
      ;; Return default values to allow the process to continue
      {:protocol constants/PG_PROTOCOL_VERSION
       :parameters {"user" "chrondb"
                    "database" "chrondb"}})))

(defn send-authentication-ok
  "Sends an authentication OK message to the client.
   Parameters:
   - out: The output stream
   Returns: nil"
  [^OutputStream out]
  (let [buffer (ByteBuffer/allocate 4)]
    (.putInt buffer 0)  ;; Successful authentication (0)
    (write-message out constants/PG_AUTHENTICATION_OK (.array buffer))))

(defn send-parameter-status
  "Sends a parameter status message to the client.
   Parameters:
   - out: The output stream
   - param: The parameter name
   - value: The parameter value
   Returns: nil"
  [^OutputStream out param value]
  (let [buffer (ByteBuffer/allocate 1024)
        param-bytes (.getBytes param StandardCharsets/UTF_8)
        value-bytes (.getBytes value StandardCharsets/UTF_8)]

    ;; Add the parameter name
    (.put buffer param-bytes)
    (.put buffer (byte 0))  ;; Null terminator for the name

    ;; Add the parameter value
    (.put buffer value-bytes)
    (.put buffer (byte 0))  ;; Null terminator for the value

    (let [pos (.position buffer)
          final-data (byte-array pos)]
      (.flip buffer)
      (.get buffer final-data)
      (write-message out constants/PG_PARAMETER_STATUS final-data))))

(defn send-backend-key-data
  "Sends backend key data to the client.
   Parameters:
   - out: The output stream
   Returns: nil"
  [^OutputStream out]
  (let [buffer (ByteBuffer/allocate 8)]
    ;; Process ID (4 bytes)
    (.putInt buffer 12345)
    ;; Secret key (4 bytes)
    (.putInt buffer 67890)
    (write-message out constants/PG_BACKEND_KEY_DATA (.array buffer))))

(defn send-ready-for-query
  "Sends a ready for query message to the client.
   Parameters:
   - out: The output stream
   Returns: nil"
  [^OutputStream out]
  (let [buffer (ByteBuffer/allocate 1)]
    ;; Status: 'I' = idle (ready for queries)
    (.put buffer (byte (int \I)))
    (write-message out constants/PG_READY_FOR_QUERY (.array buffer))))

(defn send-error-response
  "Sends an error response message to the client.
   Parameters:
   - out: The output stream
   - message: The error message
   Returns: nil"
  [^OutputStream out message]
  (let [buffer (ByteBuffer/allocate 1024)]
    ;; Add error fields
    (.put buffer (byte (int \S)))   ;; Severity field identifier
    (.put buffer (.getBytes "ERROR" StandardCharsets/UTF_8))
    (.put buffer (byte 0))

    (.put buffer (byte (int \C)))  ;; Code field identifier
    (.put buffer (.getBytes "42501" StandardCharsets/UTF_8))  ;; Permission denied (for UPDATE/DELETE without WHERE)
    (.put buffer (byte 0))

    (.put buffer (byte (int \M)))  ;; Message field identifier
    (.put buffer (.getBytes message StandardCharsets/UTF_8))
    (.put buffer (byte 0))

    (.put buffer (byte (int \H)))  ;; Hint field identifier
    (.put buffer (.getBytes "Add a WHERE clause with a specific ID" StandardCharsets/UTF_8))
    (.put buffer (byte 0))

    (.put buffer (byte (int \P)))  ;; Position field identifier
    (.put buffer (.getBytes "1" StandardCharsets/UTF_8))
    (.put buffer (byte 0))

    (.put buffer (byte 0))  ;; Final terminator

    (let [pos (.position buffer)
          final-data (byte-array pos)]
      (.flip buffer)
      (.get buffer final-data)
      (write-message out constants/PG_ERROR_RESPONSE final-data))))

(defn send-command-complete
  "Sends a command complete message to the client.
   Parameters:
   - out: The output stream
   - command: The command that was executed (e.g., 'SELECT', 'INSERT')
   - rows: The number of rows affected
   Returns: nil"
  [^OutputStream out command rows]
  (let [buffer (ByteBuffer/allocate 128)
        ;; The format depends on the command
        command-str (cond
                      (= command "INSERT") "INSERT 0 1"  ;; For INSERT, the format should be "INSERT 0 1" where 0 is the OID and 1 is the row count
                      (and (= command "SELECT") (zero? rows)) "SELECT 0 0"  ;; Explicit format for zero results (0 rows)
                      :else (str command " " rows))]
    (log/log-debug (str "Sending command complete: " command-str))
    (.put buffer (.getBytes command-str StandardCharsets/UTF_8))
    (.put buffer (byte 0))  ;; Null terminator for the string

    (let [pos (.position buffer)
          final-data (byte-array pos)]
      (.flip buffer)
      (.get buffer final-data)
      (write-message out constants/PG_COMMAND_COMPLETE final-data))))

(defn send-row-description
  "Sends a row description message to the client.
   Parameters:
   - out: The output stream
   - columns: A sequence of column names
   Returns: nil"
  [^OutputStream out columns]
  (let [buffer (ByteBuffer/allocate 1024)]
    ;; Number of fields (2 bytes)
    (.putShort buffer (short (count columns)))

    ;; Information for each column (only if there are columns)
    (when (seq columns)
      (doseq [col columns]
        ;; Column name (null-terminated string)
        (.put buffer (.getBytes col StandardCharsets/UTF_8))
        (.put buffer (byte 0))

        ;; Table OID (4 bytes)
        (.putInt buffer 0)

        ;; Attribute number (2 bytes)
        (.putShort buffer (short 0))

        ;; Data type OID (4 bytes) - VARCHAR
        (.putInt buffer 25)  ;; TEXT

        ;; Type size (2 bytes)
        (.putShort buffer (short -1))

        ;; Type modifier (4 bytes)
        (.putInt buffer -1)

        ;; Format code (2 bytes) - 0=text
        (.putShort buffer (short 0))))

    (let [pos (.position buffer)
          final-data (byte-array pos)]
      (.flip buffer)
      (.get buffer final-data)
      (write-message out constants/PG_ROW_DESCRIPTION final-data))))

(defn send-data-row
  "Sends a data row message"
  [^OutputStream out values]
  (log/log-debug (str "Enviando linha de dados: " values))
  (let [buffer (ByteBuffer/allocate (+ 2 (* (count values) 256)))]  ;; Estimated size
    ;; Number of values in the row
    (.putShort buffer (short (count values)))

    ;; For each value
    (doseq [value values]
      (if (nil? value)
        ;; For NULL values, write a length of -1
        (.putInt buffer -1)
        ;; For non-NULL, write the value's length and then the value itself
        (let [str-value (str value)
              bytes (.getBytes str-value "UTF-8")]
          (.putInt buffer (count bytes))
          (.put buffer bytes))))

    ;; Reset buffer position for reading
    (.flip buffer)

    ;; Create byte array from buffer
    (let [size (.remaining buffer)
          message-body (byte-array size)]
      (.get buffer message-body)

      ;; Send the data row
      (write-message out constants/PG_DATA_ROW message-body))))
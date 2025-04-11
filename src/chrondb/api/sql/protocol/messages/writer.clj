(ns chrondb.api.sql.protocol.messages.writer
  "Functions for writing PostgreSQL protocol messages"
  (:require [chrondb.api.sql.protocol.constants :as constants]
            [chrondb.util.logging :as log])
  (:import [java.io OutputStream]
           [java.nio ByteBuffer]
           [java.nio.charset StandardCharsets]))

(defn- prepare-int-bytes
  "Prepara os bytes de um número inteiro para escrita em um buffer"
  [n]
  [(bit-and (bit-shift-right n 24) 0xFF)
   (bit-and (bit-shift-right n 16) 0xFF)
   (bit-and (bit-shift-right n 8) 0xFF)
   (bit-and n 0xFF)])

(defn- write-bytes
  "Escreve bytes para um OutputStream"
  [^OutputStream out bytes]
  (doseq [b bytes]
    (.write out (int b))))

;; Funções principais para escrita de mensagens
(defn write-message
  "Escreve uma mensagem com um tipo e corpo para um OutputStream"
  [^OutputStream out type body]
  (try
    (let [is-byte-array (instance? (Class/forName "[B") body)
          body-bytes (if is-byte-array
                       ^bytes body
                       (.getBytes (str body) StandardCharsets/UTF_8))
          length (+ 4 (count body-bytes))]

      ;; Message type (except for startup message)
      (when-not (= type constants/PG_STARTUP_MESSAGE)
        (.write out (int type)))

      ;; Message length (int32)
      (write-bytes out (prepare-int-bytes length))

      ;; Message body
      (.write out body-bytes)

      ;; Garantir que tudo seja enviado para o cliente
      (.flush out)

      ;; Pequena pausa para evitar sobrecarga - ajustado para ser maior
      (Thread/sleep 5))
    (catch java.net.SocketException e
      (log/log-warn (str "Socket error while writing message of type: " (when type (char type)) " - " (.getMessage e)))
      nil)
    (catch Exception e
      (log/log-error (str "Error writing message: " (.getMessage e) " for type: " (when type (char type))))
      nil)))

;; Funções específicas para mensagens
(defn send-authentication-ok
  "Sends an authentication OK message"
  [^OutputStream out]
  (let [buffer (ByteBuffer/allocate 4)]
    (.putInt buffer 0)  ;; Auth code 0 = OK
    (.flip buffer)
    (let [bytes (byte-array (.limit buffer))]
      (.get buffer bytes)
      (write-message out constants/PG_AUTHENTICATION_OK bytes))))

(defn send-error-response
  "Sends an error response message"
  [^OutputStream out message]
  (let [buffer (ByteBuffer/allocate 1024)
        ;; Severity field: 'S' + "ERROR" + null byte
        _ (.put buffer (byte (int \S)))
        _ (.put buffer (.getBytes "ERROR" StandardCharsets/UTF_8))
        _ (.put buffer (byte 0))

        ;; Message field: 'M' + message + null byte
        _ (.put buffer (byte (int \M)))
        _ (.put buffer (.getBytes message StandardCharsets/UTF_8))
        _ (.put buffer (byte 0))

        ;; Final null terminator
        _ (.put buffer (byte 0))

        ;; Prepare final data
        _ (.flip buffer)
        bytes (byte-array (.limit buffer))]

    (.get buffer bytes)
    (write-message out constants/PG_ERROR_RESPONSE bytes)))

(defn send-notice-response
  "Sends a notice response message"
  [^OutputStream out message]
  (let [buffer (ByteBuffer/allocate 1024)
        ;; Severity field
        _ (.put buffer (byte (int \S)))
        _ (.put buffer (.getBytes "NOTICE" StandardCharsets/UTF_8))
        _ (.put buffer (byte 0))

        ;; Message field
        _ (.put buffer (byte (int \M)))
        _ (.put buffer (.getBytes message StandardCharsets/UTF_8))
        _ (.put buffer (byte 0))

        ;; Final null terminator
        _ (.put buffer (byte 0))

        _ (.flip buffer)
        bytes (byte-array (.limit buffer))]

    (.get buffer bytes)
    (write-message out constants/PG_NOTICE_RESPONSE bytes)))

(defn send-parameter-status
  "Sends a parameter status message"
  [^OutputStream out name value]
  (let [name-bytes (.getBytes name StandardCharsets/UTF_8)
        value-bytes (.getBytes value StandardCharsets/UTF_8)
        buffer (ByteBuffer/allocate (+ (count name-bytes) (count value-bytes) 2))]

    (.put buffer name-bytes)
    (.put buffer (byte 0))
    (.put buffer value-bytes)
    (.put buffer (byte 0))

    (.flip buffer)
    (let [bytes (byte-array (.limit buffer))]
      (.get buffer bytes)
      (write-message out constants/PG_PARAMETER_STATUS bytes))))

(defn send-command-complete
  "Sends a command complete message"
  [^OutputStream out command rows]
  (let [command-str (cond
                      (= command "INSERT") "INSERT 0 1"
                      (and (= command "SELECT") (zero? rows)) "SELECT 0"
                      :else (str command " " rows))
        command-bytes (.getBytes command-str StandardCharsets/UTF_8)
        buffer (ByteBuffer/allocate (inc (count command-bytes)))]

    (.put buffer command-bytes)
    (.put buffer (byte 0))

    (.flip buffer)
    (let [bytes (byte-array (.limit buffer))]
      (.get buffer bytes)
      (write-message out constants/PG_COMMAND_COMPLETE bytes))))

(defn send-ready-for-query
  "Sends a ready for query message"
  [^OutputStream out state]
  (let [buffer (ByteBuffer/allocate 1)]
    (.put buffer (byte (int state)))
    (.flip buffer)
    (let [bytes (byte-array 1)]
      (.get buffer bytes)
      (write-message out constants/PG_READY_FOR_QUERY bytes))))

(defn send-row-description
  "Sends a row description message.
   Accepts columns in string or map format."
  [^OutputStream out columns]
  (try
    (log/log-info (str "Sending row description with " (count columns) " columns: " (pr-str columns)))

    (let [buffer-size (* 256 (max 1 (count columns)))  ;; Increase buffer size and ensure minimum
          buffer (ByteBuffer/allocate buffer-size)]

      ;; Number of fields
      (.putShort buffer (short (count columns)))

      ;; Each column description
      (doseq [column columns]
        (cond
          ;; Case 1: Column is a simple string
          (string? column)
          (do
            ;; Column name + null terminator
            (.put buffer (.getBytes column StandardCharsets/UTF_8))
            (.put buffer (byte 0))

            ;; Table OID (0 = doesn't belong to a specific table)
            (.putInt buffer 0)

            ;; Column OID (0 = virtual columns)
            (.putShort buffer (short 0))

            ;; Type OID (PostgreSQL type)
            (.putInt buffer 25)  ;; Default for text

            ;; Type size
            (.putShort buffer (short -1))  ;; -1 = variable size

            ;; Type modifier
            (.putInt buffer -1)

            ;; Format (0 = text, 1 = binary)
            (.putShort buffer (short 0)))

          ;; Case 2: Column is a map with properties
          (map? column)
          (let [{:keys [name type-id type-size type-mod format] :or {format 0}} column]
            ;; Column name + null terminator
            (.put buffer (.getBytes name StandardCharsets/UTF_8))
            (.put buffer (byte 0))

            ;; Table OID (0 = doesn't belong to a specific table)
            (.putInt buffer 0)

            ;; Column OID (0 = virtual columns)
            (.putShort buffer (short 0))

            ;; Type OID (PostgreSQL type)
            (.putInt buffer (or type-id 25))  ;; Default for text if not specified

            ;; Type size
            (.putShort buffer (short (or type-size -1)))  ;; -1 = variable size

            ;; Type modifier
            (.putInt buffer (or type-mod -1))

            ;; Format (0 = text, 1 = binary)
            (.putShort buffer (short format)))))

      (.flip buffer)
      (let [bytes (byte-array (.limit buffer))]
        (.get buffer bytes)

        ;; Send message directly without using write-message
        (.write out (int \T))
        (write-bytes out (prepare-int-bytes (+ 4 (count bytes))))
        (.write out bytes)
        (.flush out)))

    (catch Exception e
      (log/log-error (str "Error sending row description: " (.getMessage e)))
      (try
        (send-error-response out (str "Error in row description: " (.getMessage e)))
        (send-ready-for-query out \E)
        (catch Exception inner-e
          (log/log-error (str "Failed to send error response: " (.getMessage inner-e))))))))

(defn send-data-row
  "Sends a data row message."
  [^OutputStream out values]
  (try
    (log/log-info (str "Sending data row with " (count values) " values"))

    ;; Prepare bytes for each value in advance
    (let [value-bytes (mapv (fn [value]
                              (if (nil? value)
                                nil  ;; Marker for null values
                                (let [str-value (str value)]
                                  (.getBytes str-value StandardCharsets/UTF_8))))
                            values)

          ;; Calculate total buffer size needed
          total-size (+ 2  ;; Number of columns (short)
                        (* 4 (count values))  ;; Size of each value (int for each column)
                        (reduce + 0 (map #(if % (count %) 0) value-bytes)))  ;; Sum of bytes for each value

          ;; Create buffer with adequate size
          buffer (ByteBuffer/allocate (+ total-size 100))]  ;; Add safety margin

      ;; Number of columns (exactly as in row description)
      (.putShort buffer (short (count values)))

      ;; Value of each column
      (doseq [bytes value-bytes]
        (if (nil? bytes)
          ;; Null value: size -1
          (.putInt buffer -1)
          (do
            ;; Size of value in bytes
            (.putInt buffer (int (count bytes)))
            ;; Value bytes
            (.put buffer bytes))))

      ;; Prepare buffer for sending
      (.flip buffer)
      (let [final-bytes (byte-array (.limit buffer))]
        (.get buffer final-bytes)

        ;; Send 'D' message with the prepared bytes
        (.write out (int \D))
        ;; Message length (int32) + body size
        (write-bytes out (prepare-int-bytes (+ 4 (count final-bytes))))
        ;; Message body
        (.write out final-bytes)
        ;; Ensure flush
        (.flush out)))

    (catch Exception e
      (log/log-error (str "Error sending data row: " (.getMessage e)))
      ;; Ensure we don't break the protocol by sending an error message
      (try
        (send-error-response out (str "Error sending data row: " (.getMessage e)))
        (send-ready-for-query out \E)
        (catch Exception inner-e
          (log/log-error (str "Failed to send error response: " (.getMessage inner-e))))))))
(ns chrondb.api.sql.protocol.messages.reader
  "Functions for reading PostgreSQL protocol messages"
  (:require [chrondb.util.logging :as log])
  (:import [java.nio ByteBuffer]
           [java.nio.charset StandardCharsets]
           [java.io InputStream]))

;; Funções puras para leitura de dados básicos
(defn read-byte
  "Read a single byte from the input stream"
  [^InputStream in]
  (.read in))

(defn read-int
  "Read a 4-byte integer from the input stream"
  [^InputStream in]
  (->> [(read-byte in) (read-byte in) (read-byte in) (read-byte in)]
       (map-indexed (fn [idx b] (bit-shift-left (bit-and b 0xFF) (* 8 (- 3 idx)))))
       (reduce bit-or)))

(defn read-short
  "Read a 2-byte short from the input stream"
  [^InputStream in]
  (->> [(read-byte in) (read-byte in)]
       (map-indexed (fn [idx b] (bit-shift-left (bit-and b 0xFF) (* 8 (- 1 idx)))))
       (reduce bit-or)))

(defn read-null-terminated-string
  "Read a null-terminated string from the input stream"
  [^InputStream in]
  (loop [bytes []]
    (let [b (read-byte in)]
      (if (zero? b)
        (String. (byte-array bytes) StandardCharsets/UTF_8)
        (recur (conj bytes b))))))

;; Funções para leitura de mensagens específicas
(defn read-startup-message
  "Read a startup message from the client"
  [^InputStream in]
  (try
    (let [length (read-int in)
          protocol-version (read-int in)
          parameter-bytes (byte-array (- length 8))
          _ (.read in parameter-bytes)
          parameters (ByteBuffer/wrap parameter-bytes)

          ;; Helper function to read key-value pairs from the buffer
          read-key-value (fn [buffer]
                           (let [key-start (.position buffer)
                                 key-length (loop [pos key-start]
                                              (if (or (>= pos (.limit buffer)) (zero? (.get buffer pos)))
                                                (- pos key-start)
                                                (recur (inc pos))))
                                 key-valid? (pos? key-length)]

                             (when key-valid?
                               (let [key-bytes (byte-array key-length)
                                     _ (.position buffer key-start)
                                     _ (.get buffer key-bytes 0 key-length)
                                     _ (.position buffer (+ key-start key-length 1))
                                     key (String. key-bytes StandardCharsets/UTF_8)

                                     value-start (.position buffer)
                                     value-length (loop [pos value-start]
                                                    (if (or (>= pos (.limit buffer)) (zero? (.get buffer pos)))
                                                      (- pos value-start)
                                                      (recur (inc pos))))
                                     value-bytes (byte-array value-length)
                                     _ (.position buffer value-start)
                                     _ (.get buffer value-bytes 0 value-length)
                                     _ (.position buffer (+ value-start value-length 1))
                                     value (String. value-bytes StandardCharsets/UTF_8)]

                                 [key value]))))]

      ;; Build the parameter map by reading all key-value pairs
      (loop [result {:protocol-version protocol-version
                     :parameters {}}]
        (if (>= (.position parameters) (.limit parameters))
          result
          (if-let [[k v] (read-key-value parameters)]
            (recur (update result :parameters assoc k v))
            result))))
    (catch Exception e
      (log/log-error (str "Error reading startup message: " (.getMessage e)))
      nil)))

(defn read-query-message
  "Read a simple query message"
  [^InputStream in]
  (try
    (let [;; Read the message length
          length (read-int in)
          ;; The length includes the 4 bytes of the length field itself
          content-length (- length 4)
          ;; Read the rest of the message
          message-bytes (byte-array content-length)
          bytes-read (.read in message-bytes 0 content-length)
          ;; Get the SQL query text (ignoring the null byte at the end)
          query-text (String. message-bytes 0 (max 0 (dec bytes-read)) "UTF-8")]

      {:query query-text})
    (catch Exception e
      (log/log-error (str "Error reading query message: " (.getMessage e)))
      {:error (.getMessage e)})))
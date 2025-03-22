(ns chrondb.api.sql.sql-integration-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [chrondb.api.sql.core :as sql]
            [chrondb.api.sql.test-helpers :refer [create-test-resources]])
  (:import [java.net Socket]
           [java.io BufferedReader BufferedWriter InputStreamReader OutputStreamWriter]
           [java.nio.charset StandardCharsets]
           [java.nio ByteBuffer]))

;; Helper functions for PostgreSQL protocol client simulation
(defn connect-to-sql [host port]
  (let [socket (Socket. host port)]
    {:socket socket
     :reader (BufferedReader. (InputStreamReader. (.getInputStream socket) StandardCharsets/UTF_8))
     :writer (BufferedWriter. (OutputStreamWriter. (.getOutputStream socket) StandardCharsets/UTF_8))
     :input-stream (.getInputStream socket)
     :output-stream (.getOutputStream socket)}))

(defn close-sql-connection [conn]
  (.close (:writer conn))
  (.close (:reader conn))
  (.close (:socket conn)))

(defn send-startup-message [conn]
  (let [out (:output-stream conn)
        protocol-version (bit-or (bit-shift-left 3 16) 0)  ;; Version 3.0
        parameters {"user" "postgres"
                    "database" "chrondb"
                    "client_encoding" "UTF8"}

        ;; Calculate message size
        parameters-size (reduce + (map #(+ (count (first %)) 1 (count (second %)) 1) parameters))
        total-size (+ 4 4 parameters-size 1)  ;; Length(4) + Protocol(4) + params + NULL

        ;; Create message buffer
        buffer (ByteBuffer/allocate total-size)]

    ;; Write message length (including self but not including the message type)
    (.putInt buffer total-size)

    ;; Write protocol version
    (.putInt buffer protocol-version)

    ;; Write parameters as key-value pairs
    (doseq [[k v] parameters]
      (.put buffer (.getBytes k StandardCharsets/UTF_8))
      (.put buffer (byte 0))  ;; NULL terminator
      (.put buffer (.getBytes v StandardCharsets/UTF_8))
      (.put buffer (byte 0)))  ;; NULL terminator

    ;; Terminate parameters with NULL
    (.put buffer (byte 0))

    ;; Send the message
    (.write out (.array buffer))
    (.flush out)))

(defn send-query [conn query-text]
  (let [out (:output-stream conn)
        query-bytes (.getBytes query-text StandardCharsets/UTF_8)

        ;; Message: 'Q' + length(4) + query + NULL terminator
        message-length (+ 4 (count query-bytes) 1)
        buffer (ByteBuffer/allocate (+ 1 message-length))]

    ;; Message type
    (.put buffer (byte (int \Q)))

    ;; Length (including self but not including the message type)
    (.putInt buffer message-length)

    ;; Query text
    (.put buffer query-bytes)

    ;; NULL terminator
    (.put buffer (byte 0))

    ;; Send the message
    (.write out (.array buffer))
    (.flush out)))

(defn read-message [conn]
  (let [in (:input-stream conn)
        type (.read in)]
    (when (pos? type)
      (let [byte-buffer (byte-array 4)]
        (.readNBytes in byte-buffer 0 4)
        (let [buffer (ByteBuffer/wrap byte-buffer)
              length (.getInt buffer)
              content-length (- length 4)
              content (byte-array content-length)]
          (.readNBytes in content 0 content-length)
          {:type (char type)
           :length length
           :content content})))))

(defn send-terminate [conn]
  (let [out (:output-stream conn)
        buffer (ByteBuffer/allocate 5)]

    ;; Message type 'X'
    (.put buffer (byte (int \X)))

    ;; Length (4 bytes)
    (.putInt buffer 4)

    ;; Send the message
    (.write out (.array buffer))
    (.flush out)))

;; Integration test with a PostgreSQL protocol client
(deftest ^:integration test-sql-integration
  (testing "SQL client integration"
    (let [{storage :storage index :index} (create-test-resources)
          port 15432  ;; Use a port different from the default PostgreSQL port
          server (sql/start-sql-server storage index port)]
      (try
        ;; Connect to the SQL server
        (let [conn (connect-to-sql "localhost" port)]
          (try
            ;; Send startup message
            (testing "Connection startup"
              (send-startup-message conn)
              (let [auth-message (read-message conn)]
                (is (= \R (char (:type auth-message)))))

              ;; Skip reading several parameter status messages
              (dotimes [_ 5]
                (read-message conn)))

            ;; Test a simple query
            (testing "Simple SELECT query"
              (send-query conn "SELECT 1 as test")

              ;; Skip various response messages to get to the result
              (dotimes [_ 3]
                (read-message conn)))

            ;; Terminate the connection
            (testing "Connection termination"
              (send-terminate conn))

            (finally
              (close-sql-connection conn))))
        (finally
          (sql/stop-sql-server server))))))

;; Define a fixture that can be used to run only integration tests
(defn integration-fixture [f]
  (f))

;; Use the fixture for integration tests
(use-fixtures :once integration-fixture)
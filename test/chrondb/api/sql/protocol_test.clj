(ns chrondb.api.sql.protocol-test
  (:require [clojure.test :refer [deftest is testing]]
            [chrondb.api.sql.protocol.messages :as messages])
  (:import [java.io ByteArrayOutputStream DataOutputStream ByteArrayInputStream DataInputStream]
           [java.nio ByteBuffer]
           [java.nio.charset StandardCharsets]))

;; Helper function to test message encoding/decoding
(defn create-output-stream []
  (ByteArrayOutputStream.))

(defn create-data-output-stream []
  (DataOutputStream. (create-output-stream)))

(defn get-output-bytes [stream]
  (when (instance? DataOutputStream stream)
    (.flush stream))
  (when (instance? ByteArrayOutputStream stream)
    (.toByteArray stream)))

(defn create-input-stream [bytes]
  (ByteArrayInputStream. bytes))

(defn create-data-input-stream [bytes]
  (DataInputStream. (create-input-stream bytes)))

;; Test message construction and parsing
(deftest test-message-construction
  (testing "Authentication OK message"
    (let [out (create-output-stream)]
      (messages/send-authentication-ok out)
      (let [bytes (get-output-bytes out)
            type (char (aget bytes 0))
            buffer (ByteBuffer/wrap bytes 1 4)
            length (.getInt buffer)]
        (is (= \R type))
        (is (= 8 length)))))

  (testing "Parameter Status message"
    (let [out (create-output-stream)]
      (messages/send-parameter-status out "server_version" "14.0")
      (let [bytes (get-output-bytes out)
            type (char (aget bytes 0))]
        (is (= \S type)))))

  (testing "Ready for Query message"
    (let [out (create-output-stream)]
      (messages/send-ready-for-query out \I)
      (let [bytes (get-output-bytes out)
            type (char (aget bytes 0))
            status (char (aget bytes 5))]
        (is (= \Z type))
        (is (= \I status)))))

  (testing "Error Response message"
    (let [out (create-output-stream)]
      (messages/send-error-response out "Erro de teste")
      (let [bytes (get-output-bytes out)
            type (char (aget bytes 0))]
        (is (= \E type)))))

  (testing "Row Description message"
    (let [out (create-output-stream)]
      (messages/send-row-description out ["id" "nome" "valor"])
      (let [bytes (get-output-bytes out)
            type (char (aget bytes 0))
            buffer (ByteBuffer/wrap bytes 5 2)
            field-count (.getShort buffer)]
        (is (= \T type))
        (is (= 3 field-count)))))

  (testing "Data Row message"
    (let [out (create-output-stream)]
      (messages/send-data-row out ["1" "Teste" "100.5"])
      (let [bytes (get-output-bytes out)
            type (char (aget bytes 0))
            buffer (ByteBuffer/wrap bytes 5 2)
            field-count (.getShort buffer)]
        (is (= \D type))
        (is (= 3 field-count)))))

  (testing "Command Complete message"
    (let [out (create-output-stream)]
      (messages/send-command-complete out "SELECT" 10)
      (let [bytes (get-output-bytes out)
            type (char (aget bytes 0))]
        (is (= \C type))))))

;; Test reading startup message
(deftest test-startup-message-reading
  (testing "Reading startup message"
    (let [protocol-version (bit-or (bit-shift-left 3 16) 0)  ;; Version 3.0
          parameters {"user" "postgres"
                      "database" "chrondb"}

          ;; Calculate message size
          parameters-size (reduce + (map #(+ (count (first %)) 1 (count (second %)) 1) parameters))
          total-size (+ 4 4 parameters-size 1)  ;; Length(4) + Protocol(4) + params + NULL

          ;; Create message buffer
          buffer (ByteBuffer/allocate total-size)]

      ;; Write message length
      (.putInt buffer total-size)

      ;; Write protocol version
      (.putInt buffer protocol-version)

      ;; Write parameters
      (doseq [[k v] parameters]
        (.put buffer (.getBytes k StandardCharsets/UTF_8))
        (.put buffer (byte 0))
        (.put buffer (.getBytes v StandardCharsets/UTF_8))
        (.put buffer (byte 0)))

      ;; Terminate with NULL
      (.put buffer (byte 0))

      ;; Test reading the message
      (let [input-stream (create-input-stream (.array buffer))
            result (messages/read-startup-message input-stream)]
        (is (= "postgres" (get-in result [:parameters "user"])))
        (is (= "chrondb" (get-in result [:parameters "database"])))
        (is (= protocol-version (:protocol-version result)))))))

;; Test transaction status characters
(deftest test-transaction-status
  (testing "Transaction status characters"
    (is (= \I \I))  ;; Idle
    (is (= \T \T))  ;; In transaction block
    (is (= \E \E)))) ;; Error in transaction
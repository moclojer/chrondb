(ns chrondb.api.redis.redis-integration-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [chrondb.api.redis.core :as redis]
            [chrondb.storage.memory :as memory]
            [chrondb.index.lucene :as lucene]
            [chrondb.test-helpers :refer [with-test-data delete-directory]]
            [clojure.java.io :as io])
  (:import [java.net Socket]
           [java.io BufferedReader BufferedWriter InputStreamReader OutputStreamWriter]
           [java.nio.charset StandardCharsets]))

;; Helper functions for Redis client simulation
(defn connect-to-redis [host port]
  (let [socket (Socket. host port)]
    {:socket socket
     :reader (BufferedReader. (InputStreamReader. (.getInputStream socket) StandardCharsets/UTF_8))
     :writer (BufferedWriter. (OutputStreamWriter. (.getOutputStream socket) StandardCharsets/UTF_8))}))

(defn close-redis-connection [conn]
  (.close (:writer conn))
  (.close (:reader conn))
  (.close (:socket conn)))

(defn send-redis-command [conn command & args]
  (let [writer (:writer conn)
        full-command (concat [command] args)]
    (.write writer (str "*" (count full-command) "\r\n"))
    (doseq [arg full-command]
      (.write writer (str "$" (count (str arg)) "\r\n" arg "\r\n")))
    (.flush writer)))

(defn read-redis-response [conn]
  (let [reader (:reader conn)
        first-line (.readLine reader)
        type (first first-line)
        content (subs first-line 1)]
    (case type
      \+ content  ; Simple String
      \- (throw (ex-info content {}))  ; Error
      \: (Long/parseLong content)  ; Integer
      \$ (if (= content "-1")  ; Bulk String
           nil
           (let [len (Long/parseLong content)
                 data (char-array len)
                 _ (dotimes [i len]
                     (aset data i (char (.read reader))))
                 _ (.read reader)  ; consume CR
                 _ (.read reader)]  ; consume LF
             (String. data)))
      \* (let [count (Long/parseLong content)]  ; Array
           (if (neg? count)
             nil
             (vec (repeatedly count #(read-redis-response conn)))))
      (throw (ex-info (str "Unknown RESP type: " type) {})))))

;; Integration test with a real Redis client
(deftest ^:integration test-redis-integration
  (testing "Redis client integration"
    #_{:clj-kondo/ignore [:unresolved-symbol]}
    (with-test-data [storage index]
      (let [port 16379  ; Use a non-standard port for testing
            server (redis/start-redis-server storage index port)]
        (try
          ;; Connect to the Redis server
          (let [conn (connect-to-redis "localhost" port)]
            (try
              ;; Test PING command
              (testing "PING command"
                (send-redis-command conn "PING")
                (is (= "PONG" (read-redis-response conn))))

              ;; Test ECHO command
              (testing "ECHO command"
                (send-redis-command conn "ECHO" "hello")
                (is (= "hello" (read-redis-response conn))))

              ;; Test SET and GET commands
              (testing "SET and GET commands"
                (send-redis-command conn "SET" "test:key" "test-value")
                (is (= "OK" (read-redis-response conn)))

                (send-redis-command conn "GET" "test:key")
                (is (= "test-value" (read-redis-response conn))))

              ;; Test DEL command
              (testing "DEL command"
                (send-redis-command conn "DEL" "test:key")
                (is (= 1 (read-redis-response conn)))

                (send-redis-command conn "GET" "test:key")
                (is (nil? (read-redis-response conn))))

              ;; Test unknown command
              (testing "Unknown command"
                (try
                  (send-redis-command conn "UNKNOWN")
                  (read-redis-response conn)
                  (is false "Should have thrown an exception")
                  (catch Exception e
                    (is (= "ERR unknown command 'unknown'" (.getMessage e))))))

              (finally
                (close-redis-connection conn))))
          (finally
            (redis/stop-redis-server server)))))))

;; Define a fixture that can be used to run only integration tests
(defn integration-fixture [f]
  (f))

;; Use the fixture for integration tests
(use-fixtures :once integration-fixture)
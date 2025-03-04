(ns chrondb.api.redis.redis-test
  (:require [clojure.test :refer [deftest is testing]]
            [chrondb.api.redis.core :as redis]
            [chrondb.storage.memory :as memory]
            [chrondb.test-helpers :refer [with-test-data]])
  (:import [java.io StringReader StringWriter BufferedReader BufferedWriter]
           [java.net ServerSocket]))

;; Helper functions for testing
(defn create-string-reader [s]
  (BufferedReader. (StringReader. s)))

(defn create-string-writer []
  (let [sw (StringWriter.)
        bw (BufferedWriter. sw)]
    {:writer bw
     :string-writer sw}))

(defn get-writer-output [writer-map]
  (.flush (:writer writer-map))
  (str (.getBuffer (:string-writer writer-map))))

;; Test RESP Protocol Serialization Functions
(deftest test-write-simple-string
  (testing "Writing simple string"
    (let [writer (create-string-writer)]
      (redis/write-simple-string (:writer writer) "OK")
      (is (= "+OK\r\n" (get-writer-output writer))))))

(deftest test-write-error
  (testing "Writing error"
    (let [writer (create-string-writer)]
      (redis/write-error (:writer writer) "Error message")
      (is (= "-ERR Error message\r\n" (get-writer-output writer))))))

(deftest test-write-integer
  (testing "Writing integer"
    (let [writer (create-string-writer)]
      (redis/write-integer (:writer writer) 42)
      (is (= ":42\r\n" (get-writer-output writer))))))

(deftest test-write-bulk-string
  (testing "Writing bulk string"
    (let [writer (create-string-writer)]
      (redis/write-bulk-string (:writer writer) "hello")
      (is (= "$5\r\nhello\r\n" (get-writer-output writer)))))

  (testing "Writing nil bulk string"
    (let [writer (create-string-writer)]
      (redis/write-bulk-string (:writer writer) nil)
      (is (= "$-1\r\n" (get-writer-output writer))))))

(deftest test-write-array
  (testing "Writing array"
    (let [writer (create-string-writer)]
      (redis/write-array (:writer writer) ["hello" "world"])
      (is (= "*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n" (get-writer-output writer)))))

  (testing "Writing empty array"
    (let [writer (create-string-writer)]
      (redis/write-array (:writer writer) [])
      (is (= "*0\r\n" (get-writer-output writer)))))

  (testing "Writing nil array"
    (let [writer (create-string-writer)]
      (redis/write-array (:writer writer) nil)
      (is (= "*-1\r\n" (get-writer-output writer))))))

;; Test RESP Protocol Parsing Functions
(deftest test-read-resp
  (testing "Reading simple string"
    (let [reader (create-string-reader "+OK\r\n")]
      (is (= "OK" (redis/read-resp reader)))))

  (testing "Reading error"
    (let [reader (create-string-reader "-Error message\r\n")]
      (try
        (redis/read-resp reader)
        (is false "Should have thrown an exception")
        (catch clojure.lang.ExceptionInfo e
          (is (= "Error message" (.getMessage e)))))))

  (testing "Reading integer"
    (let [reader (create-string-reader ":42\r\n")]
      (is (= 42 (redis/read-resp reader)))))

  (testing "Reading bulk string"
    (let [reader (create-string-reader "$5\r\nhello\r\n")]
      (is (= "hello" (redis/read-resp reader)))))

  (testing "Reading nil bulk string"
    (let [reader (create-string-reader "$-1\r\n")]
      (is (nil? (redis/read-resp reader)))))

  (testing "Reading array"
    (let [reader (create-string-reader "*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n")]
      (is (= ["hello" "world"] (redis/read-resp reader)))))

  (testing "Reading empty array"
    (let [reader (create-string-reader "*0\r\n")]
      (is (= [] (redis/read-resp reader)))))

  (testing "Reading nil array"
    (let [reader (create-string-reader "*-1\r\n")]
      (is (nil? (redis/read-resp reader))))))

;; Test Command Handlers
(deftest test-handle-ping
  (testing "PING command"
    (let [writer (create-string-writer)]
      (redis/handle-ping (:writer writer) [])
      (is (= "+PONG\r\n" (get-writer-output writer))))))

(deftest test-handle-echo
  (testing "ECHO with argument"
    (let [writer (create-string-writer)]
      (redis/handle-echo (:writer writer) ["hello"])
      (is (= "$5\r\nhello\r\n" (get-writer-output writer)))))

  (testing "ECHO without arguments"
    (let [writer (create-string-writer)]
      (redis/handle-echo (:writer writer) [])
      (is (= "-ERR ERR wrong number of arguments for 'echo' command\r\n" (get-writer-output writer))))))

(deftest test-handle-get
  (testing "GET with storage"
    #_:clj-kondo/ignore
    (with-test-data [storage index]
      (let [doc {:id "test:key" :value "test-value"}
            _ (memory/save-document-memory (.data storage) doc)
            writer (create-string-writer)]

        (testing "GET with key"
          (redis/handle-get storage (:writer writer) ["test:key"])
          (is (= "$10\r\ntest-value\r\n" (get-writer-output writer))))

        (testing "GET with nonexistent key"
          (let [writer (create-string-writer)]
            (redis/handle-get storage (:writer writer) ["nonexistent"])
            (is (= "$-1\r\n" (get-writer-output writer)))))

        (testing "GET without arguments"
          (let [writer (create-string-writer)]
            (redis/handle-get storage (:writer writer) [])
            (is (= "-ERR ERR wrong number of arguments for 'get' command\r\n" (get-writer-output writer)))))))))

(deftest test-handle-set
  (testing "SET with storage and index"
    (with-test-data [storage index]
      (let [writer (create-string-writer)]

        (testing "SET with key and value"
          (redis/handle-set storage index (:writer writer) ["test:key" "test-value"])
          (is (= "+OK\r\n" (get-writer-output writer)))
          (is (= "test-value" (:value (memory/get-document-memory (.data storage) "test:key")))))

        (testing "SET without enough arguments"
          (let [writer (create-string-writer)]
            (redis/handle-set storage index (:writer writer) ["test:key"])
            (is (= "-ERR ERR wrong number of arguments for 'set' command\r\n" (get-writer-output writer)))))))))

(deftest test-handle-del
  (testing "DEL with storage"
    (with-test-data [storage index]
      (let [doc {:id "test:key" :value "test-value"}
            _ (memory/save-document-memory (.data storage) doc)
            writer (create-string-writer)]

        (testing "DEL with key"
          (redis/handle-del storage (:writer writer) ["test:key"])
          (is (= ":1\r\n" (get-writer-output writer)))
          (is (nil? (memory/get-document-memory (.data storage) "test:key"))))

        (testing "DEL with nonexistent key"
          (let [writer (create-string-writer)]
            (redis/handle-del storage (:writer writer) ["nonexistent"])
            (is (= ":0\r\n" (get-writer-output writer)))))

        (testing "DEL without arguments"
          (let [writer (create-string-writer)]
            (redis/handle-del storage (:writer writer) [])
            (is (= "-ERR ERR wrong number of arguments for 'del' command\r\n" (get-writer-output writer)))))))))

(deftest test-process-command
  (testing "Process commands with storage and index"
    (with-test-data [storage index]
      (testing "Process PING command"
        (let [writer (create-string-writer)]
          (redis/process-command storage index (:writer writer) "PING" [])
          (is (= "+PONG\r\n" (get-writer-output writer)))))

      (testing "Process unknown command"
        (let [writer (create-string-writer)]
          (redis/process-command storage index (:writer writer) "UNKNOWN" [])
          (is (= "-ERR unknown command 'unknown'\r\n" (get-writer-output writer))))))))

;; Test Server Functions
(deftest test-redis-server
  (testing "Start and stop Redis server"
    (with-test-data [storage index]
      (let [server (redis/start-redis-server storage index 0)]
        (is (not (nil? server)))
        (is (instance? ServerSocket server))
        (is (not (.isClosed server)))
        (redis/stop-redis-server server)
        (is (.isClosed server))))))
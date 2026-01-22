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
      (let [ctx-atom (atom (redis/create-connection-context))]
        (testing "Process PING command"
          (let [writer (create-string-writer)]
            (redis/process-command storage index (:writer writer) "PING" [] ctx-atom)
            (is (= "+PONG\r\n" (get-writer-output writer)))))

        (testing "Process unknown command"
          (let [writer (create-string-writer)]
            (redis/process-command storage index (:writer writer) "UNKNOWN" [] ctx-atom)
            (is (= "-ERR unknown command 'unknown'\r\n" (get-writer-output writer)))))))))

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

;; Test RESP3 Protocol Functions
(deftest test-write-null
  (testing "Writing RESP3 null"
    (let [writer (create-string-writer)]
      (redis/write-null (:writer writer))
      (is (= "_\r\n" (get-writer-output writer))))))

(deftest test-write-double
  (testing "Writing RESP3 double"
    (let [writer (create-string-writer)]
      (redis/write-double (:writer writer) 3.14)
      (is (= ",3.14\r\n" (get-writer-output writer)))))

  (testing "Writing positive infinity"
    (let [writer (create-string-writer)]
      (redis/write-double (:writer writer) Double/POSITIVE_INFINITY)
      (is (= ",inf\r\n" (get-writer-output writer)))))

  (testing "Writing negative infinity"
    (let [writer (create-string-writer)]
      (redis/write-double (:writer writer) Double/NEGATIVE_INFINITY)
      (is (= ",-inf\r\n" (get-writer-output writer))))))

(deftest test-write-boolean
  (testing "Writing RESP3 boolean true"
    (let [writer (create-string-writer)]
      (redis/write-boolean (:writer writer) true)
      (is (= "#t\r\n" (get-writer-output writer)))))

  (testing "Writing RESP3 boolean false"
    (let [writer (create-string-writer)]
      (redis/write-boolean (:writer writer) false)
      (is (= "#f\r\n" (get-writer-output writer))))))

(deftest test-write-map
  (testing "Writing RESP3 map"
    (let [writer (create-string-writer)]
      (redis/write-map (:writer writer) {:key "value"})
      (is (= "%1\r\n$3\r\nkey\r\n$5\r\nvalue\r\n" (get-writer-output writer))))))

;; Test Connection Context
(deftest test-connection-context
  (testing "Create connection context"
    (let [ctx (redis/create-connection-context)]
      (is (= 2 (:protocol-version ctx)))
      (is (nil? (:client-name ctx)))))

  (testing "Check RESP3 mode"
    (let [ctx2 (redis/->ConnectionContext 2 nil)
          ctx3 (redis/->ConnectionContext 3 "test-client")]
      (is (false? (redis/resp3? ctx2)))
      (is (true? (redis/resp3? ctx3))))))

;; Test Glob Pattern Matching
(deftest test-glob-to-regex
  (testing "Simple patterns"
    (is (redis/matches-pattern? "*" "anything"))
    (is (redis/matches-pattern? "user:*" "user:123"))
    (is (redis/matches-pattern? "user:*" "user:"))
    (is (not (redis/matches-pattern? "user:*" "other:123"))))

  (testing "Question mark pattern"
    (is (redis/matches-pattern? "user:???" "user:123"))
    (is (not (redis/matches-pattern? "user:???" "user:1234"))))

  (testing "Character class pattern"
    (is (redis/matches-pattern? "user:[abc]" "user:a"))
    (is (redis/matches-pattern? "user:[abc]" "user:b"))
    (is (not (redis/matches-pattern? "user:[abc]" "user:d"))))

  (testing "Nil and star pattern"
    (is (redis/matches-pattern? nil "anything"))
    (is (redis/matches-pattern? "*" "anything"))))

;; Test Cursor Encoding/Decoding
(deftest test-cursor-encoding
  (testing "Encode cursor"
    (is (= "0" (redis/encode-cursor 0)))
    (is (= "10" (redis/encode-cursor 10)))
    (is (= "100" (redis/encode-cursor 100))))

  (testing "Decode cursor"
    (is (= 0 (redis/decode-cursor "0")))
    (is (= 10 (redis/decode-cursor "10")))
    (is (= 100 (redis/decode-cursor "100")))))

;; Test SCAN Options Parsing
(deftest test-parse-scan-options
  (testing "Parse MATCH option"
    (let [opts (redis/parse-scan-options ["MATCH" "user:*"])]
      (is (= "user:*" (:match opts)))))

  (testing "Parse COUNT option"
    (let [opts (redis/parse-scan-options ["COUNT" "20"])]
      (is (= 20 (:count opts)))))

  (testing "Parse TYPE option"
    (let [opts (redis/parse-scan-options ["TYPE" "string"])]
      (is (= "string" (:type opts)))))

  (testing "Parse multiple options"
    (let [opts (redis/parse-scan-options ["MATCH" "user:*" "COUNT" "50" "TYPE" "hash"])]
      (is (= "user:*" (:match opts)))
      (is (= 50 (:count opts)))
      (is (= "hash" (:type opts)))))

  (testing "Default values"
    (let [opts (redis/parse-scan-options [])]
      (is (nil? (:match opts)))
      (is (= 10 (:count opts)))
      (is (nil? (:type opts))))))

;; Test History Cursor Encoding/Decoding
(deftest test-history-cursor-encoding
  (testing "Encode empty cursor"
    (is (= "0" (redis/encode-history-cursor {}))))

  (testing "Decode zero cursor"
    (let [result (redis/decode-history-cursor "0")]
      (is (= {:offset 0} result))))

  (testing "Decode nil cursor"
    (let [result (redis/decode-history-cursor nil)]
      (is (= {:offset 0} result))))

  (testing "Roundtrip encoding"
    (let [original {:offset 10 :commit-id "abc123"}
          encoded (redis/encode-history-cursor original)
          decoded (redis/decode-history-cursor encoded)]
      (is (= original decoded)))))

;; Test HISTORY Options Parsing
(deftest test-parse-history-options
  (testing "Parse CURSOR option"
    (let [opts (redis/parse-history-options ["CURSOR" "abc"])]
      (is (= "abc" (:cursor opts)))))

  (testing "Parse SINCE option"
    (let [opts (redis/parse-history-options ["SINCE" "1705920000"])]
      (is (= 1705920000 (:since opts)))))

  (testing "Parse COUNT option"
    (let [opts (redis/parse-history-options ["COUNT" "50"])]
      (is (= 50 (:count opts)))))

  (testing "Default values"
    (let [opts (redis/parse-history-options [])]
      (is (nil? (:cursor opts)))
      (is (nil? (:since opts)))
      (is (= 100 (:count opts))))))
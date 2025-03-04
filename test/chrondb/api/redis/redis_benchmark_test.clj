(ns chrondb.api.redis.redis-benchmark-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [chrondb.api.redis.core :as redis]
            [chrondb.test-helpers :refer [with-test-data]])
  (:import [java.net Socket]
           [java.io BufferedReader BufferedWriter InputStreamReader OutputStreamWriter]
           [java.nio.charset StandardCharsets]
           [java.util.concurrent Executors TimeUnit CountDownLatch]
           [java.time Instant Duration]
           [java.util Random]))

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
                 _ (.read reader data 0 len)
                 _ (.read reader 2)]  ; consume CRLF
             (String. data)))
      \* (let [count (Long/parseLong content)]  ; Array
           (if (neg? count)
             nil
             (vec (repeatedly count #(read-redis-response conn)))))
      (throw (ex-info (str "Unknown RESP type: " type) {})))))

;; Generate random data for benchmarking
(defn random-string [^Random rnd length]
  (let [chars "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
        chars-count (count chars)
        sb (StringBuilder.)]
    (dotimes [_ length]
      (.append sb (.charAt chars (.nextInt rnd chars-count))))
    (.toString sb)))

;; Benchmark functions
(defn run-benchmark [host port operations-config num-clients]
  (let [executor (Executors/newFixedThreadPool num-clients)
        total-operations (reduce + (map :count operations-config))
        latch (CountDownLatch. total-operations)
        start-time (Instant/now)
        rnd (Random.)
        operation-results (atom {})

        ;; Create a function to run a specific operation type
        run-operation (fn [conn op-type key value]
                        (let [start (System/nanoTime)]
                          (try
                            (case op-type
                              :set (do (send-redis-command conn "SET" key value)
                                       (read-redis-response conn))
                              :get (do (send-redis-command conn "GET" key)
                                       (read-redis-response conn))
                              :del (do (send-redis-command conn "DEL" key)
                                       (read-redis-response conn))
                              :incr (do (send-redis-command conn "INCR" key)
                                        (read-redis-response conn))
                              :lpush (do (send-redis-command conn "LPUSH" key value)
                                         (read-redis-response conn))
                              :rpop (do (send-redis-command conn "RPOP" key)
                                        (read-redis-response conn))
                              :ping (do (send-redis-command conn "PING")
                                        (read-redis-response conn))
                              :echo (do (send-redis-command conn "ECHO" value)
                                        (read-redis-response conn)))
                            (catch Exception e
                              (println (str "Error in operation " op-type ": " (.getMessage e))))
                            (finally
                              (let [duration-ns (- (System/nanoTime) start)]
                                (swap! operation-results update op-type
                                       (fn [stats]
                                         (if stats
                                           (-> stats
                                               (update :count inc)
                                               (update :total-ns + duration-ns)
                                               (update :min-ns #(if % (min % duration-ns) duration-ns))
                                               (update :max-ns #(if % (max % duration-ns) duration-ns)))
                                           {:count 1
                                            :total-ns duration-ns
                                            :min-ns duration-ns
                                            :max-ns duration-ns})))
                                (.countDown latch))))))]

    ;; Submit tasks to the executor
    (dotimes [client-id num-clients]
      (.submit executor
               ^Runnable
               (fn []
                 (let [conn (connect-to-redis host port)]
                   (try
                     (doseq [op-config operations-config]
                       (let [op-type (:type op-config)
                             op-count (:count op-config)
                             key-size (get op-config :key-size 10)
                             value-size (get op-config :value-size 100)
                             operations-per-client (quot op-count num-clients)
                             remainder (rem op-count num-clients)
                             client-op-count (if (< client-id remainder)
                                               (inc operations-per-client)
                                               operations-per-client)]
                         (dotimes [i client-op-count]
                           (let [key (str "bench:" (name op-type) ":" client-id ":" i ":" (random-string rnd key-size))
                                 value (random-string rnd value-size)]
                             (run-operation conn op-type key value)))))
                     (finally
                       (close-redis-connection conn)))))))

    ;; Wait for all operations to complete with a timeout
    (if-not (.await latch 60 TimeUnit/SECONDS) ; Add 60 second timeout
      (println "WARNING: Benchmark timed out after 60 seconds, results may be incomplete")
      nil)

    ;; Shutdown the executor
    (.shutdown executor)
    (if-not (.awaitTermination executor 30 TimeUnit/SECONDS) ; Reduce from 5 minutes to 30 seconds
      (do
        (.shutdownNow executor)
        (println "WARNING: Executor shutdown timed out, forcing termination"))
      nil)

    ;; Calculate and return benchmark results
    (let [end-time (Instant/now)
          duration (Duration/between start-time end-time)
          duration-ms (.toMillis duration)
          operations-per-second (/ total-operations (/ duration-ms 1000.0))
          results (reduce-kv
                   (fn [m k v]
                     (assoc m k (assoc v
                                       :avg-ms (/ (:total-ns v) (:count v) 1000000.0)
                                       :min-ms (/ (:min-ns v) 1000000.0)
                                       :max-ms (/ (:max-ns v) 1000000.0)
                                       :ops-per-sec (/ (:count v) (/ duration-ms 1000.0)))))
                   {}
                   @operation-results)]
      {:total-operations total-operations
       :duration-ms duration-ms
       :operations-per-second operations-per-second
       :operation-stats results})))

(defn print-benchmark-results [results]
  (println "\nBenchmark Results:")
  (println (str "Total operations: " (:total-operations results)))
  (println (str "Duration: " (:duration-ms results) " ms"))
  (println (str "Overall operations per second: " (format "%.2f" (:operations-per-second results))))

  (println "\nOperation Statistics:")
  (doseq [[op-type stats] (sort-by (comp name first) (:operation-stats results))]
    (println (str "  " (name op-type) ":"))
    (println (str "    Count: " (:count stats)))
    (println (str "    Avg latency: " (format "%.3f" (:avg-ms stats)) " ms"))
    (println (str "    Min latency: " (format "%.3f" (:min-ms stats)) " ms"))
    (println (str "    Max latency: " (format "%.3f" (:max-ms stats)) " ms"))
    (println (str "    Ops/sec: " (format "%.2f" (:ops-per-sec stats))))))

;; Benchmark test
(deftest ^:benchmark test-redis-benchmark
  (testing "Redis server benchmark"
    #_{:clj-kondo/ignore [:unresolved-symbol]}
    (with-test-data [storage index]
      (let [port 16381  ; Use a non-standard port for testing
            server (redis/start-redis-server storage index port)]
        (try
          ;; Simple operations benchmark
          (testing "Simple operations benchmark"
            (let [operations-config [{:type :ping, :count 100}
                                     {:type :set, :count 100, :key-size 10, :value-size 100}
                                     {:type :get, :count 100, :key-size 10}
                                     {:type :del, :count 50, :key-size 10}]
                  results (run-benchmark "localhost" port operations-config 2)]
              (print-benchmark-results results)
              (is (> (:operations-per-second results) 0))))

          ;; Data size impact benchmark
          (testing "Data size impact benchmark"
            (let [operations-config [{:type :set, :count 50, :key-size 10, :value-size 10}
                                     {:type :set, :count 50, :key-size 10, :value-size 1000}
                                     {:type :set, :count 50, :key-size 10, :value-size 5000}]
                  results (run-benchmark "localhost" port operations-config 1)]
              (print-benchmark-results results)
              (is (> (:operations-per-second results) 0))))

          ;; Concurrency impact benchmark
          (testing "Concurrency impact benchmark"
            (let [operations-config [{:type :set, :count 200, :key-size 10, :value-size 100}]
                  single-client-results (run-benchmark "localhost" port operations-config 1)
                  multi-client-results (run-benchmark "localhost" port operations-config 4)]
              (println "\nConcurrency Comparison:")
              (println (str "Single client ops/sec: "
                            (format "%.2f" (:operations-per-second single-client-results))))
              (println (str "Multi client ops/sec: "
                            (format "%.2f" (:operations-per-second multi-client-results))))
              (is (> (:operations-per-second multi-client-results) 0))))

          (finally
            (redis/stop-redis-server server)))))))

;; Define a fixture that can be used to run only benchmark tests
(defn benchmark-fixture [f]
  (f))

;; Use the fixture for benchmark tests
(use-fixtures :once benchmark-fixture)
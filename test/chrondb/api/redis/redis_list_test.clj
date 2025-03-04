(ns chrondb.api.redis.redis-list-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [chrondb.api.redis.server :as redis-server]
            [chrondb.storage.memory :as memory])
  (:import [redis.clients.jedis Jedis]))

(def ^:dynamic *redis-server* nil)
(def ^:dynamic *jedis* nil)

(defn redis-server-fixture [f]
  (let [storage (memory/create-memory-storage)
        server (redis-server/start-server storage 6380)]
    (binding [*redis-server* server
              *jedis* (Jedis. "localhost" 6380)]
      (try
        (f)
        (catch Exception e
          (println "Error in test:" (.getMessage e))
          (throw e))
        (finally
          (println "Closing Jedis connection and stopping Redis server...")
          (try
            (.close *jedis*)
            (catch Exception e
              (println "Warning: Failed to close Jedis connection:" (.getMessage e))))
          (try
            (redis-server/stop-server server)
            (catch Exception e
              (println "Warning: Failed to stop Redis server:" (.getMessage e))))
          ;; Espera adicional para garantir que a porta seja liberada
          (Thread/sleep 1000))))))

(use-fixtures :each redis-server-fixture)

(deftest test-lpush-rpush
  (testing "LPUSH and RPUSH commands"
    (let [key "mylist"]
      ;; Test LPUSH with single value
      (is (= 1 (.lpush *jedis* key (into-array String ["value1"]))))

      ;; Test LPUSH with multiple values using multiple calls
      (is (= 2 (.lpush *jedis* key (into-array String ["value2"]))))
      (is (= 3 (.lpush *jedis* key (into-array String ["value3"]))))

      ;; Test RPUSH with single value
      (is (= 4 (.rpush *jedis* key (into-array String ["value4"]))))

      ;; Test RPUSH with another value
      (is (= 5 (.rpush *jedis* key (into-array String ["value5"]))))

      ;; Verify list content with LRANGE
      (let [values (.lrange *jedis* key 0 -1)]
        (is (= 5 (count values)))
        (is (= ["value3" "value2" "value1" "value4" "value5"] (into [] values)))))))

(deftest test-lpop-rpop
  (testing "LPOP and RPOP commands"
    (let [key "poplist"]
      ;; Setup list
      (.lpush *jedis* key (into-array String ["value1"]))
      (.lpush *jedis* key (into-array String ["value2"]))
      (.lpush *jedis* key (into-array String ["value3"]))

      ;; Test LPOP
      (is (= "value3" (.lpop *jedis* key)))

      ;; Test RPOP
      (is (= "value1" (.rpop *jedis* key)))

      ;; Verify remaining content
      (let [values (.lrange *jedis* key 0 -1)]
        (is (= 1 (count values)))
        (is (= ["value2"] (into [] values))))

      ;; Pop the last element
      (is (= "value2" (.lpop *jedis* key)))

      ;; Verify empty list behavior
      (is (nil? (.lpop *jedis* key)))
      (is (nil? (.rpop *jedis* key))))))

(deftest test-lrange
  (testing "LRANGE command"
    (let [key "rangelist"]
      ;; Setup list
      (.rpush *jedis* key (into-array String ["value1"]))
      (.rpush *jedis* key (into-array String ["value2"]))
      (.rpush *jedis* key (into-array String ["value3"]))
      (.rpush *jedis* key (into-array String ["value4"]))
      (.rpush *jedis* key (into-array String ["value5"]))

      ;; Test different ranges
      (let [all-values (.lrange *jedis* key 0 -1)
            first-three (.lrange *jedis* key 0 2)
            last-two (.lrange *jedis* key -2 -1)
            middle (.lrange *jedis* key 1 3)
            empty-range (.lrange *jedis* key 10 20)]

        (is (= ["value1" "value2" "value3" "value4" "value5"] (into [] all-values)))
        (is (= ["value1" "value2" "value3"] (into [] first-three)))
        (is (= ["value4" "value5"] (into [] last-two)))
        (is (= ["value2" "value3" "value4"] (into [] middle)))
        (is (= 0 (count empty-range)))))))

(deftest test-llen
  (testing "LLEN command"
    (let [key "lenlist"]
      ;; Test on empty list
      (is (= 0 (.llen *jedis* key)))

      ;; Add elements and test
      (.rpush *jedis* key (into-array String ["value1"]))
      (.rpush *jedis* key (into-array String ["value2"]))
      (.rpush *jedis* key (into-array String ["value3"]))
      (is (= 3 (.llen *jedis* key)))

      ;; Remove elements and test
      (.lpop *jedis* key)
      (is (= 2 (.llen *jedis* key)))

      ;; Clear list and test
      (.lpop *jedis* key)
      (.lpop *jedis* key)
      (is (= 0 (.llen *jedis* key))))))
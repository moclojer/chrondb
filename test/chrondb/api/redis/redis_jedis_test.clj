(ns chrondb.api.redis.redis-jedis-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [chrondb.api.redis.core :as redis]
            [chrondb.test-helpers :refer [with-test-data]])
  (:import [redis.clients.jedis JedisPool JedisPoolConfig]
           [java.time Duration]))

;; Helper functions for Jedis client
(defn create-jedis-pool [host port]
  (let [pool-config (doto (JedisPoolConfig.)
                      (.setMaxTotal 16)
                      (.setMaxIdle 8)
                      (.setMinIdle 4)
                      (.setTestOnBorrow true)
                      (.setTestOnReturn true)
                      (.setTestWhileIdle true)
                      (.setMinEvictableIdleTimeMillis (.toMillis (Duration/ofSeconds 60)))
                      (.setTimeBetweenEvictionRunsMillis (.toMillis (Duration/ofSeconds 30)))
                      (.setNumTestsPerEvictionRun 3)
                      (.setBlockWhenExhausted true))]
    (JedisPool. pool-config host port)))

(defn with-jedis [pool f]
  (with-open [jedis (.getResource pool)]
    (f jedis)))

;; Test compatibility with Jedis client
(deftest ^:jedis test-jedis-compatibility
  (testing "Redis server compatibility with Jedis client"
    #_{:clj-kondo/ignore [:unresolved-symbol]}
    (with-test-data [storage index]
      (let [port 16392  ; Alterado de 16382 para 16392 para evitar conflitos
            server (redis/start-redis-server storage index port)
            pool (create-jedis-pool "localhost" port)]
        (try
          ;; Test basic string operations
          (testing "String operations"
            (with-jedis pool
              (fn [jedis]
                ;; SET and GET
                (is (= "OK" (.set jedis "jedis:test:string" "hello")))
                (is (= "hello" (.get jedis "jedis:test:string")))

                ;; SET with expiration
                (is (= "OK" (.setex jedis "jedis:test:string:ex" 3600 "expire-test")))
                (is (= "expire-test" (.get jedis "jedis:test:string:ex")))

                ;; SETNX (set if not exists)
                (is (= 1 (.setnx jedis "jedis:test:setnx" "new-value")))
                (is (= 0 (.setnx jedis "jedis:test:setnx" "another-value")))
                (is (= "new-value" (.get jedis "jedis:test:setnx"))))))

          ;; Test key operations
          (testing "Key operations"
            (with-jedis pool
              (fn [jedis]
                ;; EXISTS
                (is (.exists jedis "jedis:test:string"))
                (is (not (.exists jedis "jedis:test:nonexistent")))

                ;; DEL
                (.set jedis "jedis:test:to-delete" "delete-me")
                (is (= 1 (.del jedis (into-array String ["jedis:test:to-delete"]))))
                (is (nil? (.get jedis "jedis:test:to-delete"))))))

          ;; Test server commands
          (testing "Server commands"
            (with-jedis pool
              (fn [jedis]
                ;; PING
                (is (= "PONG" (.ping jedis)))

                ;; ECHO
                (is (= "hello jedis" (.echo jedis "hello jedis"))))))

          ;; Test hash operations
          (testing "Hash operations"
            (with-jedis pool
              (fn [jedis]
                ;; HSET and HGET
                (is (= 1 (.hset jedis "jedis:test:hash" "field1" "value1")))
                (is (= "value1" (.hget jedis "jedis:test:hash" "field1")))

                ;; HMSET and HMGET
                (let [hash-map (java.util.HashMap.)]
                  (.put hash-map "field2" "value2")
                  (.put hash-map "field3" "value3")
                  (is (= "OK" (.hmset jedis "jedis:test:hash" hash-map)))

                  ;; Usar hmget com um array de strings para os campos
                  (let [fields (into-array String ["field1" "field2" "field3"])
                        values (.hmget jedis "jedis:test:hash" fields)]
                    (is (= "value1" (.get values 0)))
                    (is (= "value2" (.get values 1)))
                    (is (= "value3" (.get values 2)))))

                ;; HGETALL
                (let [hash-map (.hgetAll jedis "jedis:test:hash")]
                  (is (= "value1" (.get hash-map "field1")))
                  (is (= "value2" (.get hash-map "field2")))
                  (is (= "value3" (.get hash-map "field3")))))))

          ;; Test list operations
          (testing "List operations"
            (with-jedis pool
              (fn [jedis]
                ;; LPUSH, RPUSH, LRANGE
                (.del jedis (into-array String ["jedis:test:list"]))
                (is (= 1 (.lpush jedis "jedis:test:list" (into-array String ["item1"]))))
                (is (= 2 (.rpush jedis "jedis:test:list" (into-array String ["item2"]))))
                (let [items (.lrange jedis "jedis:test:list" 0 -1)]
                  (is (= 2 (.size items)))
                  (is (= "item1" (.get items 0)))
                  (is (= "item2" (.get items 1))))

                ;; LPOP and RPOP
                (is (= "item1" (.lpop jedis "jedis:test:list")))
                (is (= "item2" (.rpop jedis "jedis:test:list")))
                (is (= 0 (.llen jedis "jedis:test:list"))))))

          ;; Test set operations
          (testing "Set operations"
            (with-jedis pool
              (fn [jedis]
                ;; SADD, SMEMBERS
                (.del jedis (into-array String ["jedis:test:set"]))
                (is (= 1 (.sadd jedis "jedis:test:set" (into-array String ["member1"]))))
                (is (= 1 (.sadd jedis "jedis:test:set" (into-array String ["member2"]))))
                (let [members (.smembers jedis "jedis:test:set")]
                  (is (= 2 (.size members)))
                  (is (.contains members "member1"))
                  (is (.contains members "member2")))

                ;; SISMEMBER
                (is (.sismember jedis "jedis:test:set" "member1"))
                (is (not (.sismember jedis "jedis:test:set" "nonexistent")))

                ;; SREM
                (is (= 1 (.srem jedis "jedis:test:set" (into-array String ["member1"]))))
                (let [members (.smembers jedis "jedis:test:set")]
                  (is (= 1 (.size members)))
                  (is (.contains members "member2"))))))

          ;; Test sorted set operations
          (testing "Sorted set operations"
            (with-jedis pool
              (fn [jedis]
                ;; ZADD, ZRANGE
                (.del jedis (into-array String ["jedis:test:zset"]))
                (is (= 1 (.zadd jedis "jedis:test:zset" 1.0 "member1")))
                (is (= 1 (.zadd jedis "jedis:test:zset" 2.0 "member2")))
                (let [members (.zrange jedis "jedis:test:zset" 0 -1)]
                  (is (= 2 (.size members)))
                  (is (= "member1" (.get members 0)))
                  (is (= "member2" (.get members 1))))

                ;; ZRANK, ZSCORE
                (is (= 0 (.zrank jedis "jedis:test:zset" "member1")))
                (is (not (nil? (.zscore jedis "jedis:test:zset" "member1"))))

                ;; ZREM
                (is (= 1 (.zrem jedis "jedis:test:zset" (into-array String ["member1"]))))
                (let [members (.zrange jedis "jedis:test:zset" 0 -1)]
                  (is (= 1 (.size members)))
                  (is (= "member2" (.get members 0)))))))

          (finally
            (.close pool)
            (redis/stop-redis-server server)))))))

;; Define a fixture that can be used to run only Jedis compatibility tests
(defn jedis-fixture [f]
  (f))

;; Use the fixture for Jedis compatibility tests
(use-fixtures :once jedis-fixture)
(ns chrondb.index.cache-test
  (:require [clojure.test :refer [deftest testing is]]
            [chrondb.index.cache :as cache]))

(deftest test-create-query-cache
  (testing "Create cache with defaults"
    (let [c (cache/create-query-cache)]
      (is (some? c))
      (is (instance? chrondb.index.cache.LRUQueryCache c))))

  (testing "Create cache with custom settings"
    (let [c (cache/create-query-cache :max-size 500 :ttl-ms 30000)
          stats (cache/cache-stats c)]
      (is (= 500 (:max-size stats)))
      (is (= 30000 (:ttl-ms stats))))))

(deftest test-cache-put-and-get
  (testing "Put and get value"
    (let [c (cache/create-query-cache)]
      (cache/cache-put! c :key1 "value1")
      (is (= "value1" (cache/cache-get c :key1)))))

  (testing "Get non-existent key returns nil"
    (let [c (cache/create-query-cache)]
      (is (nil? (cache/cache-get c :non-existent))))))

(deftest test-cache-invalidation
  (testing "Invalidate specific key"
    (let [c (cache/create-query-cache)]
      (cache/cache-put! c :key1 "value1")
      (cache/cache-put! c :key2 "value2")
      (cache/cache-invalidate! c :key1)
      (is (nil? (cache/cache-get c :key1)))
      (is (= "value2" (cache/cache-get c :key2)))))

  (testing "Invalidate all keys"
    (let [c (cache/create-query-cache)]
      (cache/cache-put! c :key1 "value1")
      (cache/cache-put! c :key2 "value2")
      (cache/cache-invalidate-all! c)
      (is (nil? (cache/cache-get c :key1)))
      (is (nil? (cache/cache-get c :key2))))))

(deftest test-cache-branch-invalidation
  (testing "Invalidate by branch"
    (let [c (cache/create-query-cache)]
      (cache/cache-put! c [(hash {:query :a}) "main" nil] "result1")
      (cache/cache-put! c [(hash {:query :b}) "main" nil] "result2")
      (cache/cache-put! c [(hash {:query :c}) "develop" nil] "result3")
      (cache/cache-invalidate-branch! c "main")
      (is (nil? (cache/cache-get c [(hash {:query :a}) "main" nil])))
      (is (nil? (cache/cache-get c [(hash {:query :b}) "main" nil])))
      (is (= "result3" (cache/cache-get c [(hash {:query :c}) "develop" nil]))))))

(deftest test-cache-ttl
  (testing "Entry expires after TTL"
    (let [c (cache/create-query-cache :ttl-ms 50)]
      (cache/cache-put! c :key1 "value1")
      (is (= "value1" (cache/cache-get c :key1)))
      (Thread/sleep 100)
      (is (nil? (cache/cache-get c :key1))))))

(deftest test-cache-lru-eviction
  (testing "LRU eviction when max size exceeded"
    (let [c (cache/create-query-cache :max-size 3)]
      (cache/cache-put! c :key1 "v1")
      (cache/cache-put! c :key2 "v2")
      (cache/cache-put! c :key3 "v3")
      ;; Access key1 to make it recently used
      (cache/cache-get c :key1)
      ;; Add key4, should evict key2 (least recently used)
      (cache/cache-put! c :key4 "v4")
      (is (= "v1" (cache/cache-get c :key1)))
      (is (= "v3" (cache/cache-get c :key3)))
      (is (= "v4" (cache/cache-get c :key4))))))

(deftest test-cache-stats
  (testing "Cache statistics tracking"
    (let [c (cache/create-query-cache)]
      (cache/cache-put! c :key1 "value1")
      (cache/cache-get c :key1) ; hit
      (cache/cache-get c :key1) ; hit
      (cache/cache-get c :key2) ; miss
      (let [stats (cache/cache-stats c)]
        (is (= 2 (:hits stats)))
        (is (= 1 (:misses stats)))
        (is (= 3 (:total stats)))
        (is (> (:hit-rate stats) 0.6))))))

(deftest test-cache-key-generation
  (testing "Generate cache key"
    (let [query-map {:field "name" :value "test"}
          branch "main"
          opts {:limit 10}
          key (cache/cache-key query-map branch opts)]
      (is (vector? key))
      (is (= 3 (count key)))
      (is (= branch (second key)))))

  (testing "Different queries produce different keys"
    (let [key1 (cache/cache-key {:a 1} "main" nil)
          key2 (cache/cache-key {:a 2} "main" nil)]
      (is (not= key1 key2))))

  (testing "Same query same key"
    (let [key1 (cache/cache-key {:a 1} "main" nil)
          key2 (cache/cache-key {:a 1} "main" nil)]
      (is (= key1 key2)))))

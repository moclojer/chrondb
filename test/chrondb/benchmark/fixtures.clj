(ns chrondb.benchmark.fixtures
  (:require [chrondb.storage.protocol :as storage]
            [chrondb.index.protocol :as index])
  (:import (java.util UUID)))

(defn generate-random-string [length]
  (apply str (take length (repeatedly #(char (+ (rand-int 26) 97))))))

(defn- generate-nested-data [depth max-depth]
  (if (>= depth max-depth)
    (generate-random-string (+ 10 (rand-int 90)))
    (let [choice (rand-int 3)]
      (case choice
        0 (generate-random-string (+ 100 (rand-int 900)))
        1 (into {} (for [_ (range (+ 2 (rand-int 5)))]
                     [(str "key" _) (generate-nested-data (inc depth) max-depth)]))
        2 (into [] (for [_ (range (+ 2 (rand-int 8)))]
                     (generate-nested-data (inc depth) max-depth)))))))

(defn generate-large-document [id table-name]
  (let [base-data {:id id
                   :_table table-name
                   :created_at (System/currentTimeMillis)
                   :title (generate-random-string (+ 20 (rand-int 80)))
                   :description (generate-random-string (+ 200 (rand-int 800)))
                   :tags (into [] (take (+ 3 (rand-int 7))
                                        (repeatedly #(generate-random-string (+ 5 (rand-int 15))))))
                   :metadata (into {} (for [_ (range (+ 5 (rand-int 10)))]
                                        [(str "meta_" (generate-random-string 8))
                                         (generate-nested-data 0 3)]))}]
    ;; Add additional data to reach desired size
    (assoc base-data
           :extra_data (generate-nested-data 0 4)
           :large_text (generate-random-string (+ 10000 (rand-int 20000))))))

(defn generate-benchmark-data
  "Generate benchmark data with at least 1GB of data volume.
   Returns the number of documents created."
  [storage index table-name num-docs branch]
  (println "Generating" num-docs "documents for benchmark tests...")
  (let [batch-size 100]
    (loop [i 0
           total-size 0]
      (if (and (< i num-docs) (< total-size (* 1024 1024 1024))) ;; 1GB in bytes
        (let [batch (for [_ (range batch-size)
                          :let [doc-id (str (UUID/randomUUID))
                                doc (generate-large-document doc-id table-name)
                                doc-size (count (pr-str doc))]]
                      [doc-id doc doc-size])
              batch-docs (mapv second batch)
              batch-ids (mapv first batch)
              new-size (+ total-size (reduce + 0 (map #(nth % 2) batch)))]

          ;; Store the batch of documents
          (doseq [[_ doc] (map vector batch-ids batch-docs)]
            (storage/save-document storage doc branch)
            (when index
              (index/index-document index doc)))

          (when (zero? (mod (+ i batch-size) 1000))
            (println (format "Generated %d documents, approx %.2f MB so far"
                             (+ i batch-size)
                             (/ new-size 1048576.0))))

          (recur (+ i batch-size) new-size))
        (do
          (println (format "Benchmark data generation complete: %d documents, %.2f MB"
                           i (/ total-size 1048576.0)))
          i)))))
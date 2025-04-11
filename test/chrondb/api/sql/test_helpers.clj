(ns chrondb.api.sql.test-helpers
  (:require [chrondb.storage.memory :as memory]
            [chrondb.index.memory :as memory-index]))

;; Explanation: The problem of unresolved symbols is occurring because the macro is being checked
;; by the linter before it is expanded. One way to resolve this is to make these variables be qualified
;; symbols with the namespace.

(defmacro with-test-data
  "Macro that creates a storage and an index for tests.
   Example of usage: (with-test-data [storage index] (test code))"
  [[storage-sym index-sym] & body]
  `(let [~storage-sym (memory/create-memory-storage)
         ~index-sym (memory-index/create-memory-index)]
     ~@body))

;; Direct function for cases where the macro causes problems with the linter
(defn create-test-resources []
  {:storage (memory/create-memory-storage)
   :index (memory-index/create-memory-index)})

;; Helper functions for string I/O testing
(defn create-string-reader [s]
  (java.io.BufferedReader. (java.io.StringReader. s)))

(defn create-string-writer []
  (let [sw (java.io.StringWriter.)
        bw (java.io.BufferedWriter. sw)]
    {:writer bw
     :string-writer sw}))

(defn get-writer-output [writer-map]
  (.flush (:writer writer-map))
  (str (.getBuffer (:string-writer writer-map))))
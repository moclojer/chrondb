(ns chrondb.api.sql.sql-test
  (:require [clojure.test :refer [deftest is testing]]
            [chrondb.api.sql.core :as sql]
            [chrondb.api.sql.test-helpers :refer [create-test-resources]])
  (:import [java.io BufferedReader BufferedWriter StringReader StringWriter]
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

;; Helper function to get a random available port
(defn get-available-port []
  (with-open [socket (ServerSocket. 0)]
    (.getLocalPort socket)))

;; Test Server Functions
(deftest test-sql-server
  (testing "Start and stop SQL server"
    (let [{storage :storage index :index} (create-test-resources)
          port (get-available-port)
          server (sql/start-sql-server storage index port)]
      (is (not (nil? server)))
      (is (instance? ServerSocket server))
      (is (not (.isClosed server)))
      (sql/stop-sql-server server)
      (is (.isClosed server)))))

;; Test multiple arities (overloads) of server initialization functions
(deftest test-sql-server-overloads
  (testing "Start SQL server with just storage"
    (let [{storage :storage} (create-test-resources)
          port (get-available-port)
          server (sql/start-sql-server storage nil port)]
      (is (not (nil? server)))
      (is (instance? ServerSocket server))
      (is (not (.isClosed server)))
      (sql/stop-sql-server server)
      (is (.isClosed server))))

  (testing "Start SQL server with storage and index"
    (let [{storage :storage index :index} (create-test-resources)
          port (get-available-port)
          server (sql/start-sql-server storage index port)]
      (is (not (nil? server)))
      (is (instance? ServerSocket server))
      (is (not (.isClosed server)))
      (sql/stop-sql-server server)
      (is (.isClosed server)))))

;; Test to ensure the server can be stopped safely
(deftest test-sql-server-safe-stop
  (testing "Stopping SQL server safely"
    (let [{storage :storage index :index} (create-test-resources)]
      ;; Normal case: server started and then stopped
      (let [port (get-available-port)
            server (sql/start-sql-server storage index port)]
        (sql/stop-sql-server server)
        (is (.isClosed server)))

      ;; Edge case: try to stop an already closed server
      (let [port (get-available-port)
            server (sql/start-sql-server storage index port)]
        (sql/stop-sql-server server)
        (sql/stop-sql-server server) ;; Should not throw an exception
        (is (.isClosed server)))

      ;; Edge case: try to stop with nil
      (sql/stop-sql-server nil)))) ;; Should not throw an exception
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

;; Test Server Functions
(deftest test-sql-server
  (testing "Start and stop SQL server"
    (let [{storage :storage index :index} (create-test-resources)
          server (sql/start-sql-server storage index 0)]
      (is (not (nil? server)))
      (is (instance? ServerSocket server))
      (is (not (.isClosed server)))
      (sql/stop-sql-server server)
      (is (.isClosed server)))))

;; Test múltiplas aridades (overloads) das funções de inicialização do servidor
(deftest test-sql-server-overloads
  (testing "Start SQL server with just storage"
    (let [{storage :storage} (create-test-resources)
          server (sql/start-sql-server storage)]
      (is (not (nil? server)))
      (is (instance? ServerSocket server))
      (is (not (.isClosed server)))
      (sql/stop-sql-server server)
      (is (.isClosed server))))

  (testing "Start SQL server with storage and index"
    (let [{storage :storage index :index} (create-test-resources)
          server (sql/start-sql-server storage index)]
      (is (not (nil? server)))
      (is (instance? ServerSocket server))
      (is (not (.isClosed server)))
      (sql/stop-sql-server server)
      (is (.isClosed server)))))

;; Teste para garantir que o servidor pode ser parado com segurança
(deftest test-sql-server-safe-stop
  (testing "Stopping SQL server safely"
    (let [{storage :storage index :index} (create-test-resources)]
      ;; Caso normal: servidor iniciado e então parado
      (let [server (sql/start-sql-server storage index 0)]
        (sql/stop-sql-server server)
        (is (.isClosed server)))

      ;; Caso de borda: tentar parar um servidor já fechado
      (let [server (sql/start-sql-server storage index 0)]
        (sql/stop-sql-server server)
        (sql/stop-sql-server server) ;; Não deve lançar exceção
        (is (.isClosed server)))

      ;; Caso de borda: tentar parar com nil
      (sql/stop-sql-server nil)))) ;; Não deve lançar exceção
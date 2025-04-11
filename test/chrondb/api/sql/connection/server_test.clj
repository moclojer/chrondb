(ns chrondb.api.sql.connection.server-test
  (:require [clojure.test :refer [deftest is testing]]
            [chrondb.api.sql.connection.server :as server]
            [chrondb.api.sql.test-helpers :refer [create-test-resources]])
  (:import [java.net ServerSocket Socket InetAddress]
           [java.util.concurrent Executors TimeUnit]))

;; Helper function to get a random available port
(defn get-available-port []
  (with-open [socket (ServerSocket. 0)]
    (.getLocalPort socket)))

(deftest test-create-server-socket
  (testing "Creating a server socket with a specific port"
    (let [port (get-available-port)
          server-socket (#'server/create-server-socket port)]
      (try
        (is (not (nil? server-socket)))
        (is (instance? ServerSocket server-socket))
        (is (= port (.getLocalPort server-socket)))
        (finally
          (.close server-socket))))))

(deftest test-start-sql-server
  (testing "Starting SQL server"
    (let [{storage :storage index :index} (create-test-resources)
          port (get-available-port)
          server-socket (server/start-sql-server storage index port)]
      (try
        (is (not (nil? server-socket)))
        (is (instance? ServerSocket server-socket))
        (is (not (.isClosed server-socket)))
        (is (= port (.getLocalPort server-socket)))
        (finally
          (server/stop-sql-server server-socket))))))

(deftest test-stop-sql-server
  (testing "Stopping SQL server with ServerSocket instance"
    (let [port (get-available-port)
          server-socket (doto (ServerSocket. port 50 (InetAddress/getByName "localhost")))]
      (is (not (.isClosed server-socket)))
      (server/stop-sql-server server-socket)
      (is (.isClosed server-socket))))

  (testing "Stopping SQL server with nil should not throw exception"
    (is (nil? (server/stop-sql-server nil))))

  (testing "Stopping SQL server with server map"
    (let [port (get-available-port)
          server-socket (ServerSocket. port 50 (InetAddress/getByName "localhost"))
          thread-pool (Executors/newCachedThreadPool)
          server-map {:server-socket server-socket
                      :thread-pool thread-pool
                      :port port}]
      (is (not (.isClosed server-socket)))
      (server/stop-sql-server server-map)
      (is (.isClosed server-socket))
      (is (.isShutdown thread-pool)))))

;; Teste para o tratamento de SocketException no accept-clients
(deftest test-accept-clients-exception-handling
  (testing "Accept clients should handle SocketException when socket is closed"
    ;; Mock para o handler
    (let [client-handler (fn [socket]
                           (is (instance? Socket socket)))
          port (get-available-port)
          server-socket (ServerSocket. port 50 (InetAddress/getByName "localhost"))
          thread-pool (Executors/newCachedThreadPool)
          accept-thread (Thread. #(#'server/accept-clients server-socket thread-pool client-handler))]

      ;; Iniciar a thread de aceitação
      (.start accept-thread)

      ;; Dar um tempo para a thread iniciar
      (Thread/sleep 100)

      ;; Fechar o socket para forçar uma SocketException
      (.close server-socket)

      ;; Dar um tempo para a thread processar a exceção
      (Thread/sleep 100)

      ;; Verificar se a thread terminou
      (.shutdown thread-pool)
      (is (.awaitTermination thread-pool 1 TimeUnit/SECONDS)))))
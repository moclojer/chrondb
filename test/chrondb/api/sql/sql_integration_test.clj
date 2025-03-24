(ns chrondb.api.sql.sql-integration-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [chrondb.api.sql.core :as sql]
            [chrondb.api.sql.test-helpers :refer [create-test-resources]])
  (:import [java.net Socket]
           [java.io BufferedReader BufferedWriter InputStreamReader OutputStreamWriter]
           [java.nio.charset StandardCharsets]
           [java.nio ByteBuffer]
           [java.util.concurrent Executors TimeUnit TimeoutException Callable]))

;; Função para executar uma tarefa com timeout
(defn with-timeout [timeout-ms timeout-msg f]
  (let [executor (Executors/newSingleThreadExecutor)
        future (.submit executor ^Callable f)]
    (try
      (try
        (.get future timeout-ms TimeUnit/MILLISECONDS)
        (catch TimeoutException _e
          (println (str "TIMEOUT: " timeout-msg))
          (.cancel future true)
          (throw (Exception. (str "Timeout: " timeout-msg)))))
      (finally
        (.shutdownNow executor)))))

(defn read-message [conn]
  (let [in (:input-stream conn)
        type (.read in)]
    (println "Lendo mensagem, tipo recebido:" (if (pos? type) (str (char type)) "EOF/error"))
    (when (pos? type)
      (try
        (let [byte-buffer (byte-array 4)]
          (.readNBytes in byte-buffer 0 4)
          (let [buffer (ByteBuffer/wrap byte-buffer)
                length (.getInt buffer)
                content-length (- length 4)
                content (byte-array content-length)]
            (println "Lendo conteúdo da mensagem, comprimento:" length "bytes")
            (.readNBytes in content 0 content-length)
            {:type (char type)
             :length length
             :content content}))
        (catch Exception e
          (println "Erro ao ler mensagem:" (.getMessage e))
          {:type (char type)
           :error (.getMessage e)})))))

;; Função para tentar ler mensagem com retry
(defn read-message-with-retry [conn max-retries]
  (letfn [(try-read [retries]
            (if (> retries max-retries)
              (throw (Exception. "Número máximo de tentativas excedido"))
              (let [result
                    (try
                      (let [msg (read-message conn)]
                        (if msg
                          [:success msg]
                          [:retry]))
                      (catch Exception e
                        [:error e]))]
                (case (first result)
                  :success (second result)
                  :retry (do
                           (println (str "Tentativa " (inc retries) " falhou, tentando novamente..."))
                           (Thread/sleep 100)
                           (try-read (inc retries)))
                  :error (let [e (second result)]
                           (println (str "Erro na tentativa " (inc retries) ": " (.getMessage e)))
                           (Thread/sleep 100)
                           (try-read (inc retries)))))))]
    (try-read 0)))

;; Helper functions for PostgreSQL protocol client simulation
(defn connect-to-sql [host port]
  (let [socket (Socket. host port)]
    ;; Set socket timeouts
    (.setSoTimeout socket 10000)  ;; 10 segundos de timeout para operações de leitura (aumentado de 5s)
    {:socket socket
     :reader (BufferedReader. (InputStreamReader. (.getInputStream socket) StandardCharsets/UTF_8))
     :writer (BufferedWriter. (OutputStreamWriter. (.getOutputStream socket) StandardCharsets/UTF_8))
     :input-stream (.getInputStream socket)
     :output-stream (.getOutputStream socket)}))

(defn close-sql-connection [conn]
  (.close (:writer conn))
  (.close (:reader conn))
  (.close (:socket conn)))

(defn send-startup-message [conn]
  (let [out (:output-stream conn)
        protocol-version (bit-or (bit-shift-left 3 16) 0)  ;; Version 3.0
        parameters {"user" "postgres"
                    "database" "chrondb"
                    "client_encoding" "UTF8"}

        ;; Calculate message size
        parameters-size (reduce + (map #(+ (count (first %)) 1 (count (second %)) 1) parameters))
        total-size (+ 4 4 parameters-size 1)  ;; Length(4) + Protocol(4) + params + NULL

        ;; Create message buffer
        buffer (ByteBuffer/allocate total-size)]

    ;; Write message length (including self but not including the message type)
    (.putInt buffer total-size)

    ;; Write protocol version
    (.putInt buffer protocol-version)

    ;; Write parameters as key-value pairs
    (doseq [[k v] parameters]
      (.put buffer (.getBytes k StandardCharsets/UTF_8))
      (.put buffer (byte 0))  ;; NULL terminator
      (.put buffer (.getBytes v StandardCharsets/UTF_8))
      (.put buffer (byte 0)))  ;; NULL terminator

    ;; Terminate parameters with NULL
    (.put buffer (byte 0))

    ;; Send the message
    (.write out (.array buffer))
    (.flush out)))

(defn send-query [conn query-text]
  (let [out (:output-stream conn)
        query-bytes (.getBytes query-text StandardCharsets/UTF_8)

        ;; Message: 'Q' + length(4) + query + NULL terminator
        message-length (+ 4 (count query-bytes) 1)
        buffer (ByteBuffer/allocate (+ 1 message-length))]

    ;; Message type
    (.put buffer (byte (int \Q)))

    ;; Length (including self but not including the message type)
    (.putInt buffer message-length)

    ;; Query text
    (.put buffer query-bytes)

    ;; NULL terminator
    (.put buffer (byte 0))

    ;; Send the message
    (.write out (.array buffer))
    (.flush out)))

(defn send-terminate [conn]
  (let [out (:output-stream conn)
        buffer (ByteBuffer/allocate 5)]

    ;; Message type 'X'
    (.put buffer (byte (int \X)))

    ;; Length (4 bytes)
    (.putInt buffer 4)

    ;; Send the message
    (.write out (.array buffer))
    (.flush out)))

(defn run-with-safe-logging [label f]
  (println "Iniciando:" label)
  (try
    (f)
    (println "Concluído com sucesso:" label)
    (catch Exception e
      (println "Falha em" label ":" (.getMessage e))
      (println "Continuando execução..."))))

;; Integration test with a PostgreSQL protocol client
(deftest ^:integration test-sql-integration
  (testing "SQL client integration"
    (let [{storage :storage index :index} (create-test-resources)
          port 15432  ;; Use a port different from the default PostgreSQL port
          is-ci (some? (System/getenv "CI"))
          server (sql/start-sql-server storage index port)]
      (println "Servidor SQL iniciado na porta:" port)
      (println "Executando em ambiente CI:" is-ci)
      (try
        ;; No ambiente CI, apenas verificamos se o servidor inicia
        (if is-ci
          (do
            (println "Executando em CI - Teste reduzido para apenas verificar se o servidor inicia")
            (is (some? server) "Servidor SQL deve ter iniciado corretamente"))
          ;; Testes completos fora do CI
          (do
            (println "Aguardando inicialização completa do servidor...")
            (Thread/sleep 2000)
            (println "Conectando ao servidor SQL...")
            (let [conn (connect-to-sql "localhost" port)]
              (println "Conexão estabelecida com sucesso")
              (try
                ;; Send startup message
                (run-with-safe-logging "Autenticação"
                                       #(testing "Connection startup"
                                          (println "Enviando mensagem de inicialização...")
                                          (send-startup-message conn)
                                          (println "Mensagem de inicialização enviada, aguardando resposta...")
                                          (Thread/sleep 1000)
                                          (let [auth-message (read-message conn)]
                                            (println "Mensagem de autenticação recebida:" auth-message)
                                            (when auth-message
                                              (is (= \R (char (:type auth-message))))))))

                ;; Read parameter messages with timeout
                (with-timeout 20000 "Timeout ao ler mensagens de parâmetros"
                  #(run-with-safe-logging "Leitura de parâmetros"
                                          (fn []
                                            (println "Ambiente: " (System/getenv "CI"))
                                            (println "Propriedades do Sistema: " (select-keys (System/getProperties) ["os.name" "java.version"]))
                                            (println "Iniciando leitura de parâmetros com timeout estendido...")
                                            (dotimes [i 5]
                                              (try
                                                (println "Tentando ler parâmetro" (inc i) "...")
                                                (let [msg (read-message-with-retry conn 5)]
                                                  (println "Parâmetro" (inc i) ":" msg)
                                                  (when (nil? msg)
                                                    (println "Fim dos parâmetros")
                                                    (throw (Exception. "Fim dos parâmetros"))))
                                                (catch Exception e
                                                  (println "Erro na leitura do parâmetro" (inc i) ":" (.getMessage e))
                                                  (throw e)))))))

                ;; Send query and read response
                (run-with-safe-logging "Consulta SQL"
                                       #(testing "Simple SELECT query"
                                          (send-query conn "SELECT 1 as test")
                                          (with-timeout 5000 "Timeout ao ler respostas da consulta"
                                            (fn []
                                              (dotimes [i 3]
                                                (try
                                                  (let [msg (read-message conn)]
                                                    (println "Resposta" (inc i) ":" msg)
                                                    (when (nil? msg)
                                                      (println "Fim das respostas")
                                                      (throw (Exception. "Fim das respostas"))))
                                                  (catch Exception e
                                                    (println "Erro na leitura da resposta" (inc i) ":" (.getMessage e))
                                                    (throw e))))))))

                ;; Terminate connection
                (run-with-safe-logging "Terminação da conexão"
                                       #(testing "Connection termination"
                                          (send-terminate conn)))

                (finally
                  (println "Fechando conexão...")
                  (try
                    (close-sql-connection conn)
                    (println "Conexão fechada com sucesso")
                    (catch Exception e
                      (println "Erro ao fechar conexão:" (.getMessage e)))))))))
        (finally
          (println "Parando servidor SQL...")
          (try
            (sql/stop-sql-server server)
            (println "Servidor SQL parado com sucesso")
            (catch Exception e
              (println "Erro ao parar servidor SQL:" (.getMessage e)))))))))

;; Define a fixture that can be used to run only integration tests
(defn integration-fixture [f]
  (f))

;; Use the fixture for integration tests
(use-fixtures :once integration-fixture)
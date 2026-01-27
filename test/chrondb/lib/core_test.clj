(ns chrondb.lib.core-test
  (:require [clojure.test :refer [deftest testing is use-fixtures]]
            [chrondb.lib.core :as lib]
            [clojure.java.io :as io])
  (:import [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]))

(def ^:dynamic *test-data-dir* nil)
(def ^:dynamic *test-index-dir* nil)

(defn- create-temp-dir
  "Creates a temporary directory for testing."
  []
  (str (Files/createTempDirectory "chrondb-lib-test" (make-array FileAttribute 0))))

(defn- delete-directory
  "Recursively deletes a directory."
  [path]
  (when (.exists (io/file path))
    (doseq [f (reverse (file-seq (io/file path)))]
      (try
        (io/delete-file f true)
        (catch Exception _)))))

(defn temp-dirs-fixture [f]
  (let [data-dir (create-temp-dir)
        index-dir (create-temp-dir)]
    (binding [*test-data-dir* data-dir
              *test-index-dir* index-dir]
      (try
        (f)
        (finally
          (delete-directory data-dir)
          (delete-directory index-dir))))))

(use-fixtures :each temp-dirs-fixture)

(deftest test-lib-open-close
  (testing "lib-open deve retornar handle valido e lib-close deve fechar"
    (let [handle (lib/lib-open *test-data-dir* *test-index-dir*)]
      (is (some? handle) "lib-open deve retornar handle")
      (is (>= handle 0) "handle deve ser >= 0")
      (let [close-result (lib/lib-close handle)]
        (is (= 0 close-result) "lib-close deve retornar 0 em sucesso")))))

(deftest test-lib-put-operation
  (testing "lib-put deve salvar documento sem StackOverflowError"
    (let [handle (lib/lib-open *test-data-dir* *test-index-dir*)]
      (is (some? handle) "lib-open deve retornar handle valido")
      (is (>= handle 0) "handle deve ser >= 0")
      (try
        (let [doc-id "user:1"
              doc-json "{\"name\": \"Test User\", \"email\": \"test@example.com\"}"
              result (lib/lib-put handle doc-id doc-json nil)]
          (is (some? result) "lib-put deve retornar documento salvo")
          (is (string? result) "resultado deve ser string JSON")
          (is (.contains result "Test User") "resultado deve conter dados salvos")
          (is (.contains result "user:1") "resultado deve conter o id")

          ;; Verificar que o documento pode ser recuperado
          (let [retrieved (lib/lib-get handle doc-id nil)]
            (is (some? retrieved) "lib-get deve retornar documento")
            (is (.contains retrieved "Test User") "documento deve conter dados salvos")))
        (finally
          (lib/lib-close handle))))))

(deftest test-lib-put-multiple-documents
  (testing "lib-put deve funcionar com multiplos documentos"
    (let [handle (lib/lib-open *test-data-dir* *test-index-dir*)]
      (is (>= handle 0) "handle deve ser valido")
      (try
        (doseq [i (range 10)]
          (let [doc-id (str "item:" i)
                doc-json (format "{\"index\": %d, \"value\": \"test-%d\"}" i i)
                result (lib/lib-put handle doc-id doc-json nil)]
            (is (some? result) (str "lib-put deve funcionar para doc " i))))

        ;; Verificar que todos foram salvos
        (doseq [i (range 10)]
          (let [doc-id (str "item:" i)
                retrieved (lib/lib-get handle doc-id nil)]
            (is (some? retrieved) (str "lib-get deve retornar doc " i))
            (is (.contains retrieved (str "\"index\":" i)) (str "doc " i " deve conter dados corretos"))))
        (finally
          (lib/lib-close handle))))))

(deftest test-lib-put-and-delete
  (testing "lib-put seguido de lib-delete deve funcionar"
    (let [handle (lib/lib-open *test-data-dir* *test-index-dir*)]
      (try
        (let [doc-id "temp:1"
              doc-json "{\"temp\": true}"
              _ (lib/lib-put handle doc-id doc-json nil)
              delete-result (lib/lib-delete handle doc-id nil)]
          (is (= 0 delete-result) "delete deve retornar 0")

          ;; Verificar que foi deletado
          (let [retrieved (lib/lib-get handle doc-id nil)]
            (is (nil? retrieved) "documento deletado nao deve ser encontrado")))
        (finally
          (lib/lib-close handle))))))

(deftest test-lib-list-by-prefix
  (testing "lib-list-by-prefix deve retornar documentos com prefixo"
    (let [handle (lib/lib-open *test-data-dir* *test-index-dir*)]
      (try
        ;; Criar alguns documentos
        (lib/lib-put handle "product:1" "{\"name\": \"Product 1\"}" nil)
        (lib/lib-put handle "product:2" "{\"name\": \"Product 2\"}" nil)
        (lib/lib-put handle "category:1" "{\"name\": \"Category 1\"}" nil)

        ;; Listar por prefixo
        (let [result (lib/lib-list-by-prefix handle "product:" nil)]
          (is (some? result) "resultado deve existir")
          (is (.contains result "Product 1") "deve conter Product 1")
          (is (.contains result "Product 2") "deve conter Product 2")
          (is (not (.contains result "Category 1")) "nao deve conter Category 1"))
        (finally
          (lib/lib-close handle))))))

(deftest test-lib-history
  (testing "lib-history deve retornar historico de modificacoes"
    (let [handle (lib/lib-open *test-data-dir* *test-index-dir*)]
      (try
        ;; Criar e atualizar documento
        (lib/lib-put handle "doc:1" "{\"version\": 1}" nil)
        (lib/lib-put handle "doc:1" "{\"version\": 2}" nil)

        ;; Verificar historico
        (let [history (lib/lib-history handle "doc:1" nil)]
          (is (some? history) "historico deve existir")
          (is (string? history) "historico deve ser string JSON"))
        (finally
          (lib/lib-close handle))))))

(deftest test-lib-open-returns-number-on-failure
  (testing "lib-open deve retornar -1 (nao nil) quando falha"
    ;; Usar path invalido que causa falha
    (let [result (lib/lib-open nil nil)]
      (is (number? result) "resultado deve ser numero, nao nil")
      (is (= -1 result) "resultado deve ser -1 em caso de erro"))))

(deftest test-lib-open-invalid-paths
  (testing "lib-open com paths vazios deve retornar -1"
    (let [result (lib/lib-open "" "")]
      (is (number? result) "resultado deve ser numero")
      (is (= -1 result) "paths vazios devem retornar -1"))))

(deftest test-lib-open-cleans-stale-locks
  (testing "lib-open deve limpar arquivos .lock orfaos e abrir normalmente"
    ;; Primeiro, criar um banco de dados valido
    (let [handle (lib/lib-open *test-data-dir* *test-index-dir*)]
      (is (>= handle 0) "primeiro open deve funcionar")
      ;; Salvar um documento para garantir que o repo esta funcional
      (lib/lib-put handle "test:1" "{\"test\": true}" nil)
      (lib/lib-close handle))

    ;; Simular crash criando lock file orfao
    (let [stale-lock (io/file *test-data-dir* "refs" "heads" "main.lock")
          lucene-lock (io/file *test-index-dir* "write.lock")]
      ;; Criar locks orfaos
      (.mkdirs (.getParentFile stale-lock))
      (spit stale-lock "stale lock content")
      (spit lucene-lock "stale lucene lock")
      ;; Definir tempo de modificacao para mais de 60 segundos atras
      (.setLastModified stale-lock (- (System/currentTimeMillis) 120000))
      (.setLastModified lucene-lock (- (System/currentTimeMillis) 120000))

      (is (.exists stale-lock) "git lock file deve existir antes do reopen")
      (is (.exists lucene-lock) "lucene lock file deve existir antes do reopen")

      ;; Reabrir deve limpar os locks orfaos e funcionar
      ;; Sem a limpeza, isso falharia com OpenFailed("")
      (let [handle (lib/lib-open *test-data-dir* *test-index-dir*)]
        (is (>= handle 0) "lib-open deve funcionar apos limpar locks orfaos")
        (when (>= handle 0)
          ;; Verificar que ainda consegue acessar dados
          (let [doc (lib/lib-get handle "test:1" nil)]
            (is (some? doc) "documento deve estar acessivel apos reopen"))
          ;; Git lock deve ter sido removido (nao e recriado ao abrir repo bare)
          (is (not (.exists stale-lock)) "git lock orfao deve ter sido removido")
          (lib/lib-close handle))))))

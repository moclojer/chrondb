(ns chrondb.api.sql.test-helpers
  (:require [chrondb.storage.memory :as memory]
            [chrondb.index.memory :as memory-index]))

;; Explicação: O problema dos símbolos não resolvidos está ocorrendo porque a macro está sendo verificada
;; pelo linter antes de ser expandida. Uma forma de resolver é fazer essas variáveis serem símbolos qualificados
;; com o namespace.

(defmacro with-test-data
  "Macro que cria um storage e um index para testes.
   Exemplo de uso: (with-test-data [storage index] (código de teste))"
  [[storage-sym index-sym] & body]
  `(let [~storage-sym (memory/create-memory-storage)
         ~index-sym (memory-index/create-memory-index)]
     ~@body))

;; Função direta para casos onde a macro causa problemas de linter
(defn create-test-resources []
  {:storage (memory/create-memory-storage)
   :index (memory-index/create-memory-index)})
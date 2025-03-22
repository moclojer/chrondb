(ns chrondb.api.sql.storage.bridge
  "Interface entre as consultas SQL e a camada de armazenamento ChronDB"
  (:require [chrondb.storage.protocol :as storage]
            [chrondb.index.protocol :as index]
            [chrondb.util.logging :as log]))

(defn execute-sql
  "Executa uma consulta SQL na camada de armazenamento ChronDB.
   Parâmetros:
   - storage: A implementação de armazenamento
   - index: A implementação de índice
   - query: A consulta SQL analisada
   Retorna: O resultado da execução da consulta"
  [storage index query]
  (log/log-debug (str "Executando consulta: " query))

  ;; A implementação real delegaria para as funções apropriadas
  ;; nos outros módulos (execute-select, execute-insert, etc.)
  ;; por ora, este é apenas um stub de interface

  {:success true
   :message "Consulta executada com sucesso"
   :query query})
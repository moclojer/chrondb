(ns chrondb.tools.migrator
  (:require [chrondb.storage.protocol :as storage]
            [chrondb.storage.git :as git]
            [chrondb.index.lucene :as lucene]
            [chrondb.index.protocol :as index]
            [chrondb.util.logging :as log]
            [clojure.string :as str]
            [clojure.java.io :as io]))

(defn- ensure-table-prefix
  "Ensures that the given ID has the table prefix"
  [id table]
  (if (str/includes? id (str table ":"))
    id
    (str table ":" id)))

(defn migrate-ids
  "Migra IDs existentes no banco para incluir o prefixo da tabela.
   Params:
   - storage: Implementação do storage
   - index: Implementação do index (opcional)
   - table-name: Nome da tabela para migrar
   - table-fields: Campos que identificam esta tabela (ex: [:name :email] para usuários)
   Returns:
   - Quantidade de documentos migrados"
  [storage index table-name table-fields]
  (log/log-info (str "Iniciando migração de IDs para tabela: " table-name))

  (let [all-docs (storage/get-documents-by-prefix storage "")
        ; Filtra documentos que parecem pertencer a esta tabela baseado nos campos
        table-docs (filter (fn [doc]
                             (and (not (str/includes? (:id doc) (str table-name ":")))
                                  (every? #(contains? doc %) table-fields)))
                           all-docs)
        migrated-count (atom 0)]

    (log/log-info (str "Encontrados " (count table-docs) " documentos para migrar"))

    (doseq [doc table-docs]
      (let [old-id (:id doc)
            new-id (ensure-table-prefix old-id table-name)
            new-doc (assoc doc :id new-id)]

        (log/log-info (str "Migrando documento: " old-id " -> " new-id))

        ; Salva com novo ID
        (let [saved (storage/save-document storage new-doc)]
          (when index
            (log/log-debug "Re-indexando documento")
            (index/index-document index saved))

          ; Remove documento antigo
          (storage/delete-document storage old-id)
          (when index
            (index/delete-document index old-id))

          (swap! migrated-count inc))))

    (log/log-info (str "Migração completa. " @migrated-count " documentos migrados"))
    @migrated-count))

(defn -main
  "Utilitário de migração de IDs.
   Args:
   - Uma tabela e campos (opcional) para migrar (ex: 'user' ou 'user:name,email')"
  [& args]
  (log/init! {:min-level :info})
  (let [data-dir "data"
        ; Cria ou usa storage
        _ (log/log-info "Inicializando storage e index")
        storage (git/create-git-storage data-dir)
        index (lucene/create-lucene-index (str data-dir "/index"))

        ; Processa argumentos
        table-arg (first args)

        [table-name fields-str] (if (str/includes? table-arg ":")
                                  (str/split table-arg #":")
                                  [table-arg nil])

        table-fields (if fields-str
                       (mapv keyword (str/split fields-str #","))
                       ; Valores padrão por tabela
                       (case table-name
                         "user" [:name :email]
                         []))]

    (try
      (log/log-info (str "Migrando tabela: " table-name " (campos: " table-fields ")"))
      (let [count (migrate-ids storage index table-name table-fields)]
        (log/log-info (str "Migração concluída com sucesso: " count " documentos migrados")))

      (catch Exception e
        (log/log-error (str "Erro durante migração: " (.getMessage e))))

      (finally
        (.close storage)
        (.close index)
        (log/log-info "Recursos liberados")))))
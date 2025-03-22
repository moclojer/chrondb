(ns chrondb.api.sql.execution.query
  "Execução de consultas SQL"
  (:require [clojure.string :as str]
            [chrondb.util.logging :as log]
            [chrondb.api.sql.parser.statements :as statements]
            [chrondb.api.sql.protocol.messages :as messages]
            [chrondb.api.sql.execution.operators :as operators]
            [chrondb.api.sql.execution.functions :as functions]
            [chrondb.storage.protocol :as storage]
            [chrondb.index.protocol :as index]))

(defn handle-select
  "Manipula uma consulta SELECT.
   Parâmetros:
   - storage: A implementação de armazenamento
   - query: A consulta analisada
   Retorna: Uma sequência de documentos de resultado"
  [storage query]
  (try
    (let [where-condition (:where query)
          group-by (:group-by query)
          order-by (:order-by query)
          limit (:limit query)
          table-name (:table query)

         ;; Logs detalhados para depuração
          _ (log/log-info (str "Executando SELECT na tabela: " table-name))
          _ (log/log-debug (str "Condição WHERE: " where-condition))
          _ (log/log-debug (str "Colunas: " (:columns query)))

         ;; Sempre recupera todos os documentos (varredura completa) a menos que tenhamos uma consulta de ID
          all-docs (if (and where-condition
                            (seq where-condition)
                            (= (count where-condition) 1)
                            (get-in where-condition [0 :field])
                            (= (get-in where-condition [0 :field]) "id")
                            (= (get-in where-condition [0 :op]) "="))
                    ;; Otimização para pesquisa por ID
                     (let [id (str/replace (get-in where-condition [0 :value]) #"['\"]" "")]
                       (log/log-debug (str "Pesquisando documento por ID: " id))
                       (if-let [doc (storage/get-document storage id)]
                         [doc]
                         []))
                    ;; Varredura completa - recupera todos os documentos
                     (do
                       (log/log-debug "Realizando varredura completa de documentos")
                       (let [docs (storage/get-documents-by-prefix storage "")]
                         (log/log-debug (str "Recuperados " (count docs) " documentos"))
                         (or (seq docs) []))))

          _ (log/log-debug (str "Total de documentos recuperados: " (count all-docs)))

         ;; Aplica filtros WHERE
          filtered-docs (if where-condition
                          (do
                            (log/log-debug "Aplicando filtros WHERE")
                            (operators/apply-where-conditions all-docs where-condition))
                          all-docs)

          _ (log/log-debug (str "Após filtragem: " (count filtered-docs) " documentos"))

         ;; Agrupa documentos se necessário
          grouped-docs (operators/group-docs-by filtered-docs group-by)

         ;; Processa cada grupo
          processed-groups
          (mapv
           (fn [group]
             (let [result-cols (reduce
                                (fn [acc col-def]
                                  (case (:type col-def)
                                    :all
                                    (if (empty? group)
                                      acc
                                      (merge acc (first group)))

                                    :column
                                    (if-let [alias (:alias col-def)]
                                      (assoc acc (keyword alias) (get (first group) (keyword (:column col-def))))
                                      (assoc acc (keyword (:column col-def)) (get (first group) (keyword (:column col-def)))))

                                    :aggregate-function
                                    (let [fn-result (functions/execute-aggregate-function
                                                     (:function col-def)
                                                     group
                                                     (first (:args col-def)))]
                                      (assoc acc (keyword (str (name (:function col-def)) "_" (first (:args col-def)))) fn-result))

                                   ;; Padrão
                                    acc))
                                {}
                                (:columns query))]
               result-cols))
           grouped-docs)

         ;; Ordena resultados
          sorted-results (operators/sort-docs-by processed-groups order-by)

         ;; Aplica limite
          limited-results (operators/apply-limit sorted-results limit)

          _ (log/log-info (str "Retornando " (count limited-results) " resultados"))]

     ;; Retorna pelo menos uma lista vazia se não houver resultados
      (or limited-results []))
    (catch Exception e
      (let [sw (java.io.StringWriter.)
            pw (java.io.PrintWriter. sw)]
        (.printStackTrace e pw)
        (log/log-error (str "Erro em handle-select: " (.getMessage e) "\n" (.toString sw))))
      [])))

(defn handle-insert
  "Manipula uma consulta INSERT.
   Parâmetros:
   - storage: A implementação de armazenamento
   - doc: O documento a inserir
   Retorna: O documento salvo"
  [storage doc]
  (log/log-info (str "Inserindo documento: " doc))
  (let [result (storage/save-document storage doc)]
    (log/log-info (str "Documento inserido com sucesso: " result))
    result))

(defn handle-update
  "Manipula uma consulta UPDATE.
   Parâmetros:
   - storage: A implementação de armazenamento
   - id: O ID do documento a atualizar
   - updates: As atualizações a aplicar
   Retorna: O documento atualizado ou nil se não for encontrado"
  [storage id updates]
  (try
    (log/log-debug (str "Tentando atualizar documento com ID: " id))
    (when-let [doc (storage/get-document storage id)]
      (log/log-debug (str "Documento encontrado para atualização: " doc))
      (let [updated-doc (merge doc updates)]
        (log/log-debug (str "Documento mesclado: " updated-doc))
        (storage/save-document storage updated-doc)))
    (catch Exception e
      (let [sw (java.io.StringWriter.)
            pw (java.io.PrintWriter. sw)]
        (.printStackTrace e pw)
        (log/log-error (str "Erro em handle-update: " (.getMessage e) "\n" (.toString sw))))
      nil)))

(defn handle-delete
  "Manipula uma consulta DELETE.
   Parâmetros:
   - storage: A implementação de armazenamento
   - id: O ID do documento a excluir
   Retorna: O documento excluído ou nil se não for encontrado"
  [storage id]
  (storage/delete-document storage id))

(defn handle-query
  "Manipula uma consulta SQL.
   Parâmetros:
   - storage: A implementação de armazenamento
   - index: A implementação de índice
   - out: O stream de saída para escrever os resultados
   - sql: A string de consulta SQL
   Retorna: nil"
  [storage index ^java.io.OutputStream out sql]
  (log/log-info (str "Executando consulta: " sql))
  (let [parsed (statements/parse-sql-query sql)]
    (log/log-debug (str "Consulta analisada: " parsed))
    (case (:type parsed)
      :select
      (let [results (handle-select storage parsed)
            columns (if (empty? results)
                      ["id" "value"]
                      (mapv name (keys (first results))))]
        (messages/send-row-description out columns)
        (doseq [row results]
          (messages/send-data-row out (map #(get row (keyword %)) columns)))
        (messages/send-command-complete out "SELECT" (count results)))

      :insert
      (try
        (log/log-info (str "Processando INSERT na tabela: " (:table parsed)))
        (log/log-debug (str "Valores: " (:values parsed) ", colunas: " (:columns parsed)))

        (let [values (:values parsed)
              columns (:columns parsed)

             ;; Remove aspas e outras formatações dos valores
              clean-values (if (seq values)
                             (mapv #(str/replace % #"['\"]" "") values)
                             [])

              _ (log/log-debug (str "Valores limpos para INSERT: " clean-values))

             ;; Cria um documento com os valores fornecidos
              doc (if (and (seq clean-values) (seq columns))
                   ;; Se colunas e valores estiverem definidos, mapeie-os
                    (let [doc-map (reduce (fn [acc [col val]]
                                            (assoc acc (keyword col) val))
                                          {:id (str (java.util.UUID/randomUUID))}  ;; Sempre fornece um ID
                                          (map vector columns clean-values))]
                      (log/log-debug (str "Documento criado com mapeamento de colunas: " doc-map))
                      doc-map)
                   ;; Caso contrário, use o formato padrão id/value
                    (let [doc-map (cond
                                    (>= (count clean-values) 2) {:id (first clean-values)
                                                                 :value (second clean-values)}
                                    (= (count clean-values) 1) {:id (str (java.util.UUID/randomUUID))
                                                                :value (first clean-values)}
                                    :else {:id (str (java.util.UUID/randomUUID)) :value ""})]
                      (log/log-debug (str "Documento criado com formato padrão: " doc-map))
                      doc-map))

              _ (log/log-info (str "Documento a inserir: " doc))

             ;; Salva o documento no armazenamento
              saved (handle-insert storage doc)]

         ;; Indexa o documento (se tivermos um índice)
          (when index
            (log/log-debug "Indexando documento")
            (index/index-document index saved))

          (log/log-info (str "INSERT concluído com sucesso, ID: " (:id saved)))
          (messages/send-command-complete out "INSERT" 1)
          (messages/send-ready-for-query out))
        (catch Exception e
          (let [sw (java.io.StringWriter.)
                pw (java.io.PrintWriter. sw)]
            (.printStackTrace e pw)
            (log/log-error (str "Erro ao processar INSERT: " (.getMessage e) "\n" (.toString sw))))
          (messages/send-error-response out (str "Erro ao processar INSERT: " (.getMessage e)))
          (messages/send-command-complete out "INSERT" 0)
          (messages/send-ready-for-query out)))

      :update
      (try
        (log/log-info (str "Processando UPDATE na tabela: " (:table parsed)))
        (let [where-conditions (:where parsed)
              updates (try
                        (reduce-kv
                         (fn [m k v]
                           (assoc m (keyword k) (str/replace v #"['\"]" "")))
                         {}
                         (:updates parsed))
                        (catch Exception e
                          (log/log-error (str "Erro ao processar valores de atualização: " (.getMessage e)))
                          {}))]

          (log/log-debug (str "Condições de atualização: " where-conditions ", valores: " updates))

          (if (and (seq where-conditions) (seq updates))
           ;; Atualização com condições válidas
            (let [;; Encontra documentos que correspondem às condições
                  _ (log/log-info (str "Pesquisando documentos correspondentes a: " where-conditions))
                  all-docs (storage/get-documents-by-prefix storage "")
                  matching-docs (operators/apply-where-conditions all-docs where-conditions)
                  update-count (atom 0)]

              (if (seq matching-docs)
                (do
                 ;; Atualiza cada documento encontrado
                  (log/log-info (str "Encontrados " (count matching-docs) " documentos para atualizar"))
                  (doseq [doc matching-docs]
                    (let [updated-doc (merge doc updates)
                          _ (log/log-debug (str "Atualizando documento: " (:id doc) " com valores: " updates))
                          _ (log/log-debug (str "Documento mesclado: " updated-doc))
                          saved (storage/save-document storage updated-doc)]

                     ;; Re-indexa o documento se necessário
                      (when (and saved index)
                        (log/log-debug (str "Re-indexando documento atualizado: " (:id saved)))
                        (index/index-document index saved))

                      (swap! update-count inc)))

                  (log/log-info (str "Atualizados " @update-count " documentos com sucesso"))
                  (messages/send-command-complete out "UPDATE" @update-count))

               ;; Nenhum documento encontrado
                (do
                  (log/log-warn "Nenhum documento encontrado correspondente às condições WHERE")
                  (messages/send-error-response out "Nenhum documento encontrado correspondente às condições WHERE")
                  (messages/send-command-complete out "UPDATE" 0))))

           ;; Atualização sem WHERE ou com dados inválidos
            (do
              (log/log-warn "Consulta UPDATE inválida - cláusula WHERE ausente ou valores de atualização inválidos")
              (let [error-msg (if (seq where-conditions)
                                "UPDATE falhou: Nenhum valor válido para atualizar"
                                "UPDATE sem cláusula WHERE não é suportado. Por favor, especifique condições.")]
                (log/log-error error-msg)
                (messages/send-error-response out error-msg)
                (messages/send-command-complete out "UPDATE" 0)))))
        (catch Exception e
          (let [sw (java.io.StringWriter.)
                pw (java.io.PrintWriter. sw)]
            (.printStackTrace e pw)
            (log/log-error (str "Erro ao processar UPDATE: " (.getMessage e) "\n" (.toString sw))))
          (messages/send-error-response out (str "Erro ao processar UPDATE: " (.getMessage e)))
          (messages/send-command-complete out "UPDATE" 0)))

      :delete
      (let [id (when (seq (:where parsed))
                 (str/replace (get-in parsed [:where 0 :value]) #"['\"]" ""))]
        (if id
       ;; DELETE específico por ID
          (let [_ (log/log-info (str "Excluindo documento com ID: " id))
                deleted (handle-delete storage id)
                _ (when (and deleted index)
                    (index/delete-document index id))]
            (log/log-info (str "Documento " (if deleted "excluído com sucesso" "não encontrado")))
            (messages/send-command-complete out "DELETE" (if deleted 1 0)))
       ;; DELETE sem WHERE - responde com erro
          (do
            (log/log-warn "Tentativa de DELETE sem cláusula WHERE foi rejeitada")
            (let [error-msg "DELETE sem cláusula WHERE não é suportado. Use WHERE id='valor' para especificar qual documento excluir."]
              (log/log-error error-msg)
              (messages/send-error-response out error-msg)
              (messages/send-ready-for-query out)))))

     ;; Comando desconhecido
      (do
        (messages/send-error-response out (str "Comando desconhecido: " sql))
        (messages/send-command-complete out "UNKNOWN" 0)))))
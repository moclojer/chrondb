(ns chrondb.api.sql.execution.functions
  "Implementação de funções SQL como agregações"
  (:require [chrondb.util.logging :as log]))

(defn execute-aggregate-function
  "Executa uma função de agregação em uma coleção de documentos.
   Parâmetros:
   - function: A função de agregação a executar (:count, :sum, :avg, :min, :max)
   - docs: Os documentos para operar
   - field: O campo para agregar
   Retorna: O resultado da função de agregação"
  [function docs field]
  (case function
    :count (count docs)
    :sum (reduce + (keep #(get % (keyword field)) docs))
    :avg (let [values (keep #(get % (keyword field)) docs)]
           (if (empty? values)
             0
             (/ (reduce + values) (count values))))
    :min (apply min (keep #(get % (keyword field)) docs))
    :max (apply max (keep #(get % (keyword field)) docs))
    ;; Caso padrão
    (do
      (log/log-warn (str "Função de agregação não suportada: " function))
      nil)))
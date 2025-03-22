(ns chrondb.api.sql.execution.operators
  "Implementação dos operadores SQL"
  (:require [clojure.string :as str]
            [chrondb.util.logging :as log]))

(defn evaluate-condition
  "Avalia uma condição em relação a um documento.
   Parâmetros:
   - doc: O documento a ser avaliado
   - condition: A condição a ser avaliada
   Retorna: true se a condição for atendida, false caso contrário"
  [doc condition]
  (let [field-val (get doc (keyword (:field condition)))
        cond-val (str/replace (:value condition) #"['\"]" "")
        operator (:op condition)]
    (case operator
      "=" (= (str field-val) cond-val)
      "!=" (not= (str field-val) cond-val)
      "<>" (not= (str field-val) cond-val)
      ">" (> (compare (str field-val) cond-val) 0)
      "<" (< (compare (str field-val) cond-val) 0)
      ">=" (>= (compare (str field-val) cond-val) 0)
      "<=" (<= (compare (str field-val) cond-val) 0)
      "like" (re-find (re-pattern (str/replace cond-val #"%" ".*")) (str field-val))
      ;; Caso padrão
      (do
        (log/log-warn (str "Operador não suportado: " operator))
        false))))

(defn apply-where-conditions
  "Aplica condições WHERE para filtrar uma coleção de documentos.
   Parâmetros:
   - docs: Os documentos a serem filtrados
   - conditions: As condições a serem aplicadas
   Retorna: Documentos filtrados"
  [docs conditions]
  (log/log-debug (str "Aplicando condições WHERE: " conditions " a " (count docs) " documentos"))
  (if (empty? conditions)
    docs
    (filter
     (fn [document]
       (log/log-debug (str "Verificando documento: " document))
       (every?
        (fn [condition]
          (let [field (keyword (:field condition))
                operator (:op condition)
                value (str/replace (:value condition) #"['\"]" "")
                doc-value (str (get document field ""))]

            (log/log-debug (str "Verificando condição: " field " " operator " " value " contra " doc-value))

            (case operator
              "=" (= doc-value value)
              "!=" (not= doc-value value)
              ">" (> (compare doc-value value) 0)
              ">=" (>= (compare doc-value value) 0)
              "<" (< (compare doc-value value) 0)
              "<=" (<= (compare doc-value value) 0)
              "LIKE" (let [pattern (str/replace value "%" ".*")]
                       (boolean (re-find (re-pattern pattern) doc-value)))
              ;; Padrão: não correspondente
              false)))
        conditions))
     docs)))

(defn group-docs-by
  "Agrupa documentos pelos campos especificados.
   Parâmetros:
   - docs: Os documentos a serem agrupados
   - group-fields: Os campos para agrupar
   Retorna: Documentos agrupados"
  [docs group-fields]
  (if (empty? group-fields)
    [docs]
    (let [group-fn (fn [doc]
                     (mapv #(get doc (keyword (:column %))) group-fields))]
      (vals (group-by group-fn docs)))))

(defn sort-docs-by
  "Ordena documentos pelas cláusulas de ordenação especificadas.
   Parâmetros:
   - docs: Os documentos a serem ordenados
   - order-clauses: As cláusulas de ordenação a serem aplicadas
   Retorna: Documentos ordenados"
  [docs order-clauses]
  (if (empty? order-clauses)
    docs
    (let [comparators (for [{:keys [column direction]} order-clauses]
                        (fn [a b]
                          (let [a-val (get a (keyword column))
                                b-val (get b (keyword column))
                                result (compare a-val b-val)]
                            (if (= direction :desc)
                              (- result)
                              result))))]
      (sort (fn [a b]
              (loop [comps comparators]
                (if (empty? comps)
                  0
                  (let [result ((first comps) a b)]
                    (if (zero? result)
                      (recur (rest comps))
                      result)))))
            docs))))

(defn apply-limit
  "Aplica uma cláusula LIMIT a uma coleção de documentos.
   Parâmetros:
   - docs: Os documentos a serem limitados
   - limit: O número máximo de documentos a retornar
   Retorna: Documentos limitados"
  [docs limit]
  (if limit
    (take limit docs)
    docs))
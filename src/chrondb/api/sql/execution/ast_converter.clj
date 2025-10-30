(ns chrondb.api.sql.execution.ast-converter
  "Converts SQL WHERE conditions to AST clauses for Lucene queries."
  (:require [chrondb.query.ast :as ast]
            [clojure.string :as str]))

(defn- clean-value
  "Removes quotes from a SQL value."
  [value]
  (if (string? value)
    (str/replace value #"^['\"]|['\"]$" "")
    (str value)))

(defn- parse-number
  "Attempts to parse a string as a number.
   Returns {:type :long :value n} or {:type :double :value n} or nil."
  [s]
  (try
    (let [clean (clean-value s)]
      (if (str/includes? clean ".")
        {:type :double :value (Double/parseDouble clean)}
        {:type :long :value (Long/parseLong clean)}))
    (catch Exception _
      nil)))

(defn condition->ast-clause
  "Converts a SQL WHERE condition to an AST clause.
   Parameters:
   - condition: A condition map with :type, :field, :op, :value
   Returns: An AST clause or nil"
  [condition]
  (let [field (keyword (:field condition))
        op (str/lower-case (:op condition))
        value-str (:value condition)
        clean-val (clean-value value-str)]
    (case (:type condition)
      ;; FTS conditions
      :fts-match
      (ast/fts field (:query condition))

      ;; Standard conditions
      :standard
      (case op
        "=" (ast/term field clean-val)
        "!=" (ast/not (ast/term field clean-val))
        "<>" (ast/not (ast/term field clean-val))
        ">" (let [num-info (parse-number clean-val)]
              (if num-info
                (if (= (:type num-info) :long)
                  (ast/range-long field (:value num-info) nil {:include-lower? false})
                  (ast/range-double field (:value num-info) nil {:include-lower? false}))
                (ast/range field clean-val nil {:include-lower? false})))
        ">=" (let [num-info (parse-number clean-val)]
               (if num-info
                 (if (= (:type num-info) :long)
                   (ast/range-long field (:value num-info) nil {:include-lower? true})
                   (ast/range-double field (:value num-info) nil {:include-lower? true}))
                 (ast/range field clean-val nil {:include-lower? true})))
        "<" (let [num-info (parse-number clean-val)]
              (if num-info
                (if (= (:type num-info) :long)
                  (ast/range-long field nil (:value num-info) {:include-upper? false})
                  (ast/range-double field nil (:value num-info) {:include-upper? false}))
                (ast/range field nil clean-val {:include-upper? false})))
        "<=" (let [num-info (parse-number clean-val)]
               (if num-info
                 (if (= (:type num-info) :long)
                   (ast/range-long field nil (:value num-info) {:include-upper? true})
                   (ast/range-double field nil (:value num-info) {:include-upper? true}))
                 (ast/range field nil clean-val {:include-upper? true})))
        "like" (ast/wildcard field (str/replace clean-val #"%" "*"))
        "is null" (ast/missing field)
        "is not null" (ast/exists field)
        ;; Default: treat as term match
        (ast/term field clean-val))

      ;; Unknown type
      nil)))

(defn conditions->ast-clauses
  "Converts a sequence of SQL WHERE conditions to AST clauses.
   Combines them with AND logic.
   Parameters:
   - conditions: A sequence of condition maps
   Returns: A single AST clause (may be a boolean AND)"
  [conditions]
  (when (seq conditions)
    (let [clauses (keep condition->ast-clause conditions)]
      (when (seq clauses)
        (if (= (count clauses) 1)
          (first clauses)
          (apply ast/and clauses))))))

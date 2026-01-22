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
        op (when (:op condition) (str/lower-case (:op condition)))
        value-str (:value condition)
        clean-val (when value-str (clean-value value-str))]
    (case (:type condition)
      ;; FTS conditions
      :fts-match
      (ast/fts field (:query condition))

      ;; IS NULL - field does not exist
      :is-null
      (ast/missing field)

      ;; IS NOT NULL - field exists
      :is-not-null
      (ast/exists field)

      ;; IN (val1, val2, ...) - OR of multiple terms
      :in
      (let [values (:values condition)
            clean-values (map clean-value values)
            term-clauses (map #(ast/term field %) clean-values)]
        (apply ast/or term-clauses))

      ;; NOT IN (val1, val2, ...) - must not match any value
      :not-in
      (let [values (:values condition)
            clean-values (map clean-value values)
            term-clauses (map #(ast/term field %) clean-values)]
        (ast/boolean {:must [(ast/match-all)]
                      :must-not term-clauses}))

      ;; BETWEEN lower AND upper - range query inclusive
      :between
      (let [lower-str (:lower condition)
            upper-str (:upper condition)
            clean-lower (when lower-str (clean-value lower-str))
            clean-upper (when upper-str (clean-value upper-str))
            lower-num (when clean-lower (parse-number clean-lower))
            upper-num (when clean-upper (parse-number clean-upper))]
        (cond
          ;; Both are numbers of same type
          (and lower-num upper-num (= (:type lower-num) (:type upper-num)))
          (if (= (:type lower-num) :long)
            (ast/range-long field (:value lower-num) (:value upper-num)
                            {:include-lower? true :include-upper? true})
            (ast/range-double field (:value lower-num) (:value upper-num)
                              {:include-lower? true :include-upper? true}))
          ;; At least one is a number
          (or lower-num upper-num)
          (let [use-double? (or (= (:type lower-num) :double)
                                (= (:type upper-num) :double))]
            (if use-double?
              (ast/range-double field
                                (if lower-num (:value lower-num) (some-> clean-lower Double/parseDouble))
                                (if upper-num (:value upper-num) (some-> clean-upper Double/parseDouble))
                                {:include-lower? true :include-upper? true})
              (ast/range-long field
                              (if lower-num (:value lower-num) (some-> clean-lower Long/parseLong))
                              (if upper-num (:value upper-num) (some-> clean-upper Long/parseLong))
                              {:include-lower? true :include-upper? true})))
          ;; String range
          :else
          (ast/range field clean-lower clean-upper
                     {:include-lower? true :include-upper? true})))

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

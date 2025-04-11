(ns chrondb.api.sql.protocol.messages
  "Adapter functions to forward calls to the new namespaces.
   This is to maintain backward compatibility while refactoring."
  (:require [chrondb.api.sql.protocol.messages.writer :as writer]
            [chrondb.api.sql.protocol.messages.reader :as reader]
            [chrondb.util.logging :as log]))

;; Forward all the writing functions to the new writer namespace
(defn write-message [out type body]
  (writer/write-message out type body))

(defn send-authentication-ok [out]
  (writer/send-authentication-ok out))

(defn send-error-response [out message]
  (writer/send-error-response out message))

(defn send-notice-response [out message]
  (writer/send-notice-response out message))

(defn send-parameter-status [out name value]
  (writer/send-parameter-status out name value))

(defn send-command-complete [out command rows]
  (writer/send-command-complete out command rows))

(defn send-ready-for-query [out state]
  (writer/send-ready-for-query out state))

(defn send-row-description [out columns]
  (log/log-info (str "Row description with columns: " (pr-str columns)))
  (writer/send-row-description out columns))

;; Adapted function to provide compatibility with existing code
;; Improves data handling to avoid sending nil values
(defn send-data-row
  ([out row]
   ;; Ensure 'row' is a valid sequence of values
   (let [row-data (if (sequential? row)
                    ;; Convert nil values to empty strings
                    (mapv (fn [val]
                            (if (nil? val)
                              ""
                              (str val)))
                          row)
                    ;; If not a sequence, convert the entire object to string
                    [(str row)])]
     (log/log-info (str "Sending data row with values: " (pr-str row-data)))
     ;; Send directly to the simplified version
     (writer/send-data-row out row-data)))

  ([out row columns]
   ;; Ensure values are strings and not nil
   (let [col-count (count columns)
         ;; Map values using column names
         values (if (map? row)
                  ;; Case 1: row is a map, extract values by column names
                  (mapv (fn [col]
                          (let [key-name (if (string? col)
                                           (keyword col)
                                           col)
                                val (get row key-name)]
                            (if (nil? val)
                              ""
                              (str val))))
                        columns)

                  ;; Case 2: row is already a sequence, ensure it has values for all columns
                  (if (sequential? row)
                    (let [row-size (count row)]
                      (mapv (fn [i]
                              (if (< i row-size)
                                (let [val (nth row i)]
                                  (if (nil? val) "" (str val)))
                                ""))
                            (range col-count)))
                    ;; Case 3: single non-sequence value - use for all columns
                    (vec (repeat col-count (str row)))))]
     (log/log-info (str "Sending data row with values: " (pr-str values)))
     (writer/send-data-row out values))))

;; Forward all the reading functions to the new reader namespace
(defn read-byte [in]
  (reader/read-byte in))

(defn read-int [in]
  (reader/read-int in))

(defn read-short [in]
  (reader/read-short in))

(defn read-null-terminated-string [in]
  (reader/read-null-terminated-string in))

(defn read-startup-message [in]
  (reader/read-startup-message in))

(defn read-query-message [in]
  (reader/read-query-message in))
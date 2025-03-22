(ns chrondb.api.sql.storage.bridge
  "Interface between SQL queries and the ChronDB storage layer"
  (:require [chrondb.storage.protocol :as storage]
            [chrondb.index.protocol :as index]
            [chrondb.util.logging :as log]))

(defn execute-sql
  "Executes a SQL query on the ChronDB storage layer.
   Parameters:
   - storage: The storage implementation
   - index: The index implementation
   - query: The parsed SQL query
   Returns: The result of the query execution"
  [storage index query]
  (log/log-debug (str "Executing query: " query))

  ;; The real implementation would delegate to appropriate functions
  ;; in other modules (execute-select, execute-insert, etc.)
  ;; for now, this is just an interface stub

  {:success true
   :message "Query executed successfully"
   :query query})
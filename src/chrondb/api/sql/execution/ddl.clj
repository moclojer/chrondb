(ns chrondb.api.sql.execution.ddl
  "DDL (Data Definition Language) execution handlers"
  (:require [chrondb.util.logging :as log]
            [chrondb.api.sql.protocol.messages :as messages]
            [chrondb.api.sql.schema.core :as schema-core]
            [chrondb.transaction.core :as tx]))

(defn- normalize-schema-to-branch
  "Converts a SQL schema name to a Git branch name.
   Parameters:
   - schema: The schema name (or nil)
   Returns: The corresponding Git branch name"
  [schema]
  (cond
    (nil? schema) "main"
    (= schema "public") "main"
    :else schema))

(defn- ddl-tx-options
  "Creates transaction options for DDL operations.
   Parameters:
   - operation: The DDL operation (create-table, drop-table, etc.)
   - table: The table name
   - branch: The branch name
   Returns: Map with transaction options"
  [{:keys [operation table branch]}]
  {:origin "sql"
   :metadata {:operation operation
              :table table
              :branch branch}
   :flags ["ddl"]})

;; CREATE TABLE handler

(defn handle-create-table
  "Handles a CREATE TABLE statement.
   Parameters:
   - storage: The storage implementation
   - out: Output stream to write results to
   - parsed: The parsed query details
   Returns: nil"
  [storage out parsed]
  (log/log-info (str "Handling CREATE TABLE: " (:table parsed)))

  (try
    (let [table-name (:table parsed)
          columns (:columns parsed)
          schema (:schema parsed)
          branch-name (normalize-schema-to-branch schema)
          if-not-exists (:if-not-exists parsed)
          tx-opts (ddl-tx-options {:operation "create-table"
                                   :table table-name
                                   :branch branch-name})
          result (tx/with-transaction [storage tx-opts]
                   (schema-core/create-table storage table-name columns branch-name if-not-exists))]

      (if (:success result)
        (do
          (log/log-info (:message result))
          (messages/send-command-complete out "CREATE TABLE" 0))
        (do
          (log/log-error (str "CREATE TABLE failed: " (:message result)))
          (messages/send-error-response out (:message result)))))

    (catch Exception e
      (let [sw (java.io.StringWriter.)
            pw (java.io.PrintWriter. sw)]
        (.printStackTrace e pw)
        (log/log-error (str "Error in CREATE TABLE: " (.getMessage e) "\n" (.toString sw))))
      (messages/send-error-response out (str "Error creating table: " (.getMessage e)))))

  (messages/send-ready-for-query out \I))

;; DROP TABLE handler

(defn handle-drop-table
  "Handles a DROP TABLE statement.
   Parameters:
   - storage: The storage implementation
   - out: Output stream to write results to
   - parsed: The parsed query details
   Returns: nil"
  [storage out parsed]
  (log/log-info (str "Handling DROP TABLE: " (:table parsed)))

  (try
    (let [table-name (:table parsed)
          schema (:schema parsed)
          branch-name (normalize-schema-to-branch schema)
          if-exists (:if-exists parsed)
          tx-opts (ddl-tx-options {:operation "drop-table"
                                   :table table-name
                                   :branch branch-name})
          result (tx/with-transaction [storage tx-opts]
                   (schema-core/drop-table storage table-name branch-name if-exists))]

      (if (:success result)
        (do
          (log/log-info (:message result))
          (messages/send-command-complete out "DROP TABLE" 0))
        (do
          (log/log-error (str "DROP TABLE failed: " (:message result)))
          (messages/send-error-response out (:message result)))))

    (catch Exception e
      (let [sw (java.io.StringWriter.)
            pw (java.io.PrintWriter. sw)]
        (.printStackTrace e pw)
        (log/log-error (str "Error in DROP TABLE: " (.getMessage e) "\n" (.toString sw))))
      (messages/send-error-response out (str "Error dropping table: " (.getMessage e)))))

  (messages/send-ready-for-query out \I))

;; SHOW TABLES handler

(defn handle-show-tables
  "Handles a SHOW TABLES statement.
   Parameters:
   - storage: The storage implementation
   - out: Output stream to write results to
   - parsed: The parsed query details
   Returns: nil"
  [storage out parsed]
  (log/log-info "Handling SHOW TABLES")

  (try
    (let [schema (:schema parsed)
          branch-name (normalize-schema-to-branch schema)
          tables (schema-core/list-tables storage branch-name)
          columns ["table_name" "has_schema"]]

      (log/log-info (str "Found " (count tables) " tables"))

      (messages/send-row-description out columns)

      (doseq [table tables]
        (messages/send-data-row out [(:name table)
                                      (if (:has_schema table) "YES" "NO")]))

      (messages/send-command-complete out "SELECT" (count tables)))

    (catch Exception e
      (let [sw (java.io.StringWriter.)
            pw (java.io.PrintWriter. sw)]
        (.printStackTrace e pw)
        (log/log-error (str "Error in SHOW TABLES: " (.getMessage e) "\n" (.toString sw))))
      (messages/send-error-response out (str "Error listing tables: " (.getMessage e)))))

  (messages/send-ready-for-query out \I))

;; SHOW SCHEMAS handler

(defn handle-show-schemas
  "Handles a SHOW SCHEMAS statement.
   Parameters:
   - storage: The storage implementation
   - out: Output stream to write results to
   - parsed: The parsed query details (not used currently)
   Returns: nil"
  [storage out _parsed]
  (log/log-info "Handling SHOW SCHEMAS")

  (try
    (let [branches (schema-core/list-branches storage)
          columns ["schema_name"]]

      (log/log-info (str "Found " (count branches) " schemas (branches)"))

      (messages/send-row-description out columns)

      (doseq [branch branches]
        ;; Map main branch to public schema for PostgreSQL compatibility
        (let [schema-name (if (= branch "main") "public" branch)]
          (messages/send-data-row out [schema-name])))

      (messages/send-command-complete out "SELECT" (count branches)))

    (catch Exception e
      (let [sw (java.io.StringWriter.)
            pw (java.io.PrintWriter. sw)]
        (.printStackTrace e pw)
        (log/log-error (str "Error in SHOW SCHEMAS: " (.getMessage e) "\n" (.toString sw))))
      (messages/send-error-response out (str "Error listing schemas: " (.getMessage e)))))

  (messages/send-ready-for-query out \I))

;; DESCRIBE handler

(defn handle-describe
  "Handles a DESCRIBE table or SHOW COLUMNS FROM table statement.
   Parameters:
   - storage: The storage implementation
   - out: Output stream to write results to
   - parsed: The parsed query details
   Returns: nil"
  [storage out parsed]
  (log/log-info (str "Handling DESCRIBE: " (:table parsed)))

  (try
    (let [table-name (:table parsed)
          schema (:schema parsed)
          branch-name (normalize-schema-to-branch schema)
          table-info (schema-core/describe-table storage table-name branch-name)
          columns ["Field" "Type" "Null" "Key" "Default"]]

      (if (:error table-info)
        (do
          (log/log-error (str "Table not found: " table-name))
          (messages/send-row-description out columns)
          (messages/send-command-complete out "SELECT" 0))
        (do
          (log/log-info (str "Describing table " table-name " with "
                             (count (:columns table-info)) " columns"
                             (when (:inferred table-info) " (inferred)")))

          (messages/send-row-description out columns)

          (doseq [col (:columns table-info)]
            (let [field (:name col)
                  type-name (or (:type col) "TEXT")
                  nullable (if (false? (:nullable col)) "NO" "YES")
                  key (if (:primary_key col) "PRI" "")
                  default (or (:default col) "")]
              (messages/send-data-row out [field type-name nullable key default])))

          (messages/send-command-complete out "SELECT" (count (:columns table-info))))))

    (catch Exception e
      (let [sw (java.io.StringWriter.)
            pw (java.io.PrintWriter. sw)]
        (.printStackTrace e pw)
        (log/log-error (str "Error in DESCRIBE: " (.getMessage e) "\n" (.toString sw))))
      (messages/send-error-response out (str "Error describing table: " (.getMessage e)))))

  (messages/send-ready-for-query out \I))

;; Branch function handlers

(defn handle-branch-list
  "Handles the chrondb_branch_list() function.
   Parameters:
   - storage: The storage implementation
   - out: Output stream to write results to
   - parsed: The parsed query details
   Returns: nil"
  [storage out _parsed]
  (log/log-info "Handling chrondb_branch_list()")

  (try
    (let [branches (schema-core/list-branches storage)
          columns ["branch_name" "is_default"]]

      (messages/send-row-description out columns)

      (doseq [branch branches]
        (messages/send-data-row out [branch (if (= branch "main") "YES" "NO")]))

      (messages/send-command-complete out "SELECT" (count branches)))

    (catch Exception e
      (log/log-error (str "Error in branch_list: " (.getMessage e)))
      (messages/send-error-response out (str "Error listing branches: " (.getMessage e)))))

  (messages/send-ready-for-query out \I))

(defn handle-branch-create
  "Handles the chrondb_branch_create(name) function.
   Parameters:
   - storage: The storage implementation
   - out: Output stream to write results to
   - parsed: The parsed query details
   Returns: nil"
  [storage out parsed]
  (let [branch-name (:branch-name parsed)]
    (log/log-info (str "Handling chrondb_branch_create('" branch-name "')"))

    (try
      (let [result (schema-core/branch-create storage branch-name)
            columns ["success" "message"]]

        (messages/send-row-description out columns)
        (messages/send-data-row out [(if (:success result) "true" "false")
                                      (:message result)])
        (messages/send-command-complete out "SELECT" 1))

      (catch Exception e
        (log/log-error (str "Error in branch_create: " (.getMessage e)))
        (messages/send-error-response out (str "Error creating branch: " (.getMessage e)))))

    (messages/send-ready-for-query out \I)))

(defn handle-branch-checkout
  "Handles the chrondb_branch_checkout(name) function.
   Parameters:
   - storage: The storage implementation
   - out: Output stream to write results to
   - parsed: The parsed query details
   - session-context: Optional atom with session state
   Returns: nil"
  [storage out parsed session-context]
  (let [branch-name (:branch-name parsed)]
    (log/log-info (str "Handling chrondb_branch_checkout('" branch-name "')"))

    (try
      (let [result (schema-core/branch-checkout storage branch-name)
            columns ["success" "message" "current_branch"]]

        ;; Persist branch selection in session context if available
        (when (and (:success result) session-context)
          (swap! session-context assoc :current-branch (:branch result)))

        (messages/send-row-description out columns)
        (messages/send-data-row out [(if (:success result) "true" "false")
                                      (:message result)
                                      (or (:branch result) "")])
        (messages/send-command-complete out "SELECT" 1))

      (catch Exception e
        (log/log-error (str "Error in branch_checkout: " (.getMessage e)))
        (messages/send-error-response out (str "Error checking out branch: " (.getMessage e)))))

    (messages/send-ready-for-query out \I)))

(defn handle-branch-merge
  "Handles the chrondb_branch_merge(source, target) function.
   Parameters:
   - storage: The storage implementation
   - out: Output stream to write results to
   - parsed: The parsed query details
   Returns: nil"
  [storage out parsed]
  (let [source-branch (:source-branch parsed)
        target-branch (:target-branch parsed)]
    (log/log-info (str "Handling chrondb_branch_merge('" source-branch "', '" target-branch "')"))

    (try
      (let [result (schema-core/branch-merge storage source-branch target-branch)
            columns ["success" "message"]]

        (messages/send-row-description out columns)
        (messages/send-data-row out [(if (:success result) "true" "false")
                                      (:message result)])
        (messages/send-command-complete out "SELECT" 1))

      (catch Exception e
        (log/log-error (str "Error in branch_merge: " (.getMessage e)))
        (messages/send-error-response out (str "Error merging branches: " (.getMessage e)))))

    (messages/send-ready-for-query out \I)))

(defn handle-chrondb-branch-function
  "Dispatcher for ChronDB branch functions.
   Parameters:
   - storage: The storage implementation
   - out: Output stream to write results to
   - parsed: The parsed query details
   - session-context: Optional atom with session state
   Returns: nil"
  [storage out parsed session-context]
  (case (:function parsed)
    :branch-list (handle-branch-list storage out parsed)
    :branch-create (handle-branch-create storage out parsed)
    :branch-checkout (handle-branch-checkout storage out parsed session-context)
    :branch-merge (handle-branch-merge storage out parsed)
    (do
      (log/log-error (str "Unknown branch function: " (:function parsed)))
      (messages/send-error-response out (str "Unknown branch function: " (:function parsed)))
      (messages/send-ready-for-query out \I))))

;; =============================================================================
;; Validation Schema DDL Handlers
;; =============================================================================

(defn handle-create-validation-schema
  "Handles a CREATE VALIDATION SCHEMA statement.
   Syntax: CREATE VALIDATION SCHEMA FOR namespace AS 'json-schema' MODE strict|warning
   Parameters:
   - storage: The storage implementation
   - out: Output stream to write results to
   - parsed: The parsed query details
   Returns: nil"
  [storage out parsed]
  (log/log-info (str "Handling CREATE VALIDATION SCHEMA FOR: " (:namespace parsed)))

  (try
    (let [namespace (:namespace parsed)
          schema-json (:schema parsed)
          mode (or (:mode parsed) :strict)
          schema (:schema parsed)
          branch-name (normalize-schema-to-branch schema)
          repository (:repository storage)
          _ (require 'chrondb.validation.storage)
          _ (require 'clojure.data.json)
          read-json (resolve 'clojure.data.json/read-str)
          save-fn (resolve 'chrondb.validation.storage/save-validation-schema)
          schema-def (read-json schema-json)
          result (save-fn repository namespace schema-def mode nil branch-name)]

      (log/log-info (str "Created validation schema for " namespace
                         " (version " (:version result) ", mode " (:mode result) ")"))
      (messages/send-command-complete out "CREATE VALIDATION SCHEMA" 0))

    (catch Exception e
      (let [sw (java.io.StringWriter.)
            pw (java.io.PrintWriter. sw)]
        (.printStackTrace e pw)
        (log/log-error (str "Error in CREATE VALIDATION SCHEMA: " (.getMessage e) "\n" (.toString sw))))
      (messages/send-error-response out (str "Error creating validation schema: " (.getMessage e)))))

  (messages/send-ready-for-query out \I))

(defn handle-drop-validation-schema
  "Handles a DROP VALIDATION SCHEMA statement.
   Syntax: DROP VALIDATION SCHEMA FOR namespace
   Parameters:
   - storage: The storage implementation
   - out: Output stream to write results to
   - parsed: The parsed query details
   Returns: nil"
  [storage out parsed]
  (log/log-info (str "Handling DROP VALIDATION SCHEMA FOR: " (:namespace parsed)))

  (try
    (let [namespace (:namespace parsed)
          schema (:schema parsed)
          branch-name (normalize-schema-to-branch schema)
          repository (:repository storage)
          _ (require 'chrondb.validation.storage)
          delete-fn (resolve 'chrondb.validation.storage/delete-validation-schema)
          result (delete-fn repository namespace branch-name)]

      (if result
        (do
          (log/log-info (str "Dropped validation schema for " namespace))
          (messages/send-command-complete out "DROP VALIDATION SCHEMA" 0))
        (do
          (log/log-error (str "Validation schema not found for " namespace))
          (messages/send-error-response out (str "Validation schema not found for: " namespace)))))

    (catch Exception e
      (let [sw (java.io.StringWriter.)
            pw (java.io.PrintWriter. sw)]
        (.printStackTrace e pw)
        (log/log-error (str "Error in DROP VALIDATION SCHEMA: " (.getMessage e) "\n" (.toString sw))))
      (messages/send-error-response out (str "Error dropping validation schema: " (.getMessage e)))))

  (messages/send-ready-for-query out \I))

(defn handle-show-validation-schema
  "Handles a SHOW VALIDATION SCHEMA statement.
   Syntax: SHOW VALIDATION SCHEMA FOR namespace
   Parameters:
   - storage: The storage implementation
   - out: Output stream to write results to
   - parsed: The parsed query details
   Returns: nil"
  [storage out parsed]
  (log/log-info (str "Handling SHOW VALIDATION SCHEMA FOR: " (:namespace parsed)))

  (try
    (let [namespace (:namespace parsed)
          schema (:schema parsed)
          branch-name (normalize-schema-to-branch schema)
          repository (:repository storage)
          _ (require 'chrondb.validation.storage)
          _ (require 'clojure.data.json)
          write-json (resolve 'clojure.data.json/write-str)
          get-fn (resolve 'chrondb.validation.storage/get-validation-schema)
          result (get-fn repository namespace branch-name)
          columns ["namespace" "version" "mode" "schema" "created_at" "created_by"]]

      (messages/send-row-description out columns)

      (if result
        (do
          (messages/send-data-row out [(:namespace result)
                                        (str (:version result))
                                        (:mode result)
                                        (write-json (:schema result))
                                        (str (:created_at result))
                                        (or (:created_by result) "")])
          (messages/send-command-complete out "SELECT" 1))
        (messages/send-command-complete out "SELECT" 0)))

    (catch Exception e
      (let [sw (java.io.StringWriter.)
            pw (java.io.PrintWriter. sw)]
        (.printStackTrace e pw)
        (log/log-error (str "Error in SHOW VALIDATION SCHEMA: " (.getMessage e) "\n" (.toString sw))))
      (messages/send-error-response out (str "Error showing validation schema: " (.getMessage e)))))

  (messages/send-ready-for-query out \I))

(defn handle-show-validation-schemas
  "Handles a SHOW VALIDATION SCHEMAS statement (list all).
   Syntax: SHOW VALIDATION SCHEMAS
   Parameters:
   - storage: The storage implementation
   - out: Output stream to write results to
   - parsed: The parsed query details
   Returns: nil"
  [storage out parsed]
  (log/log-info "Handling SHOW VALIDATION SCHEMAS")

  (try
    (let [schema (:schema parsed)
          branch-name (normalize-schema-to-branch schema)
          repository (:repository storage)
          _ (require 'chrondb.validation.storage)
          list-fn (resolve 'chrondb.validation.storage/list-validation-schemas)
          result (list-fn repository branch-name)
          columns ["namespace" "version" "mode" "created_at"]]

      (messages/send-row-description out columns)

      (doseq [vs result]
        (messages/send-data-row out [(:namespace vs)
                                      (str (:version vs))
                                      (:mode vs)
                                      (str (:created_at vs))]))

      (messages/send-command-complete out "SELECT" (count result)))

    (catch Exception e
      (let [sw (java.io.StringWriter.)
            pw (java.io.PrintWriter. sw)]
        (.printStackTrace e pw)
        (log/log-error (str "Error in SHOW VALIDATION SCHEMAS: " (.getMessage e) "\n" (.toString sw))))
      (messages/send-error-response out (str "Error listing validation schemas: " (.getMessage e)))))

  (messages/send-ready-for-query out \I))

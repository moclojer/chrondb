(ns chrondb.api.sql.schema.core
  "High-level schema operations for DDL commands"
  (:require [clojure.string :as str]
            [chrondb.util.logging :as log]
            [chrondb.api.sql.schema.storage :as storage]
            [chrondb.storage.protocol :as storage-protocol]
            [chrondb.config :as config])
  (:import [org.eclipse.jgit.api Git]
           [org.eclipse.jgit.lib Constants]
           [org.eclipse.jgit.revwalk RevWalk]
           [org.eclipse.jgit.treewalk TreeWalk]
           [org.eclipse.jgit.treewalk.filter PathSuffixFilter]))

(defn create-table
  "Creates a new table schema.
   Parameters:
   - storage: The storage implementation (with :repository)
   - table-name: The name of the table
   - columns: Vector of column definitions
   - branch: Optional branch name
   - if-not-exists: If true, don't error if table already exists
   Returns: Map with :success, :message, and optionally :schema"
  [storage table-name columns branch if-not-exists]
  (let [repository (:repository storage)
        exists? (storage/schema-exists? repository table-name branch)]
    (cond
      ;; Table already exists and IF NOT EXISTS is set
      (and exists? if-not-exists)
      {:success true
       :message (str "Table " table-name " already exists")
       :already-exists true}

      ;; Table already exists without IF NOT EXISTS
      exists?
      {:success false
       :message (str "Table " table-name " already exists")
       :error :table-exists}

      ;; Create the table
      :else
      (try
        (let [schema-def {:columns columns}
              saved-schema (storage/save-schema repository table-name schema-def branch)]
          (log/log-info (str "Created table " table-name " with " (count columns) " columns"))
          {:success true
           :message (str "Table " table-name " created")
           :schema saved-schema})
        (catch Exception e
          (log/log-error (str "Failed to create table " table-name ": " (.getMessage e)))
          {:success false
           :message (.getMessage e)
           :error :create-failed})))))

(defn drop-table
  "Drops a table schema.
   Parameters:
   - storage: The storage implementation (with :repository)
   - table-name: The name of the table
   - branch: Optional branch name
   - if-exists: If true, don't error if table doesn't exist
   Returns: Map with :success and :message"
  [storage table-name branch if-exists]
  (let [repository (:repository storage)
        exists? (storage/schema-exists? repository table-name branch)]
    (cond
      ;; Table doesn't exist and IF EXISTS is set
      (and (not exists?) if-exists)
      {:success true
       :message (str "Table " table-name " does not exist")
       :not-found true}

      ;; Table doesn't exist without IF EXISTS
      (not exists?)
      {:success false
       :message (str "Table " table-name " does not exist")
       :error :table-not-found}

      ;; Drop the table
      :else
      (try
        (storage/delete-schema repository table-name branch)
        (log/log-info (str "Dropped table " table-name))
        {:success true
         :message (str "Table " table-name " dropped")}
        (catch Exception e
          (log/log-error (str "Failed to drop table " table-name ": " (.getMessage e)))
          {:success false
           :message (.getMessage e)
           :error :drop-failed})))))

(defn- get-implicit-tables
  "Gets a list of tables that have documents but no explicit schema.
   Parameters:
   - repository: The Git repository
   - branch: Optional branch name
   Returns: Set of table names"
  [repository branch]
  (let [config-map (config/load-config)
        branch-name (or branch (get-in config-map [:git :default-branch]))
        head-id (.resolve repository (str branch-name "^{commit}"))]

    (if head-id
      (let [tree-walk (TreeWalk. repository)
            rev-walk (RevWalk. repository)]
        (try
          (.addTree tree-walk (.parseTree rev-walk head-id))
          (.setRecursive tree-walk true)
          (.setFilter tree-walk (PathSuffixFilter/create ".json"))

          (loop [tables #{}]
            (if (.next tree-walk)
              (let [path (.getPathString tree-walk)
                    ;; Skip _schema directory
                    skip? (str/starts-with? path "_schema/")
                    ;; Extract table name from path like "users/users_COLON_1.json"
                    parts (str/split path #"/")
                    table-name (when (and (not skip?) (> (count parts) 0))
                                 (first parts))]
                (if (and table-name (not (str/blank? table-name)))
                  (recur (conj tables table-name))
                  (recur tables)))
              tables))
          (finally
            (.close tree-walk)
            (.close rev-walk))))
      #{})))

(defn list-tables
  "Lists all tables (both explicit schemas and implicit tables with documents).
   Parameters:
   - storage: The storage implementation (with :repository)
   - branch: Optional branch name (schema in SQL terms)
   Returns: Vector of table info maps with :name, :has-schema"
  [storage branch]
  (let [repository (:repository storage)
        ;; Get explicit schemas
        schemas (storage/list-schemas repository branch)
        explicit-tables (set (map :table schemas))
        ;; Get implicit tables (tables with documents but no schema)
        implicit-tables (get-implicit-tables repository branch)
        ;; Combine both
        all-tables (into explicit-tables implicit-tables)]

    (->> all-tables
         (map (fn [table-name]
                {:name table-name
                 :has_schema (contains? explicit-tables table-name)}))
         (sort-by :name)
         vec)))

(defn list-branches
  "Lists all Git branches (schemas in SQL terms).
   Parameters:
   - storage: The storage implementation (with :repository)
   Returns: Vector of branch names"
  [storage]
  (let [repository (:repository storage)]
    (when-not repository
      (throw (Exception. "Repository is closed")))

    (try
      (let [ref-database (.getRefDatabase repository)
            refs (.getRefsByPrefix ref-database "refs/heads/")]
        (->> refs
             (map (fn [ref]
                    (let [name (.getName ref)]
                      (str/replace name "refs/heads/" ""))))
             (sort)
             vec))
      (catch Exception e
        (log/log-error (str "Failed to list branches: " (.getMessage e)))
        []))))

(defn- infer-column-type
  "Infers a SQL type from a Clojure/JSON value.
   Parameters:
   - value: The value to infer type from
   Returns: SQL type string"
  [value]
  (cond
    (nil? value) "TEXT"
    (string? value) "TEXT"
    (integer? value) "INTEGER"
    (float? value) "NUMERIC"
    (boolean? value) "BOOLEAN"
    (instance? java.util.Date value) "TIMESTAMP"
    (map? value) "JSONB"
    (sequential? value) "JSONB"
    :else "TEXT"))

(defn- infer-schema-from-documents
  "Infers a schema from existing documents in a table.
   Parameters:
   - storage: The storage implementation
   - table-name: The name of the table
   - branch: Optional branch name
   Returns: Vector of column definitions or nil if no documents"
  [storage table-name branch]
  (try
    (let [docs (storage-protocol/get-documents-by-table storage table-name branch)
          ;; Take a sample of documents to infer schema
          sample-docs (take 10 docs)]
      (when (seq sample-docs)
        ;; Collect all unique keys and their types
        (let [columns-map (reduce
                           (fn [acc doc]
                             (reduce
                              (fn [inner-acc [k v]]
                                ;; Skip internal fields
                                (if (str/starts-with? (name k) "_")
                                  inner-acc
                                  (let [col-name (name k)
                                        col-type (infer-column-type v)]
                                    (assoc inner-acc col-name col-type))))
                              acc
                              doc))
                           {}
                           sample-docs)]
          (->> columns-map
               (map (fn [[col-name col-type]]
                      {:name col-name
                       :type col-type
                       :nullable true}))
               (sort-by :name)
               vec))))
    (catch Exception e
      (log/log-error (str "Failed to infer schema for " table-name ": " (.getMessage e)))
      nil)))

(defn describe-table
  "Describes a table's schema.
   If the table has an explicit schema, returns that.
   Otherwise, tries to infer the schema from existing documents.
   Parameters:
   - storage: The storage implementation (with :repository)
   - table-name: The name of the table
   - branch: Optional branch name
   Returns: Map with :name, :columns, :inferred (true if schema was inferred)"
  [storage table-name branch]
  (let [repository (:repository storage)
        explicit-schema (storage/get-schema repository table-name branch)]
    (if explicit-schema
      ;; Return explicit schema
      {:name (:table explicit-schema)
       :columns (:columns explicit-schema)
       :inferred false
       :created_at (:created_at explicit-schema)}
      ;; Try to infer schema from documents
      (let [inferred-columns (infer-schema-from-documents storage table-name branch)]
        (if inferred-columns
          {:name table-name
           :columns inferred-columns
           :inferred true}
          {:name table-name
           :columns []
           :error :table-not-found})))))

;; Branch management functions

(defn branch-create
  "Creates a new branch.
   Parameters:
   - storage: The storage implementation (with :repository)
   - branch-name: Name of the new branch
   Returns: Map with :success and :message"
  [storage branch-name]
  (let [repository (:repository storage)]
    (when-not repository
      (throw (Exception. "Repository is closed")))

    (try
      (let [existing-ref (.exactRef repository (str "refs/heads/" branch-name))]
        (if existing-ref
          {:success false
           :message (str "Branch " branch-name " already exists")
           :error :branch-exists}
          (do
            ;; Create the branch from HEAD
            (let [head-id (.resolve repository Constants/HEAD)]
              (when head-id
                (let [ref-update (.updateRef repository (str "refs/heads/" branch-name))]
                  (.setNewObjectId ref-update head-id)
                  (.update ref-update))))
            (log/log-info (str "Created branch " branch-name))
            {:success true
             :message (str "Branch " branch-name " created")})))
      (catch Exception e
        (log/log-error (str "Failed to create branch " branch-name ": " (.getMessage e)))
        {:success false
         :message (.getMessage e)
         :error :create-failed}))))

(defn branch-checkout
  "Switches to a branch (sets session context).
   Note: In ChronDB, branch checkout is per-session, not physical checkout.
   Parameters:
   - storage: The storage implementation
   - branch-name: Name of the branch to switch to
   Returns: Map with :success and :message"
  [storage branch-name]
  (let [repository (:repository storage)]
    (when-not repository
      (throw (Exception. "Repository is closed")))

    (try
      (let [ref (.exactRef repository (str "refs/heads/" branch-name))]
        (if ref
          (do
            (log/log-info (str "Switched to branch " branch-name))
            {:success true
             :message (str "Switched to branch " branch-name)
             :branch branch-name})
          {:success false
           :message (str "Branch " branch-name " does not exist")
           :error :branch-not-found}))
      (catch Exception e
        (log/log-error (str "Failed to checkout branch " branch-name ": " (.getMessage e)))
        {:success false
         :message (.getMessage e)
         :error :checkout-failed}))))

(defn branch-merge
  "Merges one branch into another.
   Parameters:
   - storage: The storage implementation (with :repository)
   - source-branch: Branch to merge from
   - target-branch: Branch to merge into
   Returns: Map with :success and :message"
  [storage source-branch target-branch]
  (let [repository (:repository storage)]
    (when-not repository
      (throw (Exception. "Repository is closed")))

    (try
      (let [git (Git/wrap repository)
            source-ref (.exactRef repository (str "refs/heads/" source-branch))
            target-ref (.exactRef repository (str "refs/heads/" target-branch))]
        (cond
          (nil? source-ref)
          {:success false
           :message (str "Source branch " source-branch " does not exist")
           :error :source-not-found}

          (nil? target-ref)
          {:success false
           :message (str "Target branch " target-branch " does not exist")
           :error :target-not-found}

          :else
          ;; Perform the merge
          (let [merge-result (-> git
                                 (.merge)
                                 (.include source-ref)
                                 (.setMessage (str "Merge branch '" source-branch "' into " target-branch))
                                 (.call))]
            (if (.isSuccessful merge-result)
              (do
                (log/log-info (str "Merged " source-branch " into " target-branch))
                {:success true
                 :message (str "Merged " source-branch " into " target-branch)})
              {:success false
               :message "Merge failed - conflicts detected"
               :error :merge-conflict}))))
      (catch Exception e
        (log/log-error (str "Failed to merge " source-branch " into " target-branch ": " (.getMessage e)))
        {:success false
         :message (.getMessage e)
         :error :merge-failed}))))

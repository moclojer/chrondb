(ns chrondb.api.sql.schema.storage
  "Schema storage operations - persists table schemas in Git as JSON files in _schema/ directory"
  (:require [clojure.string :as str]
            [clojure.data.json :as json]
            [chrondb.config :as config]
            [chrondb.util.logging :as log]
            [chrondb.storage.git.commit :as commit])
  (:import [org.eclipse.jgit.api Git]
           [org.eclipse.jgit.revwalk RevWalk]
           [org.eclipse.jgit.treewalk TreeWalk]
           [org.eclipse.jgit.treewalk.filter PathFilter PathSuffixFilter]))

(def ^:const schema-dir "_schema")

(defn schema-path
  "Returns the path to a schema file for a given table name.
   Parameters:
   - table-name: The name of the table
   Returns: Path string like '_schema/tablename.json'"
  [table-name]
  (str schema-dir "/" (str/lower-case table-name) ".json"))

(defn save-schema
  "Saves a table schema to the Git repository.
   Parameters:
   - repository: The Git repository
   - table-name: The name of the table
   - schema-def: The schema definition map (columns, constraints, etc.)
   - branch: Optional branch name
   Returns: The saved schema definition"
  [repository table-name schema-def branch]
  (when-not repository
    (throw (Exception. "Repository is closed")))
  (when-not table-name
    (throw (Exception. "Table name cannot be nil")))

  (let [config-map (config/load-config)
        branch-name (or branch (get-in config-map [:git :default-branch]))
        path (schema-path table-name)
        ;; Add metadata to schema
        full-schema (merge schema-def
                           {:table (str/lower-case table-name)
                            :created_at (java.time.Instant/now)})
        content (json/write-str full-schema)]

    (log/log-info (str "Saving schema for table " table-name " to " path))

    (commit/commit-virtual (Git/wrap repository)
                           branch-name
                           path
                           content
                           (str "Create schema for table " table-name)
                           (get-in config-map [:git :committer-name])
                           (get-in config-map [:git :committer-email])
                           {:note {:operation "create-schema"
                                   :table table-name}})

    (commit/push-changes (Git/wrap repository) config-map)

    full-schema))

(defn get-schema
  "Retrieves a table schema from the Git repository.
   Parameters:
   - repository: The Git repository
   - table-name: The name of the table
   - branch: Optional branch name
   Returns: The schema definition map or nil if not found"
  [repository table-name branch]
  (when-not repository
    (throw (Exception. "Repository is closed")))

  (let [config-map (config/load-config)
        branch-name (or branch (get-in config-map [:git :default-branch]))
        path (schema-path table-name)
        head-id (.resolve repository (str branch-name "^{commit}"))]

    (when head-id
      (let [tree-walk (TreeWalk. repository)
            rev-walk (RevWalk. repository)]
        (try
          (.addTree tree-walk (.parseTree rev-walk head-id))
          (.setRecursive tree-walk true)
          (.setFilter tree-walk (PathFilter/create path))

          (when (.next tree-walk)
            (let [object-id (.getObjectId tree-walk 0)
                  object-loader (.open repository object-id)
                  content (String. (.getBytes object-loader) "UTF-8")]
              (try
                (json/read-str content :key-fn keyword)
                (catch Exception e
                  (log/log-error (str "Failed to parse schema for table " table-name ": " (.getMessage e)))
                  nil))))
          (finally
            (.close tree-walk)
            (.close rev-walk)))))))

(defn delete-schema
  "Deletes a table schema from the Git repository.
   Parameters:
   - repository: The Git repository
   - table-name: The name of the table
   - branch: Optional branch name
   Returns: true if deleted, false if not found"
  [repository table-name branch]
  (when-not repository
    (throw (Exception. "Repository is closed")))

  (let [config-map (config/load-config)
        branch-name (or branch (get-in config-map [:git :default-branch]))
        path (schema-path table-name)
        head-id (.resolve repository (str branch-name "^{commit}"))]

    (when head-id
      (let [tree-walk (TreeWalk. repository)
            rev-walk (RevWalk. repository)]
        (try
          (.addTree tree-walk (.parseTree rev-walk head-id))
          (.setRecursive tree-walk true)
          (.setFilter tree-walk (PathFilter/create path))

          (if (.next tree-walk)
            (do
              (log/log-info (str "Deleting schema for table " table-name))
              (commit/commit-virtual (Git/wrap repository)
                                     branch-name
                                     path
                                     nil  ;; nil content = delete
                                     (str "Drop schema for table " table-name)
                                     (get-in config-map [:git :committer-name])
                                     (get-in config-map [:git :committer-email])
                                     {:note {:operation "drop-schema"
                                             :table table-name
                                             :flags ["delete"]}})
              (commit/push-changes (Git/wrap repository) config-map)
              true)
            (do
              (log/log-info (str "Schema for table " table-name " not found"))
              false))
          (finally
            (.close tree-walk)
            (.close rev-walk)))))))

(defn- parse-schema-safe
  "Safely parses a JSON schema, returning nil on error."
  [content path]
  (try
    (json/read-str content :key-fn keyword)
    (catch Exception e
      (log/log-error (str "Failed to parse schema at " path ": " (.getMessage e)))
      nil)))

(defn list-schemas
  "Lists all table schemas in the Git repository.
   Parameters:
   - repository: The Git repository
   - branch: Optional branch name
   Returns: Sequence of schema definition maps"
  [repository branch]
  (when-not repository
    (throw (Exception. "Repository is closed")))

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

          (loop [schemas []]
            (if (.next tree-walk)
              (let [path (.getPathString tree-walk)]
                (if (str/starts-with? path (str schema-dir "/"))
                  (let [object-id (.getObjectId tree-walk 0)
                        object-loader (.open repository object-id)
                        content (String. (.getBytes object-loader) "UTF-8")
                        schema (parse-schema-safe content path)]
                    (recur (if schema (conj schemas schema) schemas)))
                  (recur schemas)))
              schemas))
          (finally
            (.close tree-walk)
            (.close rev-walk))))
      [])))

(defn schema-exists?
  "Checks if a schema exists for the given table.
   Parameters:
   - repository: The Git repository
   - table-name: The name of the table
   - branch: Optional branch name
   Returns: true if schema exists, false otherwise"
  [repository table-name branch]
  (when-not repository
    (throw (Exception. "Repository is closed")))

  (let [config-map (config/load-config)
        branch-name (or branch (get-in config-map [:git :default-branch]))
        path (schema-path table-name)
        head-id (.resolve repository (str branch-name "^{commit}"))]

    (if head-id
      (let [tree-walk (TreeWalk. repository)
            rev-walk (RevWalk. repository)]
        (try
          (.addTree tree-walk (.parseTree rev-walk head-id))
          (.setRecursive tree-walk true)
          (.setFilter tree-walk (PathFilter/create path))
          (.next tree-walk)
          (finally
            (.close tree-walk)
            (.close rev-walk))))
      false)))

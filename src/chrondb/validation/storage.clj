(ns chrondb.validation.storage
  "Validation schema storage operations - persists validation schemas in Git
   as JSON files in _schema/validation/ directory"
  (:require [clojure.string :as str]
            [clojure.data.json :as json]
            [chrondb.config :as config]
            [chrondb.util.logging :as log]
            [chrondb.storage.git.commit :as commit]
            [chrondb.validation.schema :as schema])
  (:import [org.eclipse.jgit.api Git]
           [org.eclipse.jgit.revwalk RevWalk]
           [org.eclipse.jgit.treewalk TreeWalk]
           [org.eclipse.jgit.treewalk.filter PathFilter PathSuffixFilter]))

(def ^:const validation-schema-dir "_schema/validation")

(defn validation-schema-path
  "Returns the path to a validation schema file for a given namespace.
   Parameters:
   - namespace: The name of the table/namespace
   Returns: Path string like '_schema/validation/namespace.json'"
  [namespace]
  (str validation-schema-dir "/" (str/lower-case namespace) ".json"))

;; Forward declaration for save-validation-schema to reference get-validation-schema
(declare get-validation-schema)

(defn save-validation-schema
  "Saves a validation schema to the Git repository.
   Parameters:
   - repository: The Git repository
   - namespace: The name of the table/namespace
   - schema-def: The JSON Schema definition (map or JSON string)
   - mode: Validation mode (:strict, :warning, or :disabled)
   - user: Optional user who created/updated the schema
   - branch: Optional branch name
   Returns: The saved schema definition with metadata"
  [repository namespace schema-def mode user branch]
  (when-not repository
    (throw (Exception. "Repository is closed")))
  (when-not namespace
    (throw (Exception. "Namespace cannot be nil")))
  (when-not schema-def
    (throw (Exception. "Schema definition cannot be nil")))

  (let [config-map (config/load-config)
        branch-name (or branch (get-in config-map [:git :default-branch]))
        path (validation-schema-path namespace)
        ;; Get existing schema to increment version
        existing (get-validation-schema repository namespace branch-name)
        version (if existing (inc (:version existing)) 1)
        ;; Build full schema document
        full-schema {:namespace (str/lower-case namespace)
                     :version version
                     :mode (if (keyword? mode) (name mode) (str mode))
                     :schema (if (string? schema-def)
                               (json/read-str schema-def)
                               schema-def)
                     :created_at (str (java.time.Instant/now))
                     :created_by (or user "system")}
        content (json/write-str full-schema)]

    (log/log-info (str "Saving validation schema for namespace " namespace
                       " (version " version ", mode " mode ") to " path))

    ;; Invalidate cache for this namespace
    (schema/invalidate-cache namespace branch-name)

    (commit/commit-virtual (Git/wrap repository)
                           branch-name
                           path
                           content
                           (str (if (> version 1) "Update" "Create")
                                " validation schema for " namespace)
                           (get-in config-map [:git :committer-name])
                           (get-in config-map [:git :committer-email])
                           {:note {:operation (if (> version 1)
                                                "update-validation-schema"
                                                "create-validation-schema")
                                   :namespace namespace
                                   :version version
                                   :mode (if (keyword? mode) (name mode) (str mode))}})

    (commit/push-changes (Git/wrap repository) config-map)

    full-schema))

(defn get-validation-schema
  "Retrieves a validation schema from the Git repository.
   Parameters:
   - repository: The Git repository
   - namespace: The name of the table/namespace
   - branch: Optional branch name
   Returns: The schema definition map or nil if not found"
  [repository namespace branch]
  (when-not repository
    (throw (Exception. "Repository is closed")))

  (let [config-map (config/load-config)
        branch-name (or branch (get-in config-map [:git :default-branch]))
        path (validation-schema-path namespace)
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
                  (log/log-error (str "Failed to parse validation schema for " namespace
                                      ": " (.getMessage e)))
                  nil))))
          (finally
            (.close tree-walk)
            (.close rev-walk)))))))

(defn delete-validation-schema
  "Deletes a validation schema from the Git repository.
   Parameters:
   - repository: The Git repository
   - namespace: The name of the table/namespace
   - branch: Optional branch name
   Returns: true if deleted, false if not found"
  [repository namespace branch]
  (when-not repository
    (throw (Exception. "Repository is closed")))

  (let [config-map (config/load-config)
        branch-name (or branch (get-in config-map [:git :default-branch]))
        path (validation-schema-path namespace)
        head-id (.resolve repository (str branch-name "^{commit}"))]

    ;; Invalidate cache
    (schema/invalidate-cache namespace branch-name)

    (when head-id
      (let [tree-walk (TreeWalk. repository)
            rev-walk (RevWalk. repository)]
        (try
          (.addTree tree-walk (.parseTree rev-walk head-id))
          (.setRecursive tree-walk true)
          (.setFilter tree-walk (PathFilter/create path))

          (if (.next tree-walk)
            (do
              (log/log-info (str "Deleting validation schema for namespace " namespace))
              (commit/commit-virtual (Git/wrap repository)
                                     branch-name
                                     path
                                     nil  ;; nil content = delete
                                     (str "Drop validation schema for " namespace)
                                     (get-in config-map [:git :committer-name])
                                     (get-in config-map [:git :committer-email])
                                     {:note {:operation "drop-validation-schema"
                                             :namespace namespace
                                             :flags ["delete"]}})
              (commit/push-changes (Git/wrap repository) config-map)
              true)
            (do
              (log/log-info (str "Validation schema for namespace " namespace " not found"))
              false))
          (finally
            (.close tree-walk)
            (.close rev-walk)))))))

(defn- parse-validation-schema-safe
  "Safely parses a JSON validation schema, returning nil on error."
  [content path]
  (try
    (json/read-str content :key-fn keyword)
    (catch Exception e
      (log/log-error (str "Failed to parse validation schema at " path ": " (.getMessage e)))
      nil)))

(defn list-validation-schemas
  "Lists all validation schemas in the Git repository.
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
                (if (str/starts-with? path (str validation-schema-dir "/"))
                  (let [object-id (.getObjectId tree-walk 0)
                        object-loader (.open repository object-id)
                        content (String. (.getBytes object-loader) "UTF-8")
                        schema (parse-validation-schema-safe content path)]
                    (recur (if schema (conj schemas schema) schemas)))
                  (recur schemas)))
              schemas))
          (finally
            (.close tree-walk)
            (.close rev-walk))))
      [])))

(defn validation-schema-exists?
  "Checks if a validation schema exists for the given namespace.
   Parameters:
   - repository: The Git repository
   - namespace: The name of the table/namespace
   - branch: Optional branch name
   Returns: true if schema exists, false otherwise"
  [repository namespace branch]
  (when-not repository
    (throw (Exception. "Repository is closed")))

  (let [config-map (config/load-config)
        branch-name (or branch (get-in config-map [:git :default-branch]))
        path (validation-schema-path namespace)
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

(defn get-validation-schema-history
  "Gets the history of changes for a validation schema.
   Parameters:
   - repository: The Git repository
   - namespace: The name of the table/namespace
   - branch: Optional branch name
   Returns: Sequence of commit information for the schema file"
  [repository namespace branch]
  (when-not repository
    (throw (Exception. "Repository is closed")))

  (let [config-map (config/load-config)
        branch-name (or branch (get-in config-map [:git :default-branch]))
        path (validation-schema-path namespace)
        git (Git/wrap repository)]
    (try
      (let [log-command (-> git
                            (.log)
                            (.addPath path))
            commits (iterator-seq (.iterator (.call log-command)))]
        (mapv (fn [commit]
                {:commit-id (.getName commit)
                 :timestamp (str (java.time.Instant/ofEpochSecond
                                   (.getCommitTime commit)))
                 :committer (.getName (.getCommitterIdent commit))
                 :message (.getShortMessage commit)})
              commits))
      (catch Exception e
        (log/log-error (str "Error getting validation schema history: " (.getMessage e)))
        []))))

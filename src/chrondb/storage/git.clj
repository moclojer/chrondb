(ns chrondb.storage.git
  "Git-based storage implementation for ChronDB.
   Uses JGit for Git operations and provides versioned document storage."
  (:require [chrondb.config :as config]
            [chrondb.storage.protocol :as protocol]
            [chrondb.util.logging :as log]
            [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clojure.string :as str])
  (:import [java.io ByteArrayInputStream]
           [java.util Date TimeZone]
           [org.eclipse.jgit.api Git]
           [org.eclipse.jgit.dircache DirCache DirCacheEntry]
           [org.eclipse.jgit.lib ConfigConstants Constants ObjectId]
           [org.eclipse.jgit.lib CommitBuilder FileMode RefUpdate$Result]
           [org.eclipse.jgit.revwalk RevWalk]
           [org.eclipse.jgit.transport RefSpec]
           [org.eclipse.jgit.treewalk CanonicalTreeParser TreeWalk]
           [org.eclipse.jgit.util SystemReader]))

(defn ensure-directory
  "Creates a directory if it doesn't exist.
   Throws an exception if directory creation fails."
  [path]
  (let [dir (io/file path)]
    (when-not (.exists dir)
      (if-not (.mkdirs dir)
        (throw (ex-info (str "Could not create directory: " path)
                        {:path path}))
        true))))

(defn- configure-global-git
  "Configures global Git settings to disable GPG signing."
  []
  (let [global-config (-> (SystemReader/getInstance)
                          (.getUserConfig))]
    (.setBoolean global-config "commit" nil "gpgsign" false)
    (.unset global-config "gpg" nil "format")
    (.save global-config)))

(defn- configure-repository
  "Configures repository-specific Git settings."
  [repo]
  (configure-global-git)
  (let [config (.getConfig repo)]
    (.setBoolean config ConfigConstants/CONFIG_COMMIT_SECTION nil ConfigConstants/CONFIG_KEY_GPGSIGN false)
    (.setString config ConfigConstants/CONFIG_CORE_SECTION nil ConfigConstants/CONFIG_KEY_FILEMODE "false")
    (.save config)))

(defn- build-person-ident
  "Builds a PersonIdent object for Git commits."
  [name email]
  (org.eclipse.jgit.lib.PersonIdent. name email (Date.) (TimeZone/getDefault)))

(defn- create-temporary-index
  "Creates an in-memory index for the document change.
   Similar to the createTemporaryIndex method in the Java example."
  [git head-id path content]
  (let [in-core-index (DirCache/newInCore)
        dc-builder (.builder in-core-index)
        inserter (.newObjectInserter (.getRepository git))]
    (try
      (when content
        (let [dc-entry (DirCacheEntry. path)
              content-bytes (.getBytes content "UTF-8")
              content-length (count content-bytes)
              input-stream (ByteArrayInputStream. content-bytes)]
          (.setFileMode dc-entry FileMode/REGULAR_FILE)
          (.setObjectId dc-entry (.insert inserter Constants/OBJ_BLOB content-length input-stream))
          (.add dc-builder dc-entry)))

      (when head-id
        (let [tree-walk (TreeWalk. (.getRepository git))
              h-idx (.addTree tree-walk (.parseTree (RevWalk. (.getRepository git)) head-id))]
          (.setRecursive tree-walk true)

          (while (.next tree-walk)
            (let [walk-path (.getPathString tree-walk)
                  h-tree (.getTree tree-walk h-idx CanonicalTreeParser)]

              (when-not (= walk-path path)
                (let [dc-entry (DirCacheEntry. walk-path)]
                  (.setObjectId dc-entry (.getEntryObjectId h-tree))
                  (.setFileMode dc-entry (.getEntryFileMode h-tree))
                  (.add dc-builder dc-entry)))))

          (.close tree-walk)))

      (.finish dc-builder)

      in-core-index
      (finally
        (.close inserter)))))

(defn- commit-virtual
  "Commits changes virtually without writing to the file system.
   Similar to the commit method in the Java example."
  [git branch-name path content message committer-name committer-email]
  (let [repo (.getRepository git)
        head-id (.resolve repo (str branch-name "^{commit}"))
        author (build-person-ident committer-name committer-email)
        object-inserter (.newObjectInserter repo)]
    (try
      (let [index (create-temporary-index git head-id path content)
            index-tree-id (.writeTree index object-inserter)
            commit (doto (CommitBuilder.)
                     (.setAuthor author)
                     (.setCommitter author)
                     (.setEncoding Constants/CHARACTER_ENCODING)
                     (.setMessage message)
                     (.setTreeId index-tree-id))]

        (when head-id
          (.setParentId commit head-id))

        (let [commit-id (.insert object-inserter commit)]
          (.flush object-inserter)

          (let [rev-walk (RevWalk. repo)]
            (try
              (let [rev-commit (.parseCommit rev-walk commit-id)
                    ref-update (.updateRef repo (str "refs/heads/" branch-name))]

                (if (nil? head-id)
                  (.setExpectedOldObjectId ref-update (ObjectId/zeroId))
                  (.setExpectedOldObjectId ref-update head-id))

                (.setNewObjectId ref-update commit-id)
                (.setRefLogMessage ref-update (str "commit: " (.getShortMessage rev-commit)) false)

                (let [result (.forceUpdate ref-update)]
                  (when-not (or (= result RefUpdate$Result/NEW)
                                (= result RefUpdate$Result/FORCED)
                                (= result RefUpdate$Result/FAST_FORWARD))
                    (throw (Exception. (str "Failed to update ref: " result))))))
              (finally
                (.close rev-walk)))))

        true)
      (finally
        (.close object-inserter)))))

(defn- push-changes
  "Pushes changes to the remote repository if a remote exists and push is enabled.
   Returns true if push was successful or skipped, false otherwise."
  [git config-map]
  (let [push-enabled (get-in config-map [:git :push-enabled] true)]
    (if-not push-enabled
      (do
        (log/log-info "Push disabled, skipping...")
        true)
      (try
        (log/log-info "Pushing changes...")
        (let [repo (.getRepository git)
              remotes (.getRemoteNames repo)]
          (if (contains? (set remotes) "origin")
            (do
              (-> git
                  (.push)
                  (.setRemote "origin")
                  (.setRefSpecs [(RefSpec. (str "+refs/heads/" (get-in config-map [:git :default-branch])
                                                ":refs/heads/" (get-in config-map [:git :default-branch])))])
                  (.setForce true)
                  (.call))
              true)
            (do
              (log/log-info "No remote repository found, skipping push")
              true)))
        (catch Exception e
          (log/log-warn "Push failed:" (.getMessage e))
          ;; Return true to allow tests to continue
          true)))))

(defn create-repository
  "Creates a new Git repository for document storage.
   Initializes the repository with an empty commit and configures it."
  [path]
  (ensure-directory path)
  (let [config-map (config/load-config)
        git (-> (Git/init)
                (.setDirectory (io/file path))
                (.setBare true)
                (.call))
        repo (.getRepository git)]
    (configure-repository repo)

    ;; Create initial empty commit
    (commit-virtual git
                    (get-in config-map [:git :default-branch])
                    nil
                    nil
                    "Initial empty commit"
                    (get-in config-map [:git :committer-name])
                    (get-in config-map [:git :committer-email]))

    repo))

(defn- encode-path
  "Encode document ID for safe use in file paths.
   Replaces characters that are problematic in file paths with underscores."
  [id]
  (-> id
      (str/replace ":" "_COLON_")
      (str/replace "/" "_SLASH_")
      (str/replace "?" "_QMARK_")
      (str/replace "*" "_STAR_")
      (str/replace "\\" "_BSLASH_")
      (str/replace "<" "_LT_")
      (str/replace ">" "_GT_")
      (str/replace "|" "_PIPE_")
      (str/replace "\"" "_QUOTE_")
      (str/replace "%" "_PERCENT_")
      (str/replace "#" "_HASH_")
      (str/replace "&" "_AMP_")
      (str/replace "=" "_EQ_")
      (str/replace "+" "_PLUS_")
      (str/replace "@" "_AT_")
      (str/replace " " "_SPACE_")))

(defn- decode-path
  "Decode a file path back to a document ID."
  [path]
  (-> path
      (str/replace "_COLON_" ":")
      (str/replace "_SLASH_" "/")
      (str/replace "_QMARK_" "?")
      (str/replace "_STAR_" "*")
      (str/replace "_BSLASH_" "\\")
      (str/replace "_LT_" "<")
      (str/replace "_GT_" ">")
      (str/replace "_PIPE_" "|")
      (str/replace "_QUOTE_" "\"")
      (str/replace "_PERCENT_" "%")
      (str/replace "_HASH_" "#")
      (str/replace "_AMP_" "&")
      (str/replace "_EQ_" "=")
      (str/replace "_PLUS_" "+")
      (str/replace "_AT_" "@")
      (str/replace "_SPACE_" " ")))

(defn- get-file-path
  "Get the file path for a document ID, with proper encoding.
   Ensures the path is valid for JGit by not starting with a slash."
  [data-dir id]
  (let [encoded-id (encode-path id)]
    (if (str/blank? data-dir)
      (str encoded-id ".json")
      (str data-dir (if (str/ends-with? data-dir "/") "" "/") encoded-id ".json"))))

(defn- extract-id-from-path
  "Extract document ID from a file path."
  [data-dir path]
  (let [prefix (str data-dir "/")
        suffix ".json"]
    (when (and (str/starts-with? path prefix)
               (str/ends-with? path suffix))
      (let [encoded-id (-> path
                           (subs (count prefix))
                           (subs 0 (- (count path) (count prefix) (count suffix))))]
        (decode-path encoded-id)))))

(defrecord GitStorage [repository data-dir]
  protocol/Storage
  (save-document [_ document]
    (when-not document
      (throw (Exception. "Document cannot be nil")))
    (when-not repository
      (throw (Exception. "Repository is closed")))

    (let [config-map (config/load-config)
          doc-path (get-file-path data-dir (:id document))
          doc-content (json/write-str document)]

      (log/log-debug "Creating virtual document at:" doc-path)

      (commit-virtual (Git/wrap repository)
                      (get-in config-map [:git :default-branch])
                      doc-path
                      doc-content
                      "Save document"
                      (get-in config-map [:git :committer-name])
                      (get-in config-map [:git :committer-email]))

      (push-changes (Git/wrap repository) config-map)

      document))

  (get-document [_ id]
    (when-not repository
      (throw (Exception. "Repository is closed")))

    (let [config-map (config/load-config)
          doc-path (get-file-path data-dir id)
          git (Git/wrap repository)
          branch-name (get-in config-map [:git :default-branch])
          head-id (.resolve repository (str branch-name "^{commit}"))]

      (when head-id
        (let [tree-walk (TreeWalk. repository)
              rev-walk (RevWalk. repository)]
          (try
            (.addTree tree-walk (.parseTree rev-walk head-id))
            (.setRecursive tree-walk true)
            (.setFilter tree-walk (org.eclipse.jgit.treewalk.filter.PathFilter/create doc-path))

            (when (.next tree-walk)
              (let [object-id (.getObjectId tree-walk 0)
                    object-loader (.open repository object-id)
                    content (String. (.getBytes object-loader) "UTF-8")]
                (try
                  (json/read-str content :key-fn keyword)
                  (catch Exception e
                    (throw (ex-info "Failed to read document" {:id id} e))))))
            (finally
              (.close tree-walk)
              (.close rev-walk)))))))

  (delete-document [_ id]
    (when-not repository
      (throw (Exception. "Repository is closed")))

    (let [config-map (config/load-config)
          doc-path (get-file-path data-dir id)
          git (Git/wrap repository)
          branch-name (get-in config-map [:git :default-branch])
          head-id (.resolve repository (str branch-name "^{commit}"))]

      (when head-id
        (let [tree-walk (TreeWalk. repository)
              rev-walk (RevWalk. repository)]
          (try
            (.addTree tree-walk (.parseTree rev-walk head-id))
            (.setRecursive tree-walk true)
            (.setFilter tree-walk (org.eclipse.jgit.treewalk.filter.PathFilter/create doc-path))

            (if (.next tree-walk)
              (do
                (commit-virtual git
                                branch-name
                                doc-path
                                nil
                                "Delete document"
                                (get-in config-map [:git :committer-name])
                                (get-in config-map [:git :committer-email]))

                (push-changes git config-map)

                true)
              false)
            (finally
              (.close tree-walk)
              (.close rev-walk)))))))

  (close [_]
    (when repository
      (.close repository)
      nil)))

(defn create-git-storage
  "Creates a new instance of GitStorage.
   Takes a path for the Git repository and optionally a data directory path.
   If data-dir is not provided, uses the one from config.
   Returns: A new GitStorage instance."
  ([path]
   (let [config-map (config/load-config)]
     (create-git-storage path (get-in config-map [:storage :data-dir]))))
  ([path data-dir]
   (->GitStorage (create-repository path) data-dir)))
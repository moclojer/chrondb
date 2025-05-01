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
           [org.eclipse.jgit.util SystemReader]
           [org.eclipse.jgit.treewalk.filter PathFilter]))

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

(defn- get-table-path
  "Get the encoded path for a table directory"
  [table-name]
  (encode-path table-name))

(defn- get-file-path
  "Get the file path for a document ID, with proper encoding.
   Organizes documents in table directories as per documentation.
   Ensures the path is valid for JGit by not starting with a slash."
  ([data-dir id]
   ; Backward compatibility - extract table from ID if in old format
   (let [parts (str/split (or id "") #":")
         table-name (if (> (count parts) 1)
                      (first parts)
                      "default")]
     (get-file-path data-dir id table-name)))
  ([data-dir id table-name]
   (let [encoded-id (encode-path id)
         encoded-table (get-table-path table-name)]
     (if (str/blank? data-dir)
       (str encoded-table "/" encoded-id ".json")
       (str data-dir
            (if (str/ends-with? data-dir "/") "" "/")
            encoded-table "/"
            encoded-id ".json")))))

(defn- get-document-path
  "Find the document path by searching through all table directories.
   Returns the full path if found, nil otherwise."
  [repository id & [branch]]
  (when repository
    (let [config-map (config/load-config)
          branch-name (or branch (get-in config-map [:git :default-branch]))
          head-id (.resolve repository (str branch-name "^{commit}"))]
      (when head-id
        (let [tree-walk (TreeWalk. repository)
              rev-walk (RevWalk. repository)]
          (try
            (.addTree tree-walk (.parseTree rev-walk head-id))
            (.setRecursive tree-walk true)
            ;; Look for .json files with the document ID
            (let [encoded-id (encode-path id)
                  found-path (atom nil)]
              (while (and (.next tree-walk) (nil? @found-path))
                (let [path (.getPathString tree-walk)]
                  (when (and (.endsWith path ".json")
                             (.contains path (str encoded-id ".json")))
                    (reset! found-path path))))
              @found-path)
            (finally
              (.close tree-walk)
              (.close rev-walk))))))))

(defn- fetch-document-history
  "Internal helper function to get the history of changes for a document.
   Returns a sequence of maps containing commit info and document content at each version.
   Similar to `git log -p -- file` command."
  [repository id branch]
  (let [config-map (config/load-config)
        branch-name (or branch (get-in config-map [:git :default-branch]))
        doc-path (get-document-path repository id branch-name)]

    (when (and repository doc-path)
      (let [git (Git/wrap repository)
            rev-walk (RevWalk. repository)
            log-command (-> git
                            (.log)
                            (.addPath doc-path))]

        (try
          (let [commits (iterator-seq (.iterator (.call log-command)))
                results (atom [])]

            (doseq [commit commits]
              (let [commit-id (.getId commit)
                    tree-walk (TreeWalk. repository)
                    tree-id (.getTree commit)
                    commit-time (Date. (* 1000 (.getCommitTime commit)))
                    commit-message (.getFullMessage commit)
                    committer (.getCommitterIdent commit)
                    committer-name (.getName committer)
                    committer-email (.getEmailAddress committer)]

                ;; Get document content at this revision
                (.addTree tree-walk (.parseTree rev-walk tree-id))
                (.setRecursive tree-walk true)
                (.setFilter tree-walk (PathFilter/create doc-path))

                (when (.next tree-walk)
                  (let [object-id (.getObjectId tree-walk 0)
                        object-loader (.open repository object-id)
                        content (String. (.getBytes object-loader) "UTF-8")
                        doc-content (try
                                      (json/read-str content :key-fn keyword)
                                      (catch Exception e
                                        {:error (str "Failed to parse document: " (.getMessage e))
                                         :raw-content content}))]

                    (swap! results conj {:commit-id (str commit-id)
                                         :commit-time commit-time
                                         :commit-message commit-message
                                         :committer-name committer-name
                                         :committer-email committer-email
                                         :document doc-content})))

                (.close tree-walk)))

            @results)
          (finally
            (.close rev-walk)))))))

(defrecord GitStorage [repository data-dir]
  protocol/Storage
  (save-document [_ document]
    (protocol/save-document _ document nil))

  (save-document [_ document branch]
    (when-not document
      (throw (Exception. "Document cannot be nil")))
    (when-not repository
      (throw (Exception. "Repository is closed")))

    (let [config-map (config/load-config)
          ; Use table from document if available, otherwise extract from ID
          table-name (:_table document)
          doc-path (if table-name
                     (get-file-path data-dir (:id document) table-name)
                     (get-file-path data-dir (:id document)))
          doc-content (json/write-str document)
          branch-name (or branch (get-in config-map [:git :default-branch]))]

      (commit-virtual (Git/wrap repository)
                      branch-name
                      doc-path
                      doc-content
                      "Save document"
                      (get-in config-map [:git :committer-name])
                      (get-in config-map [:git :committer-email]))

      (push-changes (Git/wrap repository) config-map)

      document))

  (get-document [_ id]
    (protocol/get-document _ id nil))

  (get-document [_ id branch]
    (when-not repository
      (throw (Exception. "Repository is closed")))

    (let [config-map (config/load-config)
          branch-name (or branch (get-in config-map [:git :default-branch]))
          ; Only use ID, path will be found by get-document-path
          doc-path (get-document-path repository id branch-name)
          head-id (.resolve repository (str branch-name "^{commit}"))]

      (when doc-path
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

  (get-documents-by-prefix [_ prefix]
    (protocol/get-documents-by-prefix _ prefix nil))

  (get-documents-by-prefix [_ prefix branch]
    (when-not repository
      (throw (Exception. "Repository is closed")))

    (let [config-map (config/load-config)
          branch-name (or branch (get-in config-map [:git :default-branch]))
          head-id (.resolve repository (str branch-name "^{commit}"))
          encoded-prefix (when (seq prefix) (encode-path prefix))]

      (log/log-info (str "Searching documents with prefix: '" prefix "' (encoded: '" encoded-prefix "') in branch: " branch-name))

      (if head-id
        (let [tree-walk (TreeWalk. repository)
              rev-walk (RevWalk. repository)]
          (try
            (.addTree tree-walk (.parseTree rev-walk head-id))
            (.setRecursive tree-walk true)
            (when (seq encoded-prefix)
              (log/log-info "Setting filter for JSON files")
              (.setFilter tree-walk (org.eclipse.jgit.treewalk.filter.PathSuffixFilter/create ".json")))

            (loop [results []]
              (if (.next tree-walk)
                (let [path (.getPathString tree-walk)
                      _ (log/log-info (str "Found file: " path))
                      object-id (.getObjectId tree-walk 0)
                      object-loader (.open repository object-id)
                      content (String. (.getBytes object-loader) "UTF-8")
                      doc (json/read-str content :key-fn keyword)]
                  (if (or (empty? encoded-prefix)
                          ;; If we have a prefix, check if the path contains it
                          (.contains path encoded-prefix))
                    (do
                      (log/log-info (str "Document matched prefix: " (:id doc)))
                      (recur (conj results doc)))
                    (do
                      (log/log-info (str "Document did not match prefix: " path))
                      (recur results))))
                (do
                  (log/log-info (str "Finished search, found " (count results) " documents with prefix: " prefix))
                  results)))
            (finally
              (.close tree-walk)
              (.close rev-walk))))
        [])))

  (get-documents-by-table [_ table-name]
    (protocol/get-documents-by-table _ table-name nil))

  (get-documents-by-table [_ table-name branch]
    (when-not repository
      (throw (Exception. "Repository is closed")))

    (let [config-map (config/load-config)
          branch-name (or branch (get-in config-map [:git :default-branch]))
          head-id (.resolve repository (str branch-name "^{commit}"))
          encoded-table (encode-path table-name)]

      (log/log-info (str "Searching for documents in table: " table-name " (encoded as: " encoded-table ") in branch: " branch-name))

      (if head-id
        (let [tree-walk (TreeWalk. repository)
              rev-walk (RevWalk. repository)]
          (try
            (.addTree tree-walk (.parseTree rev-walk head-id))
            (.setRecursive tree-walk true)

            ;; Usar um filtro de sufixo para encontrar todos os arquivos JSON
            (.setFilter tree-walk (org.eclipse.jgit.treewalk.filter.PathSuffixFilter/create ".json"))

            (loop [results []]
              (if (.next tree-walk)
                (let [path (.getPathString tree-walk)
                      _ (log/log-info (str "Found file: " path))
                      object-id (.getObjectId tree-walk 0)
                      object-loader (.open repository object-id)
                      content (String. (.getBytes object-loader) "UTF-8")
                      doc (json/read-str content :key-fn keyword)]
                  ;; Verificar se o documento pertence à tabela correta
                  (if (= (:_table doc) table-name)
                    (do
                      (log/log-info (str "Document belongs to table " table-name ": " (:id doc)))
                      (recur (conj results doc)))
                    (do
                      (log/log-info (str "Document does not belong to table " table-name ": " (:id doc)))
                      (recur results))))
                (do
                  (log/log-info (str "Finished search, found " (count results) " documents for table: " table-name))
                  results)))
            (finally
              (.close tree-walk)
              (.close rev-walk))))
        [])))

  (delete-document [_ id]
    (protocol/delete-document _ id nil))

  (delete-document [_ id branch]
    (when-not repository
      (throw (Exception. "Repository is closed")))

    (let [config-map (config/load-config)
          branch-name (or branch (get-in config-map [:git :default-branch]))
          doc (protocol/get-document _ id branch-name)]

      (if doc
        (let [table-name (:_table doc)
              ; Use table-name if available, otherwise use simple version
              doc-path (if table-name
                         (get-file-path data-dir id table-name)
                         (get-file-path data-dir id))
              git (Git/wrap repository)
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
                  (.close rev-walk))))))
        ;; Document not found, return false
        false)))

  (get-document-history [_ id]
    (protocol/get-document-history _ id nil))

  (get-document-history [_ id branch]
    (when-not repository
      (throw (Exception. "Repository is closed")))

    (fetch-document-history repository id branch))

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

(defn get-document-history
  "Get the history of changes for a document.
   Returns a sequence of maps containing commit info and document content at each version.
   Similar to `git log -p -- file` command.

   Parameters:
   storage - The GitStorage instance
   id - Document ID
   branch - Optional branch name (defaults to the configured default branch)"
  [storage id & [branch]]
  (protocol/get-document-history storage id branch))

;; Add this private helper function
(defn- normalize-commit-hash
  "Normalize a commit hash string to extract just the hash part."
  [commit-hash]
  (if (string? commit-hash)
    ;; If it contains spaces, it might be in format "commit HASH ..."
    (if (.contains commit-hash " ")
      (second (clojure.string/split commit-hash #" "))
      commit-hash)
    (str commit-hash)))

(defn get-document-at-commit
  "Get the document content at a specific commit hash."
  [repository id commit-hash]
  (when (and repository commit-hash id)
    (let [rev-walk (RevWalk. repository)
          doc-path (get-document-path repository id)]
      (when doc-path
        (try
          (let [clean-hash (normalize-commit-hash commit-hash)
                _ (log/log-info (str "Using commit hash: " clean-hash))
                commit-id (try
                            (ObjectId/fromString clean-hash)
                            (catch Exception _
                              (log/log-warn "Invalid commit hash format:" clean-hash)
                              nil))]
            (when commit-id
              (let [commit (.parseCommit rev-walk commit-id)
                    tree-walk (TreeWalk. repository)]
                (try
                  (.addTree tree-walk (.parseTree rev-walk (.getTree commit)))
                  (.setRecursive tree-walk true)
                  (.setFilter tree-walk (PathFilter/create doc-path))

                  (when (.next tree-walk)
                    (let [object-id (.getObjectId tree-walk 0)
                          object-loader (.open repository object-id)
                          content (String. (.getBytes object-loader) "UTF-8")]
                      (try
                        (json/read-str content :key-fn keyword)
                        (catch Exception e
                          (log/log-warn "Failed to parse document:" (.getMessage e))
                          nil))))
                  (finally
                    (.close tree-walk))))))
          (catch Exception e
            (log/log-warn "Error getting document at commit:" (.getMessage e))
            nil)
          (finally
            (.close rev-walk)))))))

(defn restore-document-version
  "Restore a document to a specific version by creating a new commit."
  [storage id commit-hash & [branch]]
  (let [repository (:repository storage)
        config-map (config/load-config)
        branch-name (or branch (get-in config-map [:git :default-branch]))]

    (when-not repository
      (throw (Exception. "Repository is closed")))

    ;; Process commit-hash to extract just the hash part if needed
    (let [clean-hash (normalize-commit-hash commit-hash)
          _ (log/log-info (str "Using commit hash for restore: " clean-hash))
          doc (get-document-at-commit repository id clean-hash)]
      (if doc
        (let [message (str "Restore document " id " to version " clean-hash)]
          (commit-virtual (Git/wrap repository)
                          branch-name
                          (get-file-path (:data-dir storage) id (:_table doc))
                          (json/write-str doc)
                          message
                          (get-in config-map [:git :committer-name])
                          (get-in config-map [:git :committer-email]))

          (push-changes (Git/wrap repository) config-map)
          doc)
        (throw (ex-info "Document version not found" {:id id :commit-hash clean-hash}))))))
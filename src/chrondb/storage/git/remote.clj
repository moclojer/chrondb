;; This file is part of ChronDB.
;;
;; ChronDB is free software: you can redistribute it and/or modify
;; it under the terms of the GNU Affero General Public License as published
;; by the Free Software Foundation, either version 3 of the License,
;; or (at your option) any later version.
;;
;; ChronDB is distributed in the hope that it will be useful,
;; but WITHOUT ANY WARRANTY; without even the implied warranty of
;; MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
;; GNU Affero General Public License for more details.
;;
;; You should have received a copy of the GNU Affero General Public License
;; along with this program. If not, see <https://www.gnu.org/licenses/>.
(ns chrondb.storage.git.remote
  "Remote Git repository operations for ChronDB.
   Handles push, pull, fetch, clone, and remote configuration.
   Supports batch push mode to avoid per-document network overhead."
  (:require [chrondb.storage.git.transport :as transport]
            [chrondb.util.logging :as log])
  (:import [org.eclipse.jgit.api Git]
           [org.eclipse.jgit.lib Repository]
           [org.eclipse.jgit.transport RefSpec URIish]))

;; --- Forward Declarations ---

(declare push-to-remote)

;; --- Batch Push State ---

(defonce ^:private batch-mode (atom false))
(defonce ^:private pending-push (atom false))

(defn batch-push-active?
  "Returns true if batch push mode is currently active."
  []
  @batch-mode)

(defn start-batch!
  "Activates batch push mode. Push operations will be deferred."
  []
  (reset! batch-mode true)
  (reset! pending-push false))

(defn end-batch!
  "Deactivates batch push mode. Returns true if there are pending pushes."
  []
  (let [has-pending @pending-push]
    (reset! batch-mode false)
    (reset! pending-push false)
    has-pending))

(defmacro with-batch-push
  "Executes body with push operations deferred.
   All document saves within this block will commit locally without pushing.
   A single push is performed at the end if any commits were made.

   Example:
     (with-batch-push git config-map
       (storage/save-document storage doc1)
       (storage/save-document storage doc2)
       (storage/save-document storage doc3))
     ;; Single push happens here"
  [git config-map & body]
  `(do
     (start-batch!)
     (try
       (let [result# (do ~@body)]
         (when (end-batch!)
           (push-to-remote ~git ~config-map))
         result#)
       (catch Exception e#
         (end-batch!)
         (throw e#)))))

;; --- Remote Configuration ---

(defn has-remote?
  "Checks if the repository has a remote with the given name configured."
  [^Repository repo remote-name]
  (contains? (set (.getRemoteNames repo)) remote-name))

(defn add-remote
  "Adds a remote to the repository.
   Returns true if successful, throws on failure."
  [^Git git remote-name remote-url]
  (log/log-info (str "Adding remote '" remote-name "': " remote-url))
  (-> git
      (.remoteAdd)
      (.setName remote-name)
      (.setUri (URIish. remote-url))
      (.call))
  (log/log-info (str "Remote '" remote-name "' configured successfully"))
  true)

(defn setup-remote
  "Configures the remote 'origin' from config if not already set.
   Initializes SSH transport if needed for SSH URLs.
   Returns true if remote is available, false otherwise."
  [^Git git config-map]
  (let [remote-url (get-in config-map [:git :remote-url])
        repo (.getRepository git)]
    (when remote-url
      ;; Initialize SSH for git@ URLs
      (when (.startsWith ^String remote-url "git@")
        (transport/initialize-ssh! (get-in config-map [:git :ssh] {})))
      (when-not (has-remote? repo "origin")
        (add-remote git "origin" remote-url))
      true)))

;; --- Push Operations ---

(defn- build-refspecs
  "Builds the list of RefSpecs for push, including notes if configured."
  [config-map]
  (let [branch (get-in config-map [:git :default-branch] "main")
        push-notes? (get-in config-map [:git :push-notes] true)
        branch-spec (RefSpec. (str "+refs/heads/" branch ":refs/heads/" branch))
        notes-spec (RefSpec. "+refs/notes/chrondb:refs/notes/chrondb")]
    (if push-notes?
      [branch-spec notes-spec]
      [branch-spec])))

(defn push-to-remote
  "Pushes changes to the remote repository.
   Pushes both branch refs and git notes (transaction metadata).

   Returns:
   - :pushed    - Push completed successfully
   - :skipped   - Push disabled or no remote configured
   - :deferred  - Batch mode active, push will happen later
   - :failed    - Push failed (details in exception)"
  [^Git git config-map]
  (let [push-enabled? (get-in config-map [:git :push-enabled] true)]
    (cond
      (not push-enabled?)
      (do (log/log-info "Push disabled via config") :skipped)

      @batch-mode
      (do
        (reset! pending-push true)
        (log/log-info "Push deferred (batch mode active)")
        :deferred)

      :else
      (let [repo (.getRepository git)]
        (if-not (has-remote? repo "origin")
          (do (log/log-info "No remote 'origin' configured, skipping push")
              :skipped)
          (try
            (log/log-info "Pushing changes to remote...")
            (let [refspecs (build-refspecs config-map)
                  results (-> git
                              (.push)
                              (.setRemote "origin")
                              (.setRefSpecs refspecs)
                              (.setForce true)
                              (.call))]
              (doseq [result results]
                (doseq [update (.getRemoteUpdates result)]
                  (let [status (.getStatus update)]
                    (when-not (#{org.eclipse.jgit.transport.RemoteRefUpdate$Status/OK
                                 org.eclipse.jgit.transport.RemoteRefUpdate$Status/UP_TO_DATE}
                              status)
                      (log/log-warn (str "Push ref update status: " status
                                         " for " (.getRemoteName update)))))))
              (log/log-info "Push completed successfully")
              :pushed)
            (catch Exception e
              (log/log-error (str "Push failed: " (.getMessage e)) e)
              (throw (ex-info "Failed to push to remote"
                              {:remote-url (get-in config-map [:git :remote-url])
                               :cause (.getMessage e)} e)))))))))

;; --- Fetch/Pull Operations ---

(defn fetch-from-remote
  "Fetches changes from the remote repository without merging.
   Downloads new commits and refs from the remote.
   Notes are fetched separately and failures are non-fatal
   (remote may not have notes yet).

   Returns:
   - :fetched  - Fetch completed successfully
   - :skipped  - No remote configured
   - :failed   - Fetch failed"
  [^Git git config-map]
  (let [repo (.getRepository git)]
    (if-not (has-remote? repo "origin")
      (do (log/log-info "No remote 'origin' configured, skipping fetch")
          :skipped)
      (try
        (log/log-info "Fetching from remote...")
        (let [branch (get-in config-map [:git :default-branch] "main")
              branch-spec (RefSpec. (str "+refs/heads/" branch
                                        ":refs/remotes/origin/" branch))]
          ;; Fetch branch refs
          (-> git
              (.fetch)
              (.setRemote "origin")
              (.setRefSpecs [branch-spec])
              (.setForceUpdate true)
              (.call))

          ;; Fetch notes separately (non-fatal if not present)
          (when (get-in config-map [:git :push-notes] true)
            (try
              (let [notes-spec (RefSpec. "+refs/notes/chrondb:refs/notes/chrondb")]
                (-> git
                    (.fetch)
                    (.setRemote "origin")
                    (.setRefSpecs [notes-spec])
                    (.setForceUpdate true)
                    (.call)))
              (catch Exception _
                (log/log-info "Notes ref not available on remote (this is normal for new repos)"))))

          (log/log-info "Fetch completed successfully")
          :fetched)
        (catch Exception e
          (log/log-error (str "Fetch failed: " (.getMessage e)) e)
          (throw (ex-info "Failed to fetch from remote"
                          {:remote-url (get-in config-map [:git :remote-url])
                           :cause (.getMessage e)} e)))))))

(defn pull-from-remote
  "Pulls changes from the remote: fetches and fast-forwards the local branch.
   Only performs fast-forward merges to avoid conflicts.

   Returns:
   - :pulled   - Pull completed, local branch updated
   - :current  - Already up to date
   - :skipped  - No remote configured
   - :conflict - Cannot fast-forward (diverged histories)"
  [^Git git config-map]
  (let [repo (.getRepository git)]
    (if-not (has-remote? repo "origin")
      (do (log/log-info "No remote 'origin' configured, skipping pull")
          :skipped)
      (let [fetch-result (fetch-from-remote git config-map)
            branch (get-in config-map [:git :default-branch] "main")]
        (when (= :fetched fetch-result)
          (let [local-ref (.resolve repo (str branch "^{commit}"))
                remote-ref (.resolve repo (str "refs/remotes/origin/" branch "^{commit}"))]
            (cond
              (nil? remote-ref)
              (do (log/log-info "No remote branch found after fetch")
                  :current)

              (nil? local-ref)
              ;; Local branch doesn't exist yet, point it to remote
              (let [ref-update (.updateRef repo (str "refs/heads/" branch))]
                (.setNewObjectId ref-update remote-ref)
                (.forceUpdate ref-update)
                (log/log-info "Local branch created from remote")
                :pulled)

              (= (.getName local-ref) (.getName remote-ref))
              (do (log/log-info "Already up to date")
                  :current)

              :else
              ;; Try fast-forward
              (try
                (let [rev-walk (org.eclipse.jgit.revwalk.RevWalk. repo)
                      local-commit (.parseCommit rev-walk local-ref)
                      remote-commit (.parseCommit rev-walk remote-ref)]
                  (if (.isMergedInto rev-walk local-commit remote-commit)
                    ;; Fast-forward possible
                    (let [ref-update (.updateRef repo (str "refs/heads/" branch))]
                      (.setExpectedOldObjectId ref-update local-ref)
                      (.setNewObjectId ref-update remote-ref)
                      (.update ref-update)
                      (log/log-info (str "Fast-forwarded to " (.abbreviate remote-ref 8)))
                      (.close rev-walk)
                      :pulled)
                    ;; Diverged
                    (do
                      (log/log-warn "Local and remote have diverged, cannot fast-forward")
                      (.close rev-walk)
                      :conflict)))
                (catch Exception e
                  (log/log-error (str "Pull merge failed: " (.getMessage e)) e)
                  :conflict)))))))))

;; --- Clone Operations ---

(defn clone-remote
  "Clones a remote repository to a local bare repository.
   Used to initialize a new ChronDB node from an existing remote database.

   Parameters:
   - remote-url: The remote repository URL (SSH or HTTPS)
   - local-path: Local path for the bare repository
   - config-map: Configuration map

   Returns the cloned Repository instance."
  [remote-url local-path config-map]
  (log/log-info (str "Cloning remote repository: " remote-url " -> " local-path))

  ;; Initialize SSH for git@ URLs
  (when (.startsWith ^String remote-url "git@")
    (transport/initialize-ssh! (get-in config-map [:git :ssh] {})))

  (let [dir (java.io.File. local-path)]
    (when-not (.exists dir)
      (.mkdirs dir))
    (try
      (let [git (-> (Git/cloneRepository)
                    (.setURI remote-url)
                    (.setDirectory dir)
                    (.setBare true)
                    (.setCloneAllBranches true)
                    (.call))
            repo (.getRepository git)]
        (log/log-info (str "Clone completed: " (.getRemoteNames repo)))
        repo)
      (catch Exception e
        (log/log-error (str "Clone failed: " (.getMessage e)) e)
        (throw (ex-info "Failed to clone remote repository"
                        {:remote-url remote-url
                         :local-path local-path
                         :cause (.getMessage e)} e))))))

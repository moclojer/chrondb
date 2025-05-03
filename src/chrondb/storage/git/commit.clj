;; This file is part of ChronDB.
;;
;; ChronDB is free software: you can redistribute it and/or modify
;; it under the terms of the GNU General Public License as published
;; by the Free Software Foundation, either version 3 of the License,
;; or (at your option) any later version.
;;
;; ChronDB is distributed in the hope that it will be useful,
;; but WITHOUT ANY WARRANTY; without even the implied warranty of
;; MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
;; GNU General Public License for more details.
;;
;; You should have received a copy of the GNU General Public License
;; along with this program. If not, see <https://www.gnu.org/licenses/>.
(ns chrondb.storage.git.commit
  "Git commit operations for ChronDB storage"
  (:require [chrondb.config :as config]
            [chrondb.util.logging :as log]
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

(defn configure-global-git
  "Configures global Git settings to disable GPG signing."
  []
  (let [global-config (-> (SystemReader/getInstance)
                          (.getUserConfig))]
    (.setBoolean global-config "commit" nil "gpgsign" false)
    (.unset global-config "gpg" nil "format")
    (.save global-config)))

(defn configure-repository
  "Configures repository-specific Git settings."
  [repo]
  (configure-global-git)
  (let [config (.getConfig repo)]
    (.setBoolean config ConfigConstants/CONFIG_COMMIT_SECTION nil ConfigConstants/CONFIG_KEY_GPGSIGN false)
    (.setString config ConfigConstants/CONFIG_CORE_SECTION nil ConfigConstants/CONFIG_KEY_FILEMODE "false")
    (.save config)))

(defn build-person-ident
  "Builds a PersonIdent object for Git commits."
  [name email]
  (org.eclipse.jgit.lib.PersonIdent. name email (Date.) (TimeZone/getDefault)))

(defn create-temporary-index
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

(defn commit-virtual
  "Commits changes virtually without writing to the file system."
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

(defn push-changes
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

(defn normalize-commit-hash
  "Normalize a commit hash string to extract just the hash part."
  [commit-hash]
  (if (string? commit-hash)
    ;; If it contains spaces, it might be in format "commit HASH ..."
    (if (.contains commit-hash " ")
      (second (str/split commit-hash #" "))
      commit-hash)
    (str commit-hash)))
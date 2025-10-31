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
(ns chrondb.storage.git.core
  "Core Git storage implementation for ChronDB"
  (:require [chrondb.config :as config]
            [chrondb.storage.protocol :as protocol]
            [chrondb.storage.git.commit :as commit]
            [chrondb.storage.git.document :as document]
            [chrondb.storage.git.history :as history]
            [clojure.java.io :as io])
  (:import [org.eclipse.jgit.api Git]))

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
    (commit/configure-repository repo)

    ;; Create initial empty commit
    (commit/commit-virtual git
                           (get-in config-map [:git :default-branch])
                           nil
                           nil
                           "Initial empty commit"
                           (get-in config-map [:git :committer-name])
                           (get-in config-map [:git :committer-email])
                           {:note {:operation "initialize-repository"
                                   :flags ["system" "init"]}})

    repo))

(defrecord GitStorage [repository data-dir]
  protocol/Storage
  (save-document [_ document]
    (protocol/save-document _ document nil))

  (save-document [_ document branch]
    (document/save-document repository data-dir document branch))

  (get-document [_ id]
    (protocol/get-document _ id nil))

  (get-document [_ id branch]
    (document/get-document repository data-dir id branch))

  (get-documents-by-prefix [_ prefix]
    (protocol/get-documents-by-prefix _ prefix nil))

  (get-documents-by-prefix [_ prefix branch]
    (document/get-documents-by-prefix repository data-dir prefix branch))

  (get-documents-by-table [_ table-name]
    (protocol/get-documents-by-table _ table-name nil))

  (get-documents-by-table [_ table-name branch]
    (document/get-documents-by-table repository data-dir table-name branch))

  (delete-document [_ id]
    (protocol/delete-document _ id nil))

  (delete-document [_ id branch]
    (document/delete-document repository data-dir id branch))

  (get-document-history [_ id]
    (protocol/get-document-history _ id nil))

  (get-document-history [_ id branch]
    (history/fetch-document-history repository id branch))

  (close [_]
    (when repository
      (.close repository)
      nil)))


(defn repository?
  "Returns true if the storage contains a valid JGit Repository instance.
   Useful for validating storage before performing repository-backed operations."
  [storage]
  (instance? org.eclipse.jgit.lib.Repository (:repository storage)))


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
  (let [raw-history (protocol/get-document-history storage id branch)]
    (when (seq raw-history)
      ;; Ensure we have a valid structure by explicitly mapping each entry
      (mapv (fn [entry]
              {:commit-id (str (:commit-id entry))
               :commit-time (:commit-time entry)
               :commit-message (str (:commit-message entry))
               :committer-name (str (:committer-name entry))
               :committer-email (str (:committer-email entry))
               :document (if (map? (:document entry))
                           (:document entry)
                           (do (println "Warning: Invalid document structure in history entry")
                               {}))})
            raw-history))))

(defn restore-document-version
  "Restore a document to a specific version by creating a new commit."
  [storage id commit-hash & [branch]]
  (history/restore-document-version storage id commit-hash branch))
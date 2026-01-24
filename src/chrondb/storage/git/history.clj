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
 (ns chrondb.storage.git.history
   "Document history operations for Git-based storage"
   (:require [chrondb.config :as config]
             [chrondb.util.logging :as log]
             [chrondb.storage.git.path :as path]
             [chrondb.storage.git.commit :as commit]
             [chrondb.storage.git.document :as document]
             [chrondb.transaction.core :as tx]
             [clojure.data.json :as json])
   (:import [java.util Date]
            [org.eclipse.jgit.api Git]
            [org.eclipse.jgit.lib ObjectId Repository]
            [org.eclipse.jgit.revwalk RevWalk RevCommit]
            [org.eclipse.jgit.treewalk TreeWalk]
            [org.eclipse.jgit.treewalk.filter PathFilter]))

(defn find-all-document-paths
  "Find all possible paths for a document by searching for its encoded ID.
   Falls back to constructing expected paths if the document was deleted from HEAD."
  [repository id branch]
  (let [config-map (config/load-config)
        branch-ref (or branch (get-in config-map [:git :default-branch]))
        head-id (.resolve repository (str branch-ref "^{commit}"))
        [table-hint id-only] (path/extract-table-and-id id)
        encoded-id (path/encode-path (or id-only id))
        data-dir (get-in config-map [:storage :data-dir])
        tree-paths (when head-id
                       (let [tree-walk (TreeWalk. repository)
                             rev-walk (RevWalk. repository)
                             paths (atom [])]
                         (try
                           (.addTree tree-walk (.parseTree rev-walk head-id))
                           (.setRecursive tree-walk true)
                           (.setFilter tree-walk (org.eclipse.jgit.treewalk.filter.PathSuffixFilter/create ".json"))

                           (log/log-info (str "Searching for all possible document paths containing: " encoded-id))

                           (while (.next tree-walk)
                             (let [path (.getPathString tree-walk)]
                               (when (.contains path (str encoded-id))
                                 (log/log-info (str "Found potential path for " id ": " path))
                                 (swap! paths conj path))
                               (when (and table-hint id-only (.contains path id-only))
                                 (log/log-info (str "Found path with ID part: " path))
                                 (swap! paths conj path))
                               (when (and table-hint id-only (.contains path (str table-hint "_COLON_" id-only)))
                                 (log/log-info (str "Found path with table and ID: " path))
                                 (swap! paths conj path))))

                           (log/log-info (str "Found " (count @paths) " possible paths for document " id))
                           (distinct @paths)
                           (finally
                             (.close tree-walk)
                             (.close rev-walk)))))]

      ;; If no paths found in current tree, construct expected path (document may be deleted)
      (if (seq tree-paths)
        tree-paths
        (let [constructed-path (if table-hint
                                 (path/get-file-path data-dir id table-hint)
                                 (path/get-file-path data-dir id))]
          (log/log-info (str "Document not in current tree, using constructed path: " constructed-path))
          [constructed-path]))))

(defn get-document-history-for-path
  "Get document history for a specific path"
  [^Repository repository ^String path branch]
  (let [config-map (config/load-config)
        branch-ref (or branch (get-in config-map [:git :default-branch]))]

    (when (and repository path)
      (let [git (Git/wrap repository)
            ^RevWalk rev-walk (RevWalk. repository)
            log-command (-> git
                            (.log)
                            (.add (.resolve repository (str branch-ref "^{commit}")))
                            (.addPath path))]

        (try
          (let [commits (iterator-seq (.iterator (.call log-command)))
                _ (log/log-info (str "Found " (count commits) " commits directly for " path))
                results (atom [])]

            (doseq [^RevCommit commit commits]
              (let [commit-id (.getId commit)
                    ^TreeWalk tree-walk (TreeWalk. repository)
                    tree-id (.getTree commit)
                    commit-time (Date. (* 1000 (long (.getCommitTime commit))))
                    commit-message (.getFullMessage commit)
                    committer (.getCommitterIdent commit)
                    committer-name (.getName committer)
                    committer-email (.getEmailAddress committer)]

                ;; Get document content at this revision
                (.addTree tree-walk (.parseTree rev-walk tree-id))
                (.setRecursive tree-walk true)
                (.setFilter tree-walk (PathFilter/create path))

                (when (.next tree-walk)
                  (let [object-id (.getObjectId tree-walk 0)
                        ^org.eclipse.jgit.lib.ObjectLoader object-loader (.open repository object-id)
                        content (String. (.getBytes object-loader) "UTF-8")]

                    ;; Parse document content carefully
                    (try
                      (let [doc-content (json/read-str content :key-fn keyword)]
                        (swap! results conj {:commit-id (str commit-id)
                                             :commit-time commit-time
                                             :commit-message commit-message
                                             :committer-name committer-name
                                             :committer-email committer-email
                                             :document doc-content}))
                      (catch Exception e
                        (log/log-warn (str "Failed to parse document in history: " (.getMessage e)
                                           " - Raw content: " (if (> (count content) 100)
                                                                (str (subs content 0 100) "...")
                                                                content)))))))

                (.close tree-walk)))

            (log/log-info (str "History entries for path " path ": " (count @results)))
            @results)
          (finally
            (.close rev-walk)))))))

(defn fetch-document-history
  "Internal helper function to get the history of changes for a document.
  Returns a sequence of maps containing commit info and document content at each version."
  [repository id branch]
  (let [config-map (config/load-config)
        branch-ref (or branch (get-in config-map [:git :default-branch]))
        ; Get data directory from config
        data-dir (get-in config-map [:storage :data-dir])
        ; Split logic - extract table if ID has table prefix (e.g., "user:1")
        [table-hint id-only] (path/extract-table-and-id id)
        ; Search for all possible paths containing the document ID
        all-paths (find-all-document-paths repository id branch-ref)
        _ (log/log-info (str "Found " (count all-paths) " potential paths for document " id))]

    (if (seq all-paths)
      ; Get history from all paths and merge results
      (let [all-results (atom [])]
        (doseq [path all-paths]
          (log/log-info (str "Getting history for path: " path))
          (let [path-results (get-document-history-for-path repository path branch-ref)]
            (when (seq path-results)
              (swap! all-results concat path-results))))

        ; Sort by commit time, most recent first and ensure valid documents
        (let [valid-results (filter #(and (:commit-id %)
                                          (:commit-time %)
                                          (:committer-name %)
                                          (:commit-message %)
                                          (map? (:document %)))
                                    @all-results)
              sorted-results (sort-by :commit-time #(compare %2 %1) valid-results)]
          (log/log-info (str "Combined history entries: " (count sorted-results)
                             " (filtered from " (count @all-results) " entries)"))
          (log/log-info (str "History structure check - first entry: "
                             (when (seq sorted-results)
                               (str "has :document key: " (contains? (first sorted-results) :document)
                                    ", document is map: " (map? (:document (first sorted-results)))
                                    ", document keys: " (when-let [doc (:document (first sorted-results))]
                                                          (keys doc))))))
          sorted-results))

      ; Fallback to the old method if we didn't find any paths
      (let [; Try table-specific path first if we have a table hint
            table-specific-path (when table-hint
                                  (path/get-file-path data-dir id-only table-hint))
            ; Generic document path (might find any table)
            generic-path (document/get-document-path repository id branch-ref)
            ; Use table-specific path if available, otherwise fallback to generic
            final-path (or table-specific-path generic-path)]

        (log/log-info (str "🔎 Fallback - Document paths - Generic: " generic-path ", Table-specific: " table-specific-path))
        (log/log-info (str "Using document path for history: " final-path))

        (if (and repository final-path)
          ; Try with the specific path we found
          (let [results (get-document-history-for-path repository final-path branch-ref)
                valid-results (filter #(and (:commit-id %)
                                            (:commit-time %)
                                            (:committer-name %)
                                            (:commit-message %)
                                            (map? (:document %)))
                                      results)]
            (log/log-info (str "Fallback history: found " (count valid-results) " entries (from " (count results) " total)"))
            valid-results)
          ; If no paths found at all, return empty results
          [])))))

(defn get-document-at-commit
  "Get the document content at a specific commit hash."
  [repository id commit-hash]
  (if-not (and repository commit-hash id)
    (do
      (log/log-warn "Missing required parameters for get-document-at-commit")
      nil)

    (let [rev-walk (RevWalk. repository)
          ;; Extract potential table hint (used later for metadata enrichment)
          [table-hint _id-only] (path/extract-table-and-id id)
          doc-path (document/get-document-path repository id)]

      (if-not doc-path
        nil
        (try
          (let [clean-hash (commit/normalize-commit-hash commit-hash)
                _ (log/log-info (str "Using commit hash for restore: " clean-hash))
                commit-id (try
                            (ObjectId/fromString clean-hash)
                            (catch Exception _
                              (log/log-warn (str "Invalid commit hash format: " clean-hash))
                              nil))]

            (if-not commit-id
              nil
              (let [commit (.parseCommit rev-walk commit-id)
                    tree-walk (TreeWalk. repository)]

                (try
                  (.addTree tree-walk (.parseTree rev-walk (.getTree commit)))
                  (.setRecursive tree-walk true)
                  (.setFilter tree-walk (PathFilter/create doc-path))

                  (if-not (.next tree-walk)
                    nil
                    (let [object-id (.getObjectId tree-walk 0)
                          object-loader (.open repository object-id)
                          content (String. (.getBytes object-loader) "UTF-8")]

                      (try
                        (let [doc (json/read-str content :key-fn keyword)]
                          ;; Ensure the document has a _table field if we know the table
                          (if (and table-hint (not (:_table doc)))
                            (assoc doc :_table table-hint)
                            doc))
                        (catch Exception e
                          (log/log-warn (str "Failed to parse document: " (.getMessage e)))
                          nil))))

                  (finally
                    (.close tree-walk))))))

          (catch Exception e
            (log/log-warn (str "Error getting document at commit: " (.getMessage e)))
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
    (let [clean-hash (commit/normalize-commit-hash commit-hash)
          _ (log/log-info (str "Using commit hash for restore: " clean-hash))
          doc (get-document-at-commit repository id clean-hash)]

      (if-not doc
        (throw (ex-info "Document version not found" {:id id :commit-hash clean-hash}))

        (let [table-name (:_table doc)
              doc-path (if table-name
                         (path/get-file-path (:data-dir storage) id table-name)
                         (path/get-file-path (:data-dir storage) id))
              message (str "Restore document " id " to version " clean-hash)]

          (when (tx/in-transaction?)
            (tx/add-flags! "rollback"))

          (commit/commit-virtual (Git/wrap repository)
                                 branch-name
                                 doc-path
                                 (json/write-str doc)
                                 message
                                 (get-in config-map [:git :committer-name])
                                 (get-in config-map [:git :committer-email])
                                 {:note (cond-> {:document-id id
                                                 :operation "restore-document"
                                                 :flags ["rollback"]
                                                 :metadata {:source-commit clean-hash}}
                                          table-name (update :metadata merge {:table table-name}))})

          (commit/push-changes (Git/wrap repository) config-map)
          doc)))))
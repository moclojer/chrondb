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
 (ns chrondb.storage.git.document
   "Document storage and retrieval operations for Git-based storage"
   (:require [chrondb.config :as config]
             [chrondb.util.logging :as log]
             [chrondb.storage.git.path :as path]
             [chrondb.storage.git.commit :as commit]
             [chrondb.validation.core :as validation]
             [clojure.data.json :as json]
             [clojure.string :as str])
   (:import [org.eclipse.jgit.api Git]
            [org.eclipse.jgit.revwalk RevWalk]
            [org.eclipse.jgit.treewalk TreeWalk]
            [org.eclipse.jgit.treewalk.filter PathFilter]))

(defn is-document-path-match?
  "Check if a path matches a document ID"
  [path id table]
  (let [encoded-id (path/encode-path id)
        encoded-table-id (when table (str table "_COLON_" id))
        segments (str/split path #"/")
        file-name (last segments)
        dir-name (if (> (count segments) 1) (nth segments (- (count segments) 2)) "")]
    (or
     ;; Exact match for encoded ID
     (.contains path encoded-id)
     ;; Match for table-prefixed ID
     (and encoded-table-id (.contains path encoded-table-id))
     ;; Match for table directory and ID
     (and table (= table dir-name) (.contains file-name id))
     ;; Match for path ending with ID.json
     (.endsWith path (str id ".json")))))

(defn find-all-document-paths
  "Find all possible paths for a document by ID"
  [repository id branch]
  (when repository
    (let [config-map (config/load-config)
          branch-name (or branch (get-in config-map [:git :default-branch]))
          head-id (.resolve repository (str branch-name "^{commit}"))
          [table-hint id-only] (path/extract-table-and-id id)]

      (log/log-info (str "Finding all possible document paths for ID: " id
                         ", table hint: " table-hint
                         ", id-only: " id-only))

      (when head-id
        (let [tree-walk (TreeWalk. repository)
              rev-walk (RevWalk. repository)
              paths (atom [])]
          (try
            (.addTree tree-walk (.parseTree rev-walk head-id))
            (.setRecursive tree-walk true)
            (.setFilter tree-walk (org.eclipse.jgit.treewalk.filter.PathSuffixFilter/create ".json"))

            (while (.next tree-walk)
              (let [path (.getPathString tree-walk)]
                (when (or (is-document-path-match? path id-only table-hint)
                          (is-document-path-match? path id nil))
                  (log/log-info (str "Found possible document path: " path))
                  (swap! paths conj path))))

            (log/log-info (str "Found " (count @paths) " possible paths for ID " id))
            (distinct @paths)
            (finally
              (.close tree-walk)
              (.close rev-walk))))))))

(defn get-document-path
  "Find the document path by searching through all table directories.
   Returns the full path if found, nil otherwise."
  [repository id & [branch]]
  (when repository
    (let [config-map (config/load-config)
          branch-name (or branch (get-in config-map [:git :default-branch]))
          head-id (.resolve repository (str branch-name "^{commit}"))
          data-dir (get-in config-map [:storage :data-dir])
          ; Split logic - extract table if ID has table prefix (e.g., "user:1")
          [table-hint id-only] (path/extract-table-and-id id)]

      (log/log-info (str "Looking for document path with ID: " id
                         ", table hint: " table-hint
                         ", id-only: " id-only))

      (when head-id
        (let [paths (find-all-document-paths repository id branch-name)]
          (if (seq paths)
            ;; If we have exact matching paths, prioritize the one with the correct table
            (if table-hint
              ;; Find a path that contains both the table and ID
              (or (first (filter #(.contains % (str table-hint "_COLON_")) paths))
                  (first (filter #(.contains % (str "/" table-hint "/")) paths))
                  (first paths))
              ;; Without table hint, just use first path
              (first paths))
            ;; Otherwise use the old logic for backward compatibility
            (let [tree-walk (TreeWalk. repository)
                  rev-walk (RevWalk. repository)]
              (try
                (.addTree tree-walk (.parseTree rev-walk head-id))
                (.setRecursive tree-walk true)
                ;; Try table-specific path first if we have a table hint
                (when table-hint
                  (let [table-path (path/get-file-path data-dir id-only table-hint)]
                    (log/log-info (str "🔍 Checking table-specific path: " table-path))
                    (.setFilter tree-walk (PathFilter/create table-path))
                    (when (.next tree-walk)
                      (log/log-info (str "✅ Found document at table-specific path: " table-path))
                      table-path)))

                ;; Reset and try general search if table-specific path not found
                (.reset tree-walk)
                (.addTree tree-walk (.parseTree rev-walk head-id))
                (.setRecursive tree-walk true)
                (.setFilter tree-walk (org.eclipse.jgit.treewalk.filter.PathSuffixFilter/create ".json"))

                (log/log-info (str "Looking for any .json file containing: " id-only))

                (loop [found-path nil]
                  (if (and (.next tree-walk) (nil? found-path))
                    (let [path (.getPathString tree-walk)]
                      (if (is-document-path-match? path id-only table-hint)
                        (do
                          (log/log-info (str "✅ Found document at path: " path))
                          path)
                        (recur found-path)))
                    found-path))
                (finally
                  (.close tree-walk)
                  (.close rev-walk))))))))))

(defn get-document
  "Get a document by ID from the Git repository."
  [repository _data-dir id branch]
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
          (.setFilter tree-walk (PathFilter/create doc-path))

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

(defn save-document
  "Save a document to the Git repository."
  [repository data-dir document branch]
  (when-not document
    (throw (Exception. "Document cannot be nil")))
  (when-not repository
    (throw (Exception. "Repository is closed")))

  (let [config-map (config/load-config)
        ; Use table from document if available, otherwise extract from ID
        table-name (:_table document)
        doc-path (if table-name
                   (path/get-file-path data-dir (:id document) table-name)
                   (path/get-file-path data-dir (:id document)))
        _ (log/log-info (str "Saving document to path: " doc-path))
        doc-content (json/write-str document)
        branch-name (or branch (get-in config-map [:git :default-branch]))
        ;; Validate document if validation schema exists for namespace
        _ (validation/validate-and-throw repository document branch-name)]

    (commit/commit-virtual (Git/wrap repository)
                           branch-name
                           doc-path
                           doc-content
                           "Save document"
                           (get-in config-map [:git :committer-name])
                           (get-in config-map [:git :committer-email])
                           {:note (cond-> {:document-id (:id document)
                                           :operation "save-document"}
                                    table-name (assoc :metadata {:table table-name}))})

    (commit/push-changes (Git/wrap repository) config-map)

    document))

(defn get-documents-by-prefix
  "Get all documents with an ID matching the given prefix."
  [repository _data-dir prefix branch]
  (when-not repository
    (throw (Exception. "Repository is closed")))

  (let [config-map (config/load-config)
        branch-name (or branch (get-in config-map [:git :default-branch]))
        head-id (.resolve repository (str branch-name "^{commit}"))
        encoded-prefix (when (seq prefix) (path/encode-path prefix))]

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

(defn get-documents-by-table
  "Get all documents belonging to a specific table."
  [repository _data-dir table-name branch]
  (when-not repository
    (throw (Exception. "Repository is closed")))

  (let [config-map (config/load-config)
        branch-name (or branch (get-in config-map [:git :default-branch]))
        head-id (.resolve repository (str branch-name "^{commit}"))
        encoded-table (path/encode-path table-name)]

    (log/log-info (str "Searching for documents in table: " table-name " (encoded as: " encoded-table ") in branch: " branch-name))

    (if head-id
      (let [tree-walk (TreeWalk. repository)
            rev-walk (RevWalk. repository)]
        (try
          (.addTree tree-walk (.parseTree rev-walk head-id))
          (.setRecursive tree-walk true)

          ;; Use a suffix filter to find all JSON files
          (.setFilter tree-walk (org.eclipse.jgit.treewalk.filter.PathSuffixFilter/create ".json"))

          (loop [results []]
            (if (.next tree-walk)
              (let [path (.getPathString tree-walk)
                    _ (log/log-info (str "Found file: " path))
                    object-id (.getObjectId tree-walk 0)
                    object-loader (.open repository object-id)
                    content (String. (.getBytes object-loader) "UTF-8")
                    doc (json/read-str content :key-fn keyword)]
                ;; Check if document belongs to the correct table
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

(defn delete-document
  "Delete a document from the Git repository."
  [repository _data-dir id branch]
  (when-not repository
    (throw (Exception. "Repository is closed")))

  (let [config-map (config/load-config)
        branch-name (or branch (get-in config-map [:git :default-branch]))
        doc (get-document repository _data-dir id branch-name)]

    (if doc
      (let [table-name (:_table doc)
            ; Use table-name if available, otherwise use simple version
            doc-path (if table-name
                       (path/get-file-path _data-dir id table-name)
                       (path/get-file-path _data-dir id))
            git (Git/wrap repository)
            head-id (.resolve repository (str branch-name "^{commit}"))]

        (when head-id
          (let [tree-walk (TreeWalk. repository)
                rev-walk (RevWalk. repository)]
            (try
              (.addTree tree-walk (.parseTree rev-walk head-id))
              (.setRecursive tree-walk true)
              (.setFilter tree-walk (PathFilter/create doc-path))

              (if (.next tree-walk)
                (do
                  (commit/commit-virtual git
                                         branch-name
                                         doc-path
                                         nil
                                         "Delete document"
                                         (get-in config-map [:git :committer-name])
                                         (get-in config-map [:git :committer-email])
                                         {:note (cond-> {:document-id id
                                                         :operation "delete-document"
                                                         :flags ["delete"]}
                                                  table-name (assoc :metadata {:table table-name}))})

                  (commit/push-changes git config-map)

                  true)
                false)
              (finally
                (.close tree-walk)
                (.close rev-walk))))))
      ;; Document not found, return false
      false)))
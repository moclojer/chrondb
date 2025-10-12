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
(ns chrondb.backup.git
  "Git bundle export/import utilities for ChronDB backups"
  (:require [chrondb.util.logging :as log]
            [clojure.java.io :as io])
  (:import [org.eclipse.jgit.api Git]
           [org.eclipse.jgit.lib Ref Repository]
           [org.eclipse.jgit.transport BundleWriter RefSpec URIish]
           [org.eclipse.jgit.revwalk RevWalk]
           [java.security MessageDigest]
           [java.time Instant]
           [java.math BigInteger]))

(defn- sha256-file
  "Compute SHA-256 of a file path."
  [path]
  (let [digest (MessageDigest/getInstance "SHA-256")]
    (with-open [input (io/input-stream path)]
      (let [buffer (byte-array 8192)]
        (loop []
          (let [read (.read input buffer)]
            (when (pos? read)
              (.update digest buffer 0 read)
              (recur))))))
    (format "%064x" (BigInteger. 1 (.digest digest)))))

(defn- resolve-refs
  "Resolve a sequence of ref names, or return all refs when nil."
  [^Repository repo refs]
  (if (seq refs)
    (let [resolved (keep (fn [ref]
                           (let [exact (.exactRef repo ref)]
                             (when exact
                               (.getName ^Ref exact))))
                         refs)]
      (if (seq resolved)
        resolved
        (throw (ex-info "No valid refs resolved" {:refs refs}))))
    (->> (.getRefDatabase repo)
         (.getRefsByPrefix "refs/")
         (map #(.getName ^Ref %)))))

(defn export-bundle
  "Exports the repository to a git bundle file.

   Options:
   - :output required path
   - :refs refs to include (default all)
   - :verify run git bundle verify after creation (default true)
   - :include-manifest if true include metadata in result map
   - :incremental? mark manifest as incremental
   - :base-commit base commit for incremental bundle

   Returns map {:status :ok :path string :checksum string :refs seq :manifest map}."
  [repository {:keys [output refs verify base-commit incremental? include-manifest]
               :or {verify true
                    include-manifest true}}]
  (when-not repository
    (throw (ex-info "Repository is required" {})))
  (when-not output
    (throw (ex-info "Output path is required" {})))

  (let [git (Git/wrap repository)
        all-refs (resolve-refs repository refs)
        resolved-base (when base-commit
                        (.resolve repository base-commit))
        bundle (BundleWriter. repository)]
    (log/log-info (str "Creating git bundle at " output))

    (with-open [out (io/output-stream output)
                rev-walk (RevWalk. repository)]
      (when (and incremental? resolved-base)
        (log/log-info (str "Assuming base commit " resolved-base))
        (.assume bundle (.parseCommit rev-walk resolved-base)))

      (doseq [ref-name all-refs]
        (when-let [ref (.findRef repository ref-name)]
          (log/log-info (str "Including ref " ref-name " in bundle"))
          (.include bundle (.getName ref) (.getObjectId ref))))

      (.writeBundle bundle org.eclipse.jgit.lib.NullProgressMonitor/INSTANCE out))

    (when verify
      (log/log-info (str "Verifying bundle " output))
      (let [uri (URIish. (str "file://" (.getAbsolutePath (io/file output))))
            fetch (-> git
                      (.fetch)
                      (.setRemote (.toString uri))
                      (.setDryRun true)
                      (.setRefSpecs (map #(RefSpec. %) all-refs)))]
        (.call fetch)))

    (let [checksum (sha256-file output)
          manifest (when include-manifest
                     {:type (if incremental? :incremental :full)
                      :created-at (str (Instant/now))
                      :base-commit (when resolved-base (str resolved-base))
                      :refs all-refs})]
      {:status :ok
       :path output
       :checksum checksum
       :refs all-refs
       :manifest manifest})))

(defn import-bundle
  "Imports a git bundle into the repository.

   Options:
   - :input required path to bundle
   - :refs optional refspec mappings
   - :verify run preliminary fetch (dry-run) before importing (default true)

   Returns {:status :ok :refs-updated seq}."
  [repository {:keys [input refs verify]
               :or {verify true}}]
  (when-not repository
    (throw (ex-info "Repository is required" {})))
  (when-not input
    (throw (ex-info "input bundle path required" {})))
  (when-not (.exists (io/file input))
    (throw (ex-info "Bundle file not found" {:path input})))

  (let [git (Git/wrap repository)
        uri (URIish. (str "file://" (.getAbsolutePath (io/file input))))
        refspecs (if (seq refs)
                   (map #(RefSpec. %) refs)
                   [(RefSpec. "refs/*:refs/*")])]
    (when verify
      (log/log-info (str "Dry-run fetch for bundle " input))
      (-> git
          (.fetch)
          (.setRemote (.toString uri))
          (.setDryRun true)
          (.setRefSpecs refspecs)
          (.call)))

    (log/log-info (str "Fetching bundle " input))
    (let [result (-> git
                     (.fetch)
                     (.setRemote (.toString uri))
                     (.setDryRun false)
                     (.setRefSpecs refspecs)
                     (.call))]
      {:status :ok
       :refs-updated (if (seq refs)
                       refs
                       (map #(.getName %) (.getAdvertisedRefs result)))})))

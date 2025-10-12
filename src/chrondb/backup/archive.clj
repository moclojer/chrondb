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
(ns chrondb.backup.archive
  "Archive helpers for ChronDB backups"
  (:require [chrondb.util.logging :as log]
            [clojure.java.io :as io]
            [clojure.data.json :as json]
            [clojure.string :as str])
  (:import [org.eclipse.jgit.api Git]
           [java.security MessageDigest]
           [java.time Instant]
           [java.util.zip GZIPOutputStream GZIPInputStream ZipException]
           [java.math BigInteger]
           [java.io File]
           [org.apache.commons.compress.archivers.tar TarArchiveEntry
            TarArchiveOutputStream
            TarArchiveInputStream]))

(defn- sha256-file
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

(defn- write-entry
  [^TarArchiveOutputStream tar-out ^String entry-name bytes]
  (let [entry (TarArchiveEntry. entry-name)]
    (.setSize entry (count bytes))
    (.putArchiveEntry tar-out entry)
    (.write tar-out bytes)
    (.closeArchiveEntry tar-out)))

(defn- write-manifest
  [tar-stream manifest]
  (write-entry tar-stream "manifest.json" (.getBytes (json/write-str manifest) "UTF-8")))

(defn- add-file-to-tar
  [tar-stream base-path ^File file]
  (let [base-path-obj (.toPath (io/file base-path))
        file-path (.toPath file)
        relative (.relativize base-path-obj file-path)
        entry-name (-> relative str (str/replace-first #"^/" ""))
        entry (TarArchiveEntry. entry-name)]
    (.setSize entry (.length file))
    (.putArchiveEntry tar-stream entry)
    (with-open [input (io/input-stream file)]
      (io/copy input tar-stream))
    (.closeArchiveEntry tar-stream)))

(defn- tar-output-stream
  [output compress?]
  (let [raw (io/output-stream output)
        tar (TarArchiveOutputStream. (if compress?
                                       (GZIPOutputStream. raw)
                                       raw))]
    (.setLongFileMode tar TarArchiveOutputStream/LONGFILE_POSIX)
    tar))

(defn create-tar-archive
  "Create tar(.gz) archive of the repository data directory."
  [repository data-dir {:keys [output compress verify include-manifest]
                        :or {compress true
                             verify true
                             include-manifest true}}]
  (log/log-info (str "Creating tar archive at " output))
  (with-open [tar-out (tar-output-stream output compress)]
    (when include-manifest
      (write-manifest tar-out {:type :full
                               :created-at (str (Instant/now))
                               :format (if compress :tar.gz :tar)
                               :branch (.getBranch repository)}))
    (doseq [file (file-seq (io/file data-dir))]
      (when (.isFile ^File file)
        (add-file-to-tar tar-out data-dir file))))
  (when verify
    (log/log-info "Running git gc --no-verify prior to archive (fsck unavailable via JGit)")
    (-> (Git/wrap repository)
        (.gc)
        (.setAggressive false)
        (.call)))
  (let [checksum (sha256-file output)]
    {:status :ok
     :path output
     :checksum checksum
     :manifest {:type :full
                :created-at (str (Instant/now))
                :format (if compress :tar.gz :tar)}}))

(defn create-incremental-archive
  "Creates an incremental tar archive.

   Current implementation relies on git bundle style incremental backups.
   Since tar-based incrementals are not yet supported, this function will
   raise until implemented."
  [_repository _data-dir _options]
  (throw (ex-info "Tar-based incremental backups are not supported yet" {})))

(defn extract-archive
  "Extract tar(.gz) archive into data directory."
  [repository data-dir {:keys [input verify]
                        :or {verify true}}]
  (log/log-info (str "Extracting archive " input " to " data-dir))
  (let [input-stream (io/input-stream input)
        tar-stream (try
                     (TarArchiveInputStream. (GZIPInputStream. input-stream))
                     (catch ZipException _
                       (TarArchiveInputStream. input-stream)))]
    (with-open [tar tar-stream]
      (loop []
        (when-let [entry (.getNextTarEntry tar)]
          (let [target (io/file data-dir (.getName entry))]
            (if (.isDirectory entry)
              (.mkdirs target)
              (do
                (io/make-parents target)
                (with-open [out (io/output-stream target)]
                  (io/copy tar out)))))
          (recur)))))
  (when verify
    (log/log-info "Running git gc --no-verify after extraction (fsck unavailable via JGit)")
    (-> (Git/wrap repository)
        (.gc)
        (.setAggressive false)
        (.call)))
  {:status :ok
   :restore-type :archive})

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
(ns chrondb.backup.core
  "High level backup/restore API for ChronDB."
  (:require [chrondb.backup.archive :as archive]
            [chrondb.backup.git :as git]
            [chrondb.backup.scheduler :as scheduler]
            [chrondb.util.logging :as log]
            [clojure.java.io :as io]
            [clojure.string :as str])
  (:import [java.time Instant]))

(defn- ensure-parent-dirs
  "Create parent directories for a target path if needed."
  [path]
  (let [file (io/file path)
        parent (.getParentFile file)]
    (when parent
      (.mkdirs parent))))

(defn create-full-backup
  "Creates a full backup of the ChronDB repository.

   Options map:
   - :output-path (required) destination path
   - :format :tar.gz (default) or :bundle
   - :compress? boolean, defaults to true when format is :tar.gz
   - :verify? run integrity checks (default true)
   - :include-manifest? whether to embed metadata manifest (default true)
   - :refs collection of refs when format :bundle

   Returns map {:status :ok :path string :checksum string :manifest map}."
  ([storage]
   (create-full-backup storage {}))
  ([storage {:keys [output-path format compress? verify? include-manifest? refs]
             :or {format :tar.gz
                  compress? true
                  verify? true
                  include-manifest? true}}]
   (when-not (and output-path (not (str/blank? output-path)))
     (throw (ex-info "Output path is required for backup" {:option :output-path})))

   (let [repository (:repository storage)
         data-dir (:data-dir storage)]
     (when-not repository
       (throw (ex-info "Storage repository is closed" {:storage storage})))

     (ensure-parent-dirs output-path)

     (if (= format :bundle)
       (git/export-bundle repository {:output output-path
                                      :refs refs
                                      :verify verify?
                                      :include-manifest include-manifest?})
       (archive/create-tar-archive repository data-dir {:output output-path
                                                        :compress compress?
                                                        :verify verify?
                                                        :include-manifest include-manifest?})))))

(defn create-incremental-backup
  "Creates an incremental backup using git bundle format only.

   Options map:
   - :output-path required
   - :base-commit commit hash of last full backup
   - :format currently must be :bundle

   Returns metadata map similar to `create-full-backup`."
  [storage {:keys [output-path base-commit format]
            :or {format :bundle}}]
  (when-not base-commit
    (throw (ex-info "Incremental backup requires :base-commit" {:option :base-commit})))
  (when-not output-path
    (throw (ex-info "Incremental backup requires :output-path" {:option :output-path})))
  (when (not= format :bundle)
    (throw (ex-info "Incremental backups only supported for :bundle format" {:format format})))

  (ensure-parent-dirs output-path)

  (git/export-bundle (:repository storage)
                     {:output output-path
                      :base-commit base-commit
                      :incremental? true}))

(defn restore-backup
  "Restores a ChronDB repository from a backup file.

   Options map:
   - :input-path required
   - :format :tar.gz (default) or :bundle
   - :verify? run integrity checks before applying (default true)
   - :rebuild-index? when true log request to rebuild indices (TODO)

   Returns map {:status :ok :restore-type keyword :index-rebuilt? boolean}."
  [storage {:keys [input-path format verify? rebuild-index?]
            :or {format :tar.gz
                 verify? true
                 rebuild-index? true}}]
  (when-not input-path
    (throw (ex-info "Restore requires :input-path" {})))
  (when-not (.exists (io/file input-path))
    (throw (ex-info "Backup file not found" {:path input-path})))

  (let [repository (:repository storage)
        data-dir (:data-dir storage)]
    (when-not repository
      (throw (ex-info "Storage repository is closed" {:storage storage})))

    (if (= format :bundle)
      (git/import-bundle repository {:input input-path :verify verify?})
      (archive/extract-archive repository data-dir {:input input-path :verify verify?}))

    (when rebuild-index?
      (log/log-info "Index rebuild requested after restore (not yet implemented)"))

    {:status :ok
     :restore-type (if (= format :bundle) :bundle :archive)
     :index-rebuilt? rebuild-index?}))

(defn export-snapshot
  "Exports the repository to a git bundle snapshot.

   Options map:
   - :output required path to .bundle file
   - :refs optional collection of refs to include
   - :commit optional commit-ish to include
   - :verify run git bundle verify (default true)

   Returns {:status :ok :path string :checksum string :refs [...]}"
  [storage {:keys [output] :as options}]
  (when-not output
    (throw (ex-info "export-snapshot requires :output" {:options options})))
  (ensure-parent-dirs output)
  (git/export-bundle (:repository storage) options))

(defn import-snapshot
  "Imports a git bundle snapshot into the repository.

   Options map:
   - :input required path to bundle
   - :refs optional refspec mappings
   - :verify run git bundle verify (default true)

   Returns {:status :ok :refs-updated [...]}"
  [storage {:keys [input] :as options}]
  (when-not input
    (throw (ex-info "import-snapshot requires :input" {:options options})))
  (when-not (.exists (io/file input))
    (throw (ex-info "Bundle file not found" {:path input})))
  (git/import-bundle (:repository storage) options))

(defn schedule-backup
  "Schedules recurring backups.

   Options map:
   - :mode :full (default) or :incremental
   - :format :tar.gz (default) or :bundle
   - :interval-minutes required positive integer
   - :output-dir required directory for artifacts
   - :retention optional number of backups to keep
   - :base-commit required when mode :incremental

   Returns job-id string."
  [storage {:keys [mode format interval-minutes output-dir retention base-commit]
            :or {mode :full
                 format :tar.gz}}]
  (when-not (and interval-minutes (pos? interval-minutes))
    (throw (ex-info "interval-minutes must be > 0" {:value interval-minutes})))
  (when-not output-dir
    (throw (ex-info "output-dir is required" {})))
  (when (and (= mode :incremental) (str/blank? base-commit))
    (throw (ex-info "Incremental schedule requires :base-commit" {})))

  (io/make-parents (io/file output-dir "dummy"))

  (scheduler/schedule
   (fn []
     (let [timestamp (-> (Instant/now) str (str/replace #":" "-"))
           extension (case format
                       :bundle "bundle"
                       :tar "tar"
                       "tar.gz")
           output (str output-dir "/chrondb-" (name mode) "-" timestamp "." extension)
           result (case mode
                    :incremental (create-incremental-backup storage {:output-path output
                                                                     :base-commit base-commit
                                                                     :format format})
                    (create-full-backup storage {:output-path output
                                                 :format format}))]
       (when retention
         (scheduler/enforce-retention output-dir retention))
       (log/log-info (str "Scheduled backup completed: " output))
       result))
   interval-minutes))

(defn cancel-scheduled-backup
  "Cancels a scheduled backup job.
   Returns true when job existed."
  [job-id]
  (scheduler/cancel job-id))

(defn list-scheduled-backups
  "Returns metadata about scheduled backup jobs."
  []
  (scheduler/list-jobs))

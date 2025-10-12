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
(ns chrondb.backup.scheduler
  "Simple scheduler for recurring ChronDB backups"
  (:require [chrondb.util.logging :as log]
            [clojure.java.io :as io])
  (:import [java.util.concurrent ScheduledThreadPoolExecutor TimeUnit]
           [java.util UUID]))

(defonce ^ScheduledThreadPoolExecutor executor
  (doto (ScheduledThreadPoolExecutor. 1)
    (.setRemoveOnCancelPolicy true)))

(defonce jobs (atom {}))

(defn schedule
  "Schedules a task to run every `interval-minutes`.
   Returns job-id string."
  [task interval-minutes]
  (let [job-id (str (UUID/randomUUID))
        runnable (proxy [Runnable] []
                   (run []
                     (try
                       (task)
                       (catch Exception e
                         (log/log-warn (str "Scheduled backup " job-id " failed: " (.getMessage e)))))))]
    (log/log-info (str "Scheduling backup job " job-id " every " interval-minutes " minutes"))

    (let [future (.scheduleAtFixedRate executor runnable 0 interval-minutes TimeUnit/MINUTES)]
      (swap! jobs assoc job-id future)
      job-id)))

(defn cancel
  [job-id]
  (when-let [future (get @jobs job-id)]
    (log/log-info (str "Cancelling backup job " job-id))
    (.cancel future true)
    (swap! jobs dissoc job-id)
    true))

(defn list-jobs
  []
  (map (fn [[job-id future]]
         {:job-id job-id
          :cancelled? (.isCancelled future)
          :done? (.isDone future)})
       @jobs))

(defn enforce-retention
  [dir retention]
  (let [files (->> (.listFiles (io/file dir))
                   (filter #(.isFile %))
                   (sort-by #(.lastModified %) >))]
    (when (> (count files) retention)
      (doseq [file (drop retention files)]
        (try
          (log/log-info (str "Removing old backup " (.getAbsolutePath file)))
          (.delete file)
          (catch Exception e
            (log/log-warn (str "Failed to delete old backup " (.getAbsolutePath file) ": " (.getMessage e)))))))))

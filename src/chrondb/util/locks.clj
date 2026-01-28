(ns chrondb.util.locks
  "Utility functions for handling stale lock files.
   Both Git and Lucene use file-based locking that can leave orphan locks
   when a process crashes or is killed unexpectedly."
  (:require [clojure.java.io :as io]))

;; Stale lock timeout in milliseconds (60 seconds)
;; Locks older than this are considered orphaned from crashed processes
(def ^:private stale-lock-timeout-ms 60000)

(defn find-lock-files
  "Recursively finds all .lock files in a directory.
   Returns a sequence of java.io.File objects."
  [dir]
  (let [dir-file (io/file dir)]
    (when (.exists dir-file)
      (->> (file-seq dir-file)
           (filter #(and (.isFile %)
                         (.endsWith (.getName %) ".lock")))))))

(defn stale-lock?
  "Returns true if the lock file is older than the stale timeout.
   A lock is considered stale if it was created more than 60 seconds ago,
   which indicates the creating process likely crashed."
  [^java.io.File lock-file]
  (let [last-modified (.lastModified lock-file)
        age-ms (- (System/currentTimeMillis) last-modified)]
    (> age-ms stale-lock-timeout-ms)))

(defn clean-stale-locks
  "Removes stale lock files from the given directory.
   A lock is considered stale if it's older than stale-lock-timeout-ms.

   This is safe for:
   - Single-process library usage where the previous process is gone
   - Server startup where no other process should be using the locks

   Options:
   - :force? - if true, removes all locks regardless of age (default: false)
   - :verbose? - if true, prints removed lock files (default: false)

   Returns the number of locks removed."
  ([path]
   (clean-stale-locks path {}))
  ([path opts]
   (if (nil? path)
     0
     (let [dir (io/file path)
           force? (:force? opts false)
           verbose? (:verbose? opts false)
           removed (atom 0)]
       (when (.exists dir)
         (doseq [lock-file (find-lock-files dir)]
           (when (or force? (stale-lock? lock-file))
             (try
               (when verbose?
                 (println "Removing stale lock file:" (.getPath lock-file)))
               (.delete lock-file)
               (swap! removed inc)
               (catch Exception _e
                 ;; Ignore deletion failures - file might be in use
                 nil)))))
       @removed))))

(defn clean-lucene-lock
  "Convenience function to clean the Lucene write.lock file.
   Lucene creates write.lock in the index directory root."
  ([index-path]
   (clean-lucene-lock index-path {}))
  ([index-path opts]
   (let [lock-file (io/file index-path "write.lock")
         force? (:force? opts false)
         verbose? (:verbose? opts false)]
     (when (.exists lock-file)
       (when (or force? (stale-lock? lock-file))
         (try
           (when verbose?
             (println "Removing stale Lucene lock file"))
           (.delete lock-file)
           1
           (catch Exception _e 0)))))))

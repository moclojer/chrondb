(ns chrondb.util.locks
  "Utility functions for handling stale lock files.
   Both Git and Lucene use file-based locking that can leave orphan locks
   when a process crashes or is killed unexpectedly."
  (:require [clojure.java.io :as io]
            [clojure.string :as str])
  (:import [java.lang ProcessHandle]
           [java.io RandomAccessFile]
           [java.nio.channels FileLock OverlappingFileLockException]))

;; Stale lock timeout in milliseconds (60 seconds)
;; Locks older than this are considered orphaned from crashed processes
(def ^:private stale-lock-timeout-ms 60000)

;; Known lock file patterns that ChronDB/JGit/Lucene create
(def ^:private known-lock-patterns
  [#".*\.lock$"           ; Generic .lock files
   #".*/refs/heads/.*"    ; JGit branch refs (may have .lock suffix during update)
   #"write\.lock$"])      ; Lucene write lock

(defn find-lock-files
  "Recursively finds all .lock files in a directory.
   Returns a sequence of java.io.File objects."
  [dir]
  (let [dir-file (io/file dir)]
    (when (.exists dir-file)
      (->> (file-seq dir-file)
           (filter (fn [^java.io.File f]
                     (and (.isFile f)
                          (.endsWith (.getName f) ".lock"))))))))

(defn stale-lock?
  "Returns true if the lock file is older than the stale timeout.
   A lock is considered stale if it was created more than 60 seconds ago,
   which indicates the creating process likely crashed."
  [^java.io.File lock-file]
  (let [last-modified (.lastModified lock-file)
        age-ms (- (System/currentTimeMillis) last-modified)]
    (> age-ms stale-lock-timeout-ms)))

(defn lock-is-held?
  "Tries to acquire an exclusive lock on the file.
   Returns true if the lock is currently held by another process.
   Returns false if we can acquire the lock (meaning it's orphaned)."
  [^java.io.File lock-file]
  (try
    (let [raf (RandomAccessFile. lock-file "rw")
          channel (.getChannel raf)]
      (try
        (let [lock (.tryLock channel)]
          (if lock
            (do
              ;; We got the lock, so it's not held by anyone else
              (.release lock)
              false)
            ;; Couldn't get the lock, it's held by another process
            true))
        (catch OverlappingFileLockException _
          ;; Lock is held by this JVM (another thread)
          true)
        (finally
          (.close channel)
          (.close raf))))
    (catch Exception _
      ;; If we can't even open the file, assume it's locked
      true)))

(defn can-safely-remove-lock?
  "Determines if a lock file can be safely removed.
   A lock can be removed if:
   1. It's older than the stale timeout (60s), OR
   2. It's not currently held by any process (orphaned), OR
   3. Force mode is enabled"
  [^java.io.File lock-file opts]
  (let [force? (:force? opts false)
        check-held? (:check-held? opts true)]
    (or force?
        (stale-lock? lock-file)
        (and check-held? (not (lock-is-held? lock-file))))))

(defn clean-stale-locks
  "Removes stale lock files from the given directory.
   A lock is considered removable if:
   - It's older than 60 seconds (stale timeout), OR
   - It's not currently held by any process (orphaned lock), OR
   - :force? option is true

   This is safe for:
   - Single-process library usage where the previous process is gone
   - Server startup where no other process should be using the locks
   - Recovery from crashed processes

   Options:
   - :force? - if true, removes all locks regardless of state (default: false)
   - :verbose? - if true, prints removed lock files (default: false)
   - :check-held? - if true, checks if lock is actively held (default: true)

   Returns the number of locks removed."
  ([path]
   (clean-stale-locks path {}))
  ([path opts]
   (if (nil? path)
     0
     (let [dir (io/file path)
           verbose? (:verbose? opts false)
           removed (atom 0)]
       (when (.exists dir)
         (doseq [lock-file (find-lock-files dir)]
           (when (can-safely-remove-lock? lock-file opts)
             (try
               (when verbose?
                 (println "Removing lock file:" (.getPath lock-file)
                          (if (stale-lock? lock-file)
                            "(stale)"
                            "(orphaned)")))
               (.delete lock-file)
               (swap! removed inc)
               (catch Exception _e
                 ;; Ignore deletion failures - file might be in use
                 nil)))))
       @removed))))

(defn clean-lucene-lock
  "Convenience function to clean the Lucene write.lock file.
   Lucene creates write.lock in the index directory root.
   Uses the same logic as clean-stale-locks for determining if removal is safe."
  ([index-path]
   (clean-lucene-lock index-path {}))
  ([index-path opts]
   (let [lock-file (io/file index-path "write.lock")
         verbose? (:verbose? opts false)]
     (when (.exists lock-file)
       (when (can-safely-remove-lock? lock-file opts)
         (try
           (when verbose?
             (println "Removing Lucene write.lock"
                      (if (stale-lock? lock-file) "(stale)" "(orphaned)")))
           (.delete lock-file)
           1
           (catch Exception _e 0)))))))

(defn list-active-locks
  "Lists all lock files in a directory with their status.
   Returns a sequence of maps with :path, :age-ms, :stale?, :held? keys.
   Useful for debugging lock issues."
  [path]
  (when-let [locks (find-lock-files path)]
    (for [^java.io.File lock locks]
      (let [age-ms (- (System/currentTimeMillis) (.lastModified lock))]
        {:path (.getPath lock)
         :age-ms age-ms
         :stale? (> age-ms stale-lock-timeout-ms)
         :held? (lock-is-held? lock)}))))

(defn clean-all-chrondb-locks
  "Cleans all ChronDB-related locks (Git and Lucene) from given paths.
   This is a convenience function for cleaning up after crashes.

   Parameters:
   - data-path: Git repository path
   - index-path: Lucene index path
   - opts: Same options as clean-stale-locks

   Returns: Total number of locks removed."
  ([data-path index-path]
   (clean-all-chrondb-locks data-path index-path {}))
  ([data-path index-path opts]
   (+ (clean-stale-locks data-path opts)
      (clean-stale-locks index-path opts))))

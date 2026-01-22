;; This file is part of ChronDB.
;;
;; ChronDB is free software: you can redistribute it and/or modify
;; it under the terms of the GNU Affero General Public License as published
;; by the Free Software Foundation, either version 3 of the License,
;; or (at your option) any later version.
(ns chrondb.concurrency.occ
  "Optimistic Concurrency Control for ChronDB.
   Provides version-based conflict detection and automatic retry."
  (:require [chrondb.util.logging :as log]))

;; Exception types for OCC
(defn version-conflict-exception
  "Create a version conflict exception."
  [expected actual branch document-id]
  (ex-info "Optimistic lock conflict: document was modified"
           {:type :version-conflict
            :expected-version expected
            :actual-version actual
            :branch branch
            :document-id document-id}))

(defn version-conflict?
  "Check if an exception is a version conflict."
  [e]
  (and (instance? clojure.lang.ExceptionInfo e)
       (= :version-conflict (:type (ex-data e)))))

;; Retry logic with exponential backoff
(def ^:private default-retry-config
  {:max-retries 3
   :initial-delay-ms 10
   :max-delay-ms 1000
   :backoff-multiplier 2.0
   :jitter-factor 0.1})

(defn- calculate-delay
  "Calculate delay with exponential backoff and jitter."
  [attempt {:keys [initial-delay-ms max-delay-ms backoff-multiplier jitter-factor]}]
  (let [base-delay (* initial-delay-ms (Math/pow backoff-multiplier (dec attempt)))
        capped-delay (min base-delay max-delay-ms)
        jitter (* capped-delay jitter-factor (- (* 2 (Math/random)) 1))]
    (long (+ capped-delay jitter))))

(defn with-occ-retry
  "Execute a function with OCC retry logic.

   Options:
   - :max-retries     - Maximum number of retry attempts (default 3)
   - :initial-delay-ms - Initial delay between retries (default 10)
   - :max-delay-ms    - Maximum delay between retries (default 1000)
   - :on-retry        - Function called on each retry with (attempt exception)
   - :on-conflict     - Function called on conflict before retry

   Returns the result of f, or throws the last exception after all retries."
  ([f] (with-occ-retry f {}))
  ([f opts]
   (let [config (merge default-retry-config opts)
         {:keys [max-retries on-retry on-conflict]} config]
     (loop [attempt 1]
       (let [result (try
                      {:success (f)}
                      (catch clojure.lang.ExceptionInfo e
                        (if (version-conflict? e)
                          {:conflict e}
                          (throw e))))]
         (if (:success result)
           (:success result)
           (if (< attempt max-retries)
             (let [delay-ms (calculate-delay attempt config)]
               (when on-conflict
                 (on-conflict (:conflict result)))
               (when on-retry
                 (on-retry attempt (:conflict result)))
               (log/log-info (str "OCC: Retry attempt " attempt "/" max-retries
                                  " after " delay-ms "ms delay"))
               (Thread/sleep delay-ms)
               (recur (inc attempt)))
             (do
               (log/log-warn (str "OCC: All " max-retries " retries exhausted"))
               (throw (:conflict result))))))))))

;; Version tracking
(defprotocol VersionTracker
  "Protocol for tracking document versions."
  (get-version [this document-id branch]
    "Get the current version of a document.")
  (set-version [this document-id branch version]
    "Set the version of a document.")
  (increment-version [this document-id branch]
    "Increment and return the new version of a document."))

(defrecord InMemoryVersionTracker [versions]
  VersionTracker
  (get-version [_ document-id branch]
    (get @versions [document-id branch] 0))

  (set-version [_ document-id branch version]
    (swap! versions assoc [document-id branch] version)
    version)

  (increment-version [_ document-id branch]
    (let [new-version (swap! versions update [document-id branch] (fnil inc 0))]
      (get new-version [document-id branch]))))

(defn create-version-tracker
  "Create a new in-memory version tracker."
  []
  (->InMemoryVersionTracker (atom {})))

;; OCC-aware operations
(defn verify-version
  "Verify that the expected version matches the current version.
   Throws version-conflict-exception if they don't match."
  [tracker document-id branch expected-version]
  (when expected-version
    (let [current-version (get-version tracker document-id branch)]
      (when (and (pos? current-version)
                 (not= expected-version current-version))
        (throw (version-conflict-exception expected-version current-version
                                           branch document-id))))))

(defn with-version-check
  "Execute an operation with version checking.

   Parameters:
   - tracker: Version tracker instance
   - document-id: ID of the document being modified
   - branch: Git branch
   - expected-version: Expected version (nil to skip check)
   - f: Function to execute

   Returns the result of f and increments the version on success."
  [tracker document-id branch expected-version f]
  (verify-version tracker document-id branch expected-version)
  (let [result (f)]
    (increment-version tracker document-id branch)
    result))

;; Branch-level locking for write serialization
(def ^:private branch-locks (atom {}))

(defn get-branch-lock
  "Get or create a lock for a branch."
  [branch]
  (let [lock (get @branch-locks branch)]
    (if lock
      lock
      (let [new-lock (Object.)]
        (swap! branch-locks assoc branch new-lock)
        new-lock))))

(defmacro with-branch-lock
  "Execute body with a lock on the specified branch.
   Serializes writes to the same branch."
  [branch & body]
  `(let [lock# (get-branch-lock ~branch)]
     (locking lock#
       ~@body)))

;; Utility for detecting conflicts from Git
(defn check-git-head-unchanged
  "Check if the Git HEAD for a branch matches the expected commit.
   Returns true if unchanged, throws version-conflict-exception otherwise."
  [repository branch expected-commit-id document-id]
  (when expected-commit-id
    (let [current-head (.resolve repository (str branch "^{commit}"))
          current-head-str (when current-head (.getName current-head))]
      (when (and current-head-str
                 (not= expected-commit-id current-head-str))
        (throw (version-conflict-exception expected-commit-id current-head-str
                                           branch document-id)))))
  true)

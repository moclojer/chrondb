(ns chrondb.transaction.core
  "Transactional context helpers for ChronDB operations.
   Provides thread-local metadata used to annotate Git commits and notes."
  (:require [chrondb.util.logging :as log])
  (:import [java.time Instant ZoneOffset]
           [java.time.format DateTimeFormatter]
           [java.util UUID]))

(def ^:dynamic *transaction-context*
  "Holds the currently active transaction context as an atom."
  nil)

(def ^DateTimeFormatter iso-formatter
  (-> DateTimeFormatter/ISO_INSTANT
      (.withZone ZoneOffset/UTC)))

(defn- now-iso []
  (.format iso-formatter (Instant/now)))

(defn- normalize-string [value]
  (when-not (nil? value)
    (str value)))

(defn- normalize-timestamp [value]
  (cond
    (nil? value) (now-iso)
    (instance? Instant value) (.format iso-formatter ^Instant value)
    (string? value) value
    :else (str value)))

(defn- normalize-flags [flags]
  (letfn [(spread [value]
            (cond
              (nil? value) []
              (and (coll? value) (not (string? value))) (mapcat spread value)
              :else [value]))]
    (->> flags
         spread
         (keep normalize-string)
         (into (sorted-set)))))

(defn- ensure-map [value]
  (if (map? value) value {}))

(defn- base-context
  [{:keys [tx-id origin user timestamp flags metadata status] :as opts}]
  {:tx-id (normalize-string (or tx-id (UUID/randomUUID)))
   :origin (or (normalize-string origin) "unknown")
   :user (normalize-string user)
   :timestamp (normalize-timestamp timestamp)
   :metadata (ensure-map metadata)
   :flags (normalize-flags flags)
   :status (or status :pending)
   :started-at (normalize-timestamp timestamp)
   :ended-at nil
   :commit-count 0})

(defn- merge-into-context
  [context {:keys [origin user timestamp flags metadata status]}]
  (-> context
      (update :origin #(or (normalize-string origin) %))
      (update :user #(or (normalize-string user) %))
      (cond-> timestamp (assoc :timestamp (normalize-timestamp timestamp)))
      (update :metadata merge (ensure-map metadata))
      (update :flags into (normalize-flags flags))
      (cond-> status (assoc :status status))))

(defn- ensure-context-atom []
  (or *transaction-context*
      (throw (IllegalStateException. "No active transaction context"))))

(defn in-transaction?
  "Returns true when a transaction context is currently bound."
  []
  (boolean *transaction-context*))

(defn current-context
  "Returns the current transaction context map (without metadata) or nil."
  []
  (some-> *transaction-context*
          (deref)))

(defn add-flags!
  "Adds one or more flags to the active transaction context."
  [& flags]
  (swap! (ensure-context-atom) update :flags into (normalize-flags flags))
  nil)

(defn set-origin!
  "Sets the origin for the active transaction context."
  [origin]
  (swap! (ensure-context-atom) assoc :origin (or (normalize-string origin) "unknown"))
  nil)

(defn set-user!
  "Sets the user identifier for the active transaction context."
  [user]
  (swap! (ensure-context-atom) assoc :user (normalize-string user))
  nil)

(defn merge-metadata!
  "Merges metadata into the active transaction context."
  [m]
  (swap! (ensure-context-atom) update :metadata merge (ensure-map m))
  nil)

(defn- commit-success!
  [ctx]
  (-> ctx
      (assoc :status :committed
             :ended-at (now-iso))
      (update :commit-count inc)))

(defn- commit-rollback!
  [ctx]
  (-> ctx
      (assoc :status :rolled-back
             :ended-at (now-iso))
      (update :flags into (normalize-flags ["rollback"]))))

(defn context->note
  "Converts a transaction context into a Git note payload.
   Optional overrides may include :commit-id, :commit-message, :branch,
   :path, :document-id, :operation and :flags."
  ([context]
   (context->note context nil))
  ([context {:keys [commit-id commit-message branch path document-id operation flags metadata timestamp]
             :as overrides}]
   (let [merged (-> context
                    (merge-into-context {:flags flags
                                         :metadata metadata
                                         :timestamp timestamp})
                    (update :flags normalize-flags))
         payload (cond-> {:tx_id (:tx-id merged)
                          :origin (:origin merged)
                          :timestamp (:timestamp merged)}
                   (:user merged) (assoc :user (:user merged))
                   (seq (:metadata merged)) (assoc :metadata (:metadata merged))
                   (seq (:flags merged)) (assoc :flags (vec (:flags merged)))
                   (:status merged) (assoc :status (name (:status merged)))
                   commit-id (assoc :commit_id commit-id)
                   commit-message (assoc :commit_message commit-message)
                   branch (assoc :branch branch)
                   path (assoc :path path)
                   document-id (assoc :document_id document-id)
                   operation (assoc :operation operation))]
     payload)))

(defn context-for-commit
  "Returns the note payload for the current transaction (or a fresh context when
   none exists). Accepts the same overrides as `context->note`."
  [overrides]
  (if *transaction-context*
    (let [ctx (swap! *transaction-context*
                     (fn [state]
                       (-> state
                           (merge-into-context overrides)
                           (update :commit-count inc))))]
      (context->note ctx overrides))
    (let [ctx (-> (base-context overrides)
                  (update :commit-count inc))]
      (context->note ctx overrides))))

(defn with-transaction*
  "Executes the given function within a transactional context.
   Options support :origin, :user, :timestamp, :metadata, :flags and :on-complete.
   When :on-complete is provided, it will be invoked with [final-context result error]."
  [storage {:keys [on-complete] :as opts} f]
  (let [context-opts (dissoc opts :on-complete)
        existing *transaction-context*]
    (if existing
      (let [snapshot @existing]
        (try
          (swap! existing merge-into-context context-opts)
          (f)
          (finally
            (reset! existing snapshot))))
      (let [ctx-atom (atom (base-context context-opts))]
        (binding [*transaction-context* ctx-atom]
          (try
            (let [result (f)
                  final (swap! ctx-atom commit-success!)]
              (when on-complete
                (try
                  (on-complete final result nil)
                  (catch Exception e
                    (log/log-warn "Transaction on-complete handler failed" (.getMessage e)))))
              result)
            (catch Throwable t
              (let [final (swap! ctx-atom commit-rollback!)]
                (when on-complete
                  (try
                    (on-complete final nil t)
                    (catch Exception e
                      (log/log-warn "Transaction on-complete handler failed" (.getMessage e)))))
                (throw t)))))))))

(defmacro with-transaction
  "Execute body within a transaction-aware context.
   Usage: (with-transaction [storage {:origin \"rest\"}] ...)
   The `storage` binding is evaluated but not modified."
  [[storage & {:as opts}] & body]
  `(with-transaction* ~storage ~(or opts {}) (fn [] ~@body)))


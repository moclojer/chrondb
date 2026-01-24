(ns chrondb.storage.git.notes
  "Utilities for reading and writing Git notes used by ChronDB transactions."
  (:require [clojure.data.json :as json]
            [chrondb.util.logging :as log])
  (:import [java.nio.charset StandardCharsets]
           [org.eclipse.jgit.api Git]
           [org.eclipse.jgit.lib ObjectId Repository]
           [org.eclipse.jgit.notes Note]
           [org.eclipse.jgit.revwalk RevWalk]))

(def default-notes-ref "refs/notes/chrondb")

(defn- ->object-id [commit-id]
  (cond
    (instance? ObjectId commit-id) commit-id
    (string? commit-id) (ObjectId/fromString commit-id)
    :else (throw (IllegalArgumentException.
                  (str "Unsupported commit id: " (type commit-id))))))

(defn- merge-flags [existing incoming]
  (->> (concat (or existing []) (or incoming []))
       (keep identity)
       (map str)
       distinct
       vec))

(defn merge-note
  "Merges two git-note payload maps. Flags are merged with set semantics;
   other keys from `incoming` override `existing`."
  [existing incoming]
  (-> (merge existing incoming)
      (assoc :flags (merge-flags (:flags existing) (:flags incoming)))))

(defn read-note
  "Reads the git note associated with `commit-id`. Returns a map or nil if none exists."
  (^clojure.lang.IPersistentMap [^Git git commit-id]
   (read-note git commit-id default-notes-ref))
  (^clojure.lang.IPersistentMap [^Git git commit-id notes-ref]
   (let [^ObjectId object-id (->object-id commit-id)
         ^Repository repo (.getRepository git)]
     (with-open [^RevWalk rev-walk (RevWalk. repo)]
       (let [rev-object (.parseAny rev-walk object-id)
             cmd (-> git
                     (.notesShow)
                     (.setNotesRef ^String notes-ref)
                     (.setObjectId rev-object))]
         (when-let [^Note note (.call cmd)]
           (let [^org.eclipse.jgit.lib.ObjectLoader loader (.open repo (.getData note))
                 content (String. (.getBytes loader) StandardCharsets/UTF_8)]
             (try
               (json/read-str content :key-fn keyword)
               (catch Exception e
                 (log/log-warn "Failed to parse git note" (.getMessage e))
                 nil)))))))))

(defn add-git-note
  "Adds or updates a git note for the given commit. Returns the merged payload."
  (^clojure.lang.IPersistentMap [^Git git commit-id payload]
   (add-git-note git commit-id payload {}))
  (^clojure.lang.IPersistentMap [^Git git commit-id payload {:keys [notes-ref]
                                                             :or {notes-ref default-notes-ref}}]
   (let [^ObjectId object-id (->object-id commit-id)
         ^Repository repo (.getRepository git)
         existing (read-note git object-id notes-ref)
         merged (merge-note existing payload)
         ^String message (json/write-str merged)]
     (with-open [^RevWalk rev-walk (RevWalk. repo)]
       (let [rev-object (.parseAny rev-walk object-id)]
         (when existing
           (-> git
               (.notesRemove)
               (.setNotesRef ^String notes-ref)
               (.setObjectId rev-object)
               (.call)))
         (-> git
             (.notesAdd)
             (.setNotesRef ^String notes-ref)
             (.setObjectId rev-object)
             (.setMessage message)
             (.call))
         merged)))))
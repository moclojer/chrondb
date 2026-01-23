(ns chrondb.config
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]))

(def ^:private default-config
  {:git {:committer-name "ChronDB"
         :committer-email "chrondb@example.com"
         :default-branch "main"
         :sign-commits false
         ;; Remote repository settings
         :remote-url nil        ; e.g. "git@github.com:org/repo.git"
         :push-enabled true     ; Enable/disable push after commits
         :push-notes true       ; Push transaction notes (refs/notes/chrondb)
         :pull-on-start true    ; Pull from remote on storage initialization
         ;; SSH settings (used when remote-url starts with "git@")
         :ssh {}}
   :storage {:data-dir "data"}
   :logging {:level :info  ; :debug, :info, :warn, :error
             :output :stdout  ; :stdout, :file
             :file "chrondb.log"}})

(defn- deep-merge
  "Recursively merges maps"
  [& maps]
  (if (every? map? maps)
    (apply merge-with deep-merge maps)
    (last maps)))

(defn load-config
  "Loads configuration from a file, merging with defaults"
  ([] default-config)
  ([path]
   (try
     (if (.exists (io/file path))
       (let [file-config (-> path slurp edn/read-string)]
         (deep-merge default-config file-config))
       (do
         (println "Config file not found at" path ", using defaults")
         default-config))
     (catch Exception e
       (println "Error loading config:" (.getMessage e))
       default-config))))

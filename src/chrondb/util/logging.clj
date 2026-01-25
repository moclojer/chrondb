(ns chrondb.util.logging
  (:require [clojure.string :as str])
  (:import [java.time LocalDateTime]
           [java.time.format DateTimeFormatter]))

(def ^:private log-levels
  {:debug 0
   :info 1
   :warn 2
   :error 3
   :off 4})

(def ^:private default-config
  {:min-level :info
   :output :stdout})

(def ^:private current-config
  (atom default-config))

(defn init!
  "Initialize logging with the given configuration"
  [config]
  (reset! current-config (merge default-config config)))

(defn- log-level-enabled?
  [level]
  (let [config @current-config
        min-level (get config :min-level :info)]
    (>= (get log-levels level)
        (get log-levels min-level))))

(defn- format-log-message
  [level & msgs]
  (let [timestamp (.format (LocalDateTime/now)
                           (DateTimeFormatter/ofPattern "yyyy-MM-dd HH:mm:ss.SSS"))
        level-str (str/upper-case (name level))
        msg (str/join " " msgs)]
    (format "%s [%s] %s" timestamp level-str msg)))

(defn- write-log
  [level & msgs]
  (when (log-level-enabled? level)
    (let [formatted-msg (apply format-log-message level msgs)]
      (case (get-in @current-config [:logging :output])
        :file (spit (get-in @current-config [:logging :file])
                    (str formatted-msg "\n")
                    :append true)
        :stdout (println formatted-msg)
        (println formatted-msg)))))

(defn log-debug
  "Log debug message"
  [& msgs]
  (apply write-log :debug msgs))

(defn log-info
  "Log informational message"
  [& msgs]
  (apply write-log :info msgs))

(defn log-warn
  "Log warning message"
  [& msgs]
  (apply write-log :warn msgs))

(defn log-error
  "Log error message"
  ([msg]
   (write-log :error msg))
  ([msg e]
   (let [sw (java.io.StringWriter.)
         pw (java.io.PrintWriter. sw)]
     (.printStackTrace e pw)
     (write-log :error (str msg "\n" (.toString sw))))))

(defn log-git-status
  "Log Git repository status"
  [status]
  (log-debug "\nGit status:")
  (log-debug "Modified:" (.getModified status))
  (log-debug "Added:" (.getAdded status))
  (log-debug "Removed:" (.getRemoved status))
  (log-debug "Untracked:" (.getUntracked status)))
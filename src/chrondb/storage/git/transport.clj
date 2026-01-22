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
(ns chrondb.storage.git.transport
  "SSH transport configuration for Git remote operations.
   Handles SshdSessionFactory initialization with ssh-agent support,
   enabling push/pull/clone operations over SSH."
  (:require [chrondb.util.logging :as log])
  (:import [java.io File]
           [org.eclipse.jgit.transport SshSessionFactory]
           [org.eclipse.jgit.transport.sshd SshdSessionFactoryBuilder]))

(defonce ^:private initialized? (atom false))

(defn- get-ssh-directory
  "Returns the user's SSH directory (~/.ssh)."
  []
  (File. (str (System/getProperty "user.home") "/.ssh")))

(defn- get-home-directory
  "Returns the user's home directory."
  []
  (File. (System/getProperty "user.home")))

(defn initialize-ssh!
  "Initializes the SSH transport layer for JGit.
   Configures SshdSessionFactory with ssh-agent support for passwordless
   authentication using keys managed by the system's SSH agent.

   This function is idempotent - calling it multiple times has no effect
   after the first successful initialization.

   Options map (all optional):
   - :ssh-dir     - Path to SSH directory (default: ~/.ssh)
   - :home-dir    - Path to home directory (default: user.home)
   - :auth-methods - Authentication methods (default: \"publickey\")"
  ([]
   (initialize-ssh! {}))
  ([{:keys [ssh-dir home-dir auth-methods]
     :or {auth-methods "publickey"}}]
   (when-not @initialized?
     (try
       (let [ssh-directory (or (when ssh-dir (File. ssh-dir))
                               (get-ssh-directory))
             home-directory (or (when home-dir (File. home-dir))
                                (get-home-directory))
             factory (-> (SshdSessionFactoryBuilder.)
                         (.setPreferredAuthentications auth-methods)
                         (.setHomeDirectory home-directory)
                         (.setSshDirectory ssh-directory)
                         (.build nil))]
         (SshSessionFactory/setInstance factory)
         (reset! initialized? true)
         (log/log-info (str "SSH transport initialized (ssh-dir: "
                            (.getAbsolutePath ssh-directory) ")")))
       (catch Exception e
         (log/log-error "Failed to initialize SSH transport" e)
         (throw (ex-info "SSH transport initialization failed"
                         {:cause (.getMessage e)} e)))))))

(defn initialized?-fn
  "Returns true if SSH transport has been initialized."
  []
  @initialized?)

(defn reset-ssh!
  "Resets SSH transport state. Useful for testing."
  []
  (reset! initialized? false)
  (SshSessionFactory/setInstance nil))

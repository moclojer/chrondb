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
(ns chrondb.core
  (:gen-class))

(def ^:private cli-ns "chrondb.cli.core")
(def ^:private server-ns "chrondb.cli.server")

(defn- requiring-var
  [namespace name]
  (requiring-resolve (symbol namespace name)))

(def ^:private cli-command-spec (delay @(requiring-var cli-ns "command-spec")))
(def ^:private cli-usage-fn (delay @(requiring-var cli-ns "usage")))
(def ^:private cli-main-fn (delay @(requiring-var cli-ns "-main")))

(def ^:private server-command-map (delay @(requiring-var server-ns "server-command-map")))
(def ^:private server-usage-fn (delay @(requiring-var server-ns "usage")))
(def ^:private server-parse-fn (delay @(requiring-var server-ns "parse-command")))
(def ^:private server-dispatch-fn (delay @(requiring-var server-ns "dispatch!")))

(defn- cli-command-set []
  (-> @cli-command-spec keys set))

(defn- server-command-set []
  (-> @server-command-map keys set))

(defn- cli-usage
  ([] ((@cli-usage-fn)))
  ([command] ((@cli-usage-fn) command)))

(defn- cli-main [& args]
  (apply (@cli-main-fn) args))

(defn- server-usage []
  ((@server-usage-fn)))

(defn- parse-server-command [args]
  ((@server-parse-fn) args))

(defn- dispatch-server! [command args]
  ((@server-dispatch-fn) command args))

(defn detect-mode
  [args]
  (let [[cmd & _] args
        cli-commands (cli-command-set)
        server-commands (server-command-set)]
    (cond
      (nil? cmd) :server
      (cli-commands cmd) :cli
      (server-commands cmd) :server
      (contains? #{"--help" "-h"} cmd) :help
      (some cli-commands args) :cli
      :else :server)))

(defn -main
  [& args]
  (case (detect-mode args)
    :help (do
            (println (server-usage))
            (println)
            (println (cli-usage)))
    :cli (apply cli-main args)
    (let [{:keys [command args value]} (parse-server-command args)]
      (if (= command :unknown)
        (do
          (println "Unknown command" value)
          (println (server-usage)))
        (dispatch-server! command args)))))
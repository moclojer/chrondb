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
  (:gen-class)
  (:require [chrondb.cli.core :as cli]
            [chrondb.cli.server :as server]
            [clojure.string :as str]))

(def cli-commands
  #{"init" "info" "get" "put" "history" "delete" "export" "import" "verify" "tail-history"})

(def server-commands
  (set (keys server/server-command-map)))

(defn detect-mode
  [args]
  (let [[cmd & _] args]
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
            (println (server/usage))
            (println)
            (println (cli/usage)))
    :cli (apply cli/-main args)
    (let [{:keys [command args value]} (server/parse-command args)]
      (if (= command :unknown)
        (do
          (println "Unknown command" value)
          (println (server/usage)))
        (server/dispatch! command args)))))
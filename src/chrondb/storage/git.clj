;; This file is part of ChronDB.
 ;;
 ;; ChronDB is free software: you can redistribute it and/or modify
 ;; it under the terms of the GNU General Public License as published
 ;; by the Free Software Foundation, either version 3 of the License,
 ;; or (at your option) any later version.
 ;;
 ;; ChronDB is distributed in the hope that it will be useful,
 ;; but WITHOUT ANY WARRANTY; without even the implied warranty of
 ;; MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 ;; GNU General Public License for more details.
 ;;
 ;; You should have received a copy of the GNU General Public License
 ;; along with this program. If not, see <https://www.gnu.org/licenses/>.
 (ns chrondb.storage.git
   "Git-based storage implementation for ChronDB.
   Uses JGit for Git operations and provides versioned document storage."
   (:require [chrondb.storage.git.core :as git]
             [chrondb.storage.git.path :as path]
             [chrondb.storage.git.commit :as commit]
             [chrondb.storage.git.history :as history]
             [chrondb.storage.git.document :as document]))

;; Re-export the main functions
(def create-git-storage git/create-git-storage)
(def get-document-history git/get-document-history)
(def restore-document-version git/restore-document-version)
(def get-document-at-commit history/get-document-at-commit)

;; For backward compatibility
(def ensure-directory git/ensure-directory)
(def create-repository git/create-repository)

;; Re-export GitStorage type
(def ->GitStorage git/->GitStorage)
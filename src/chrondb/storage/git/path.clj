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
(ns chrondb.storage.git.path
  "Path handling utilities for Git storage implementation"
  (:require [clojure.string :as str]
            [chrondb.util.logging :as log]
            [chrondb.config :as config]))

(defn encode-path
  "Encode document ID for safe use in file paths.
   Replaces characters that are problematic in file paths with underscores."
  [id]
  (-> id
      (str/replace ":" "_COLON_")
      (str/replace "/" "_SLASH_")
      (str/replace "?" "_QMARK_")
      (str/replace "*" "_STAR_")
      (str/replace "\\" "_BACKSLASH_")
      (str/replace "<" "_LT_")
      (str/replace ">" "_GT_")
      (str/replace "|" "_PIPE_")
      (str/replace "\"" "_QUOTE_")
      (str/replace "%" "_PERCENT_")
      (str/replace "#" "_HASH_")
      (str/replace "&" "_AMP_")
      (str/replace "=" "_EQUALS_")
      (str/replace "+" "_PLUS_")
      (str/replace "@" "_AT_")
      (str/replace " " "_SPACE_")))

(defn decode-path
  "Decode a path encoded by encode-path back to its original form.
   Reverses the replacements done by encode-path."
  [encoded-path]
  (-> encoded-path
      (str/replace "_COLON_" ":")
      (str/replace "_SLASH_" "/")
      (str/replace "_QMARK_" "?")
      (str/replace "_STAR_" "*")
      (str/replace "_BACKSLASH_" "\\")
      (str/replace "_LT_" "<")
      (str/replace "_GT_" ">")
      (str/replace "_PIPE_" "|")
      (str/replace "_QUOTE_" "\"")
      (str/replace "_PERCENT_" "%")
      (str/replace "_HASH_" "#")
      (str/replace "_AMP_" "&")
      (str/replace "_EQUALS_" "=")
      (str/replace "_PLUS_" "+")
      (str/replace "_AT_" "@")
      (str/replace "_SPACE_" " ")))

(defn get-table-path
  "Get the encoded path for a table directory"
  [table-name]
  (encode-path table-name))

(defn get-file-path
  "Get the file path for a document ID, with proper encoding.
   Organizes documents in table directories as per documentation.
   Ensures the path is valid for JGit by not starting with a slash."
  ([data-dir id]
   ; Backward compatibility - extract table from ID if in old format
   (let [parts (str/split (or id "") #":")
         table-name (if (> (count parts) 1)
                      (first parts)
                      "default")]
     (get-file-path data-dir id table-name)))
  ([data-dir id table-name]
   (let [encoded-id (encode-path id)
         encoded-table (get-table-path table-name)]
     (if (str/blank? data-dir)
       (str encoded-table "/" encoded-id ".json")
       (str data-dir
            (if (str/ends-with? data-dir "/") "" "/")
            encoded-table "/"
            encoded-id ".json")))))

(defn extract-table-and-id
  "Extract table name and clean ID from a potentially table-prefixed document ID"
  [id]
  (let [parts (str/split (or id "") #":")]
    (if (> (count parts) 1)
      [(first parts) (second parts)]
      [nil id])))
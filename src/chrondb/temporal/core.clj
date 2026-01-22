;; This file is part of ChronDB.
;;
;; ChronDB is free software: you can redistribute it and/or modify
;; it under the terms of the GNU Affero General Public License as published
;; by the Free Software Foundation, either version 3 of the License,
;; or (at your option) any later version.
(ns chrondb.temporal.core
  "Temporal query support for ChronDB.
   Implements SQL:2011 temporal query syntax."
  (:require [clojure.string :as str])
  (:import [java.time Instant ZonedDateTime ZoneOffset]
           [java.time.format DateTimeFormatter DateTimeParseException]))

;; Temporal query types
(def ^:const TEMPORAL-AS-OF :as-of)
(def ^:const TEMPORAL-BETWEEN :between)
(def ^:const TEMPORAL-FROM-TO :from-to)
(def ^:const TEMPORAL-CONTAINED-IN :contained-in)
(def ^:const TEMPORAL-VERSIONS :versions)

;; Timestamp parsing
(def ^:private datetime-formatter DateTimeFormatter/ISO_DATE_TIME)
(def ^:private date-formatter DateTimeFormatter/ISO_DATE)

(defn parse-timestamp
  "Parse a timestamp string into an Instant.
   Supports ISO-8601, Unix epoch (seconds and milliseconds), and common formats."
  [s]
  (cond
    (nil? s) nil
    (instance? Instant s) s
    (number? s) (if (> s 10000000000)
                  (Instant/ofEpochMilli s)
                  (Instant/ofEpochSecond s))
    (string? s)
    (let [s (str/trim s)]
      (cond
        ;; Unix timestamp as string
        (re-matches #"\d{10,13}" s)
        (let [ts (Long/parseLong s)]
          (if (> ts 10000000000)
            (Instant/ofEpochMilli ts)
            (Instant/ofEpochSecond ts)))

        ;; Try ISO instant first (2024-01-15T10:30:00Z)
        :else
        (try
          (Instant/parse s)
          (catch DateTimeParseException _
            (try
              ;; Try with timezone (2024-01-15T10:30:00+00:00)
              (.toInstant (ZonedDateTime/parse s datetime-formatter))
              (catch DateTimeParseException _
                (try
                  ;; Try date only (2024-01-15)
                  (.toInstant (.atStartOfDay (java.time.LocalDate/parse s date-formatter)
                                             ZoneOffset/UTC))
                  (catch DateTimeParseException e
                    (throw (ex-info "Invalid timestamp format"
                                    {:input s
                                     :supported-formats ["ISO-8601" "Unix epoch" "YYYY-MM-DD"]}
                                    e))))))))))
    :else (throw (ex-info "Cannot parse timestamp" {:input s :type (type s)}))))

(defn format-timestamp
  "Format an Instant as ISO-8601 string."
  [^Instant instant]
  (when instant
    (.toString instant)))

(defn timestamp->epoch-ms
  "Convert timestamp to epoch milliseconds."
  [ts]
  (when-let [instant (parse-timestamp ts)]
    (.toEpochMilli instant)))

;; Temporal query structures
(defn as-of-query
  "Create an AS OF SYSTEM TIME query specification."
  [timestamp]
  {:type TEMPORAL-AS-OF
   :timestamp (parse-timestamp timestamp)})

(defn between-query
  "Create a FOR SYSTEM_TIME BETWEEN query specification."
  [start-time end-time & {:keys [include-start include-end]
                          :or {include-start true include-end true}}]
  {:type TEMPORAL-BETWEEN
   :start (parse-timestamp start-time)
   :end (parse-timestamp end-time)
   :include-start include-start
   :include-end include-end})

(defn from-to-query
  "Create a FOR SYSTEM_TIME FROM...TO query specification."
  [start-time end-time]
  {:type TEMPORAL-FROM-TO
   :start (parse-timestamp start-time)
   :end (parse-timestamp end-time)})

(defn versions-query
  "Create a VERSIONS BETWEEN query specification for document history."
  [start-time end-time]
  {:type TEMPORAL-VERSIONS
   :start (parse-timestamp start-time)
   :end (parse-timestamp end-time)})

;; Temporal resolution
(defn find-commit-at-timestamp
  "Find the commit that was current at a given timestamp.
   Returns the most recent commit before or at the timestamp."
  [commits timestamp]
  (let [target-instant (parse-timestamp timestamp)]
    (->> commits
         (filter (fn [commit]
                   (let [commit-time (parse-timestamp (:commit-time commit))]
                     (and commit-time
                          (not (.isAfter commit-time target-instant))))))
         (sort-by #(.toEpochMilli (parse-timestamp (:commit-time %))))
         last)))

(defn filter-commits-in-range
  "Filter commits that fall within a time range."
  [commits {:keys [start end include-start include-end]
            :or {include-start true include-end true}}]
  (let [start-instant (parse-timestamp start)
        end-instant (parse-timestamp end)
        start-check (if include-start
                      (fn [t] (not (.isBefore t start-instant)))
                      (fn [t] (.isAfter t start-instant)))
        end-check (if include-end
                    (fn [t] (not (.isAfter t end-instant)))
                    (fn [t] (.isBefore t end-instant)))]
    (->> commits
         (filter (fn [commit]
                   (let [commit-time (parse-timestamp (:commit-time commit))]
                     (and commit-time
                          (start-check commit-time)
                          (end-check commit-time)))))
         (sort-by #(.toEpochMilli (parse-timestamp (:commit-time %)))))))

;; Bi-temporal support
(defn valid-time-fields
  "Extract valid time fields from a document."
  [doc]
  {:valid-from (some-> doc :_valid_from parse-timestamp)
   :valid-to (some-> doc :_valid_to parse-timestamp)})

(defn document-valid-at?
  "Check if a document is valid at a given time point."
  [doc timestamp]
  (let [{:keys [valid-from valid-to]} (valid-time-fields doc)
        ts (parse-timestamp timestamp)]
    (and (or (nil? valid-from) (not (.isAfter valid-from ts)))
         (or (nil? valid-to) (.isAfter valid-to ts)))))

(defn filter-by-valid-time
  "Filter documents by valid time."
  [docs timestamp]
  (filter #(document-valid-at? % timestamp) docs))

;; Temporal metadata enrichment
(defn enrich-with-temporal-metadata
  "Add temporal metadata to a document from commit info."
  [doc commit]
  (assoc doc
         :_system_time (:commit-time commit)
         :_system_version (:commit-id commit)
         :_system_committer (:committer-name commit)))

(defn enrich-history-with-metadata
  "Enrich a sequence of historical documents with temporal metadata."
  [history]
  (map (fn [{:keys [document] :as entry}]
         (enrich-with-temporal-metadata document entry))
       history))

;; SQL temporal clause parsing helpers
(defn parse-temporal-clause
  "Parse a temporal clause from SQL tokens.
   Supports:
   - AS OF SYSTEM TIME 'timestamp'
   - FOR SYSTEM_TIME BETWEEN 'start' AND 'end'
   - FOR SYSTEM_TIME FROM 'start' TO 'end'
   - VERSIONS BETWEEN 'start' AND 'end'"
  [tokens]
  (let [tokens (map str/upper-case tokens)]
    (cond
      ;; AS OF SYSTEM TIME 'timestamp'
      (and (>= (count tokens) 4)
           (= (take 4 tokens) ["AS" "OF" "SYSTEM" "TIME"]))
      (let [ts-str (nth tokens 4 nil)]
        (when ts-str
          (as-of-query (str/replace ts-str #"['\"]" ""))))

      ;; FOR SYSTEM_TIME BETWEEN ... AND ...
      (and (>= (count tokens) 6)
           (= (take 3 tokens) ["FOR" "SYSTEM_TIME" "BETWEEN"]))
      (let [start-idx 3
            and-idx (.indexOf (vec tokens) "AND")
            start-ts (when (> and-idx start-idx)
                       (str/join " " (subvec (vec tokens) start-idx and-idx)))
            end-ts (when (< (inc and-idx) (count tokens))
                     (str/join " " (subvec (vec tokens) (inc and-idx))))]
        (when (and start-ts end-ts)
          (between-query (str/replace start-ts #"['\"]" "")
                         (str/replace end-ts #"['\"]" ""))))

      ;; FOR SYSTEM_TIME FROM ... TO ...
      (and (>= (count tokens) 6)
           (= (take 3 tokens) ["FOR" "SYSTEM_TIME" "FROM"]))
      (let [start-idx 3
            to-idx (.indexOf (vec tokens) "TO")
            start-ts (when (> to-idx start-idx)
                       (str/join " " (subvec (vec tokens) start-idx to-idx)))
            end-ts (when (< (inc to-idx) (count tokens))
                     (str/join " " (subvec (vec tokens) (inc to-idx))))]
        (when (and start-ts end-ts)
          (from-to-query (str/replace start-ts #"['\"]" "")
                         (str/replace end-ts #"['\"]" ""))))

      ;; VERSIONS BETWEEN ... AND ...
      (and (>= (count tokens) 4)
           (= (take 2 tokens) ["VERSIONS" "BETWEEN"]))
      (let [start-idx 2
            and-idx (.indexOf (vec tokens) "AND")
            start-ts (when (> and-idx start-idx)
                       (str/join " " (subvec (vec tokens) start-idx and-idx)))
            end-ts (when (< (inc and-idx) (count tokens))
                     (str/join " " (subvec (vec tokens) (inc and-idx))))]
        (when (and start-ts end-ts)
          (versions-query (str/replace start-ts #"['\"]" "")
                          (str/replace end-ts #"['\"]" ""))))

      :else nil)))

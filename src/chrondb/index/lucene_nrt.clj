;; This file is part of ChronDB.
;;
;; ChronDB is free software: you can redistribute it and/or modify
;; it under the terms of the GNU Affero General Public License as published
;; by the Free Software Foundation, either version 3 of the License,
;; or (at your option) any later version.
(ns chrondb.index.lucene-nrt
  "Near Real-Time Lucene index with batch commits for improved performance.
   This module provides optimized indexing with configurable batch sizes and
   refresh intervals."
  (:require [chrondb.index.protocol :as protocol]
            [chrondb.util.logging :as log]
            [clojure.string :as str])
  (:import [org.apache.lucene.store FSDirectory]
           [org.apache.lucene.analysis.standard StandardAnalyzer]
           [org.apache.lucene.document Document StringField TextField Field$Store
            LongPoint DoublePoint NumericDocValuesField DoubleDocValuesField]
           [org.apache.lucene.index IndexWriter IndexWriterConfig IndexWriterConfig$OpenMode
            DirectoryReader Term IndexNotFoundException]
           [org.apache.lucene.search IndexSearcher ScoreDoc MatchAllDocsQuery
            TermQuery WildcardQuery TopScoreDocCollector BooleanQuery$Builder BooleanClause$Occur]
           [org.apache.lucene.queryparser.classic QueryParser]
           [java.nio.file Paths]
           [java.util.concurrent.atomic AtomicBoolean]
           [java.util.concurrent Executors TimeUnit ScheduledExecutorService]))

;; Configuration defaults
(def ^:private default-config
  {:batch-size 100           ; Documents before auto-commit
   :max-uncommitted-ms 1000  ; Max time before forced commit
   :refresh-interval-ms 1000 ; NRT refresh interval
   :ram-buffer-mb 256.0      ; RAM buffer size
   :max-buffered-docs 10000}) ; Max docs in memory

(defn- parse-long-safe [s]
  (try (Long/parseLong s) (catch Exception _ nil)))

(defn- parse-double-safe [s]
  (try (Double/parseDouble s) (catch Exception _ nil)))

(defn- create-lucene-doc
  "Creates a Lucene Document from a Clojure map."
  [doc]
  (let [ldoc (Document.)]
    (.add ldoc (StringField. "id" (:id doc) Field$Store/YES))
    (doseq [[k v] (dissoc doc :id)]
      (when v
        (let [field-name (name k)
              field-value (str v)
              long-val (parse-long-safe field-value)
              double-val (when (nil? long-val) (parse-double-safe field-value))]
          (.add ldoc (TextField. field-name field-value Field$Store/YES))
          (cond
            long-val
            (do
              (.add ldoc (LongPoint. field-name (long-array [long-val])))
              (.add ldoc (NumericDocValuesField. field-name long-val)))
            double-val
            (do
              (.add ldoc (DoublePoint. field-name (double-array [double-val])))
              (.add ldoc (DoubleDocValuesField. field-name double-val))))
          ;; Add FTS field for searchable text fields
          (when (or (= field-name "name")
                    (= field-name "description")
                    (= field-name "content")
                    (= field-name "text")
                    (str/ends-with? field-name "_fts"))
            (.add ldoc (TextField. (str field-name "_fts") field-value Field$Store/YES))))))
    ldoc))

(defprotocol NRTIndex
  "Extended protocol for NRT index operations."
  (force-refresh! [this] "Force an immediate NRT refresh")
  (force-commit! [this] "Force an immediate commit")
  (get-stats [this] "Get index statistics")
  (set-config! [this config] "Update configuration"))

(defrecord LuceneNRTIndex [directory writer
                           reader-atom searcher-atom
                           config-atom
                           pending-docs-atom
                           last-commit-time-atom
                           commit-scheduled?-atom
                           ^ScheduledExecutorService scheduler
                           running?-atom]

  protocol/Index

  (index-document [this doc]
    (try
      (let [lucene-doc (create-lucene-doc doc)]
        ;; Add document without immediate commit
        (.addDocument writer lucene-doc)
        (let [pending (swap! pending-docs-atom inc)
              config @config-atom]
          ;; Schedule commit if batch size reached
          (when (>= pending (:batch-size config))
            (force-commit! this))))
      (catch Exception e
        (log/log-error (str "Error indexing document " (:id doc) ": " (.getMessage e))))))

  (delete-document [this id]
    (try
      (.deleteDocuments writer (into-array Term [(Term. "id" id)]))
      (swap! pending-docs-atom inc)
      (let [pending @pending-docs-atom
            config @config-atom]
        (when (>= pending (:batch-size config))
          (force-commit! this)))
      true
      (catch Exception e
        (log/log-error (str "Error deleting document " id ": " (.getMessage e)))
        false)))

  (search [_ field query-string _branch]
    (try
      (when-let [_reader @reader-atom]
        (let [searcher @searcher-atom
              analyzer (StandardAnalyzer.)
              search-field (if (str/ends-with? field "_fts") field (str field "_fts"))
              parser (doto (QueryParser. search-field analyzer)
                       (.setAllowLeadingWildcard true))
              normalized-query (-> query-string
                                   (java.text.Normalizer/normalize java.text.Normalizer$Form/NFD)
                                   (str/replace #"[\p{InCombiningDiacriticalMarks}]" "")
                                   (str/lower-case))
              lucene-term (cond
                            (or (str/includes? normalized-query "*")
                                (str/includes? normalized-query "?"))
                            normalized-query
                            (< (count normalized-query) 4)
                            (str "*" normalized-query "*")
                            :else
                            (str normalized-query "*"))
              query (.parse parser lucene-term)
              top-docs (.search searcher query 100)]
          (mapv #(.get (.doc searcher (.-doc ^ScoreDoc %)) "id")
                (.-scoreDocs top-docs))))
      (catch Exception e
        (log/log-error (str "Error searching: " (.getMessage e)))
        [])))

  (search-query [_ query-map _branch opts]
    (try
      (when-let [_reader @reader-atom]
        (let [searcher @searcher-atom
              limit (or (:limit opts) (:limit query-map) 100)
              offset (or (:offset opts) (:offset query-map) 0)
              ;; Build Lucene query from AST
              query (if-let [clauses (:clauses query-map)]
                      (let [builder (BooleanQuery$Builder.)]
                        (doseq [clause clauses]
                          (case (:type clause)
                            :term
                            (.add builder (TermQuery. (Term. (:field clause) (str (:value clause))))
                                  BooleanClause$Occur/MUST)
                            :wildcard
                            (.add builder (WildcardQuery. (Term. (:field clause) (str/lower-case (:value clause))))
                                  BooleanClause$Occur/MUST)
                            :match-all
                            (.add builder (MatchAllDocsQuery.) BooleanClause$Occur/MUST)
                            nil))
                        (.build builder))
                      (MatchAllDocsQuery.))
              collector (TopScoreDocCollector/create (+ limit offset) Integer/MAX_VALUE)]
          (.search searcher query collector)
          (let [top-docs (.topDocs collector offset limit)
                ids (mapv #(.get (.doc searcher (.-doc ^ScoreDoc %)) "id")
                          (.-scoreDocs top-docs))]
            {:ids ids
             :total (.-totalHits top-docs)
             :limit limit
             :offset offset})))
      (catch Exception e
        (log/log-error (str "Error in search-query: " (.getMessage e)))
        {:ids [] :total 0 :limit 0 :offset 0})))

  (close [this]
    (log/log-info "Closing NRT Lucene index...")
    (reset! running?-atom false)
    (.shutdown scheduler)
    (try (.awaitTermination scheduler 5 TimeUnit/SECONDS) (catch Exception _))
    (force-commit! this)
    (try (.close writer) (catch Exception e (log/log-error (str "Error closing writer: " (.getMessage e)))))
    (try (when-let [r @reader-atom] (.close r)) (catch Exception _))
    (try (.close directory) (catch Exception _))
    (log/log-info "NRT Lucene index closed."))

  NRTIndex

  (force-refresh! [_]
    (try
      (when-let [current-reader @reader-atom]
        (when-let [new-reader (DirectoryReader/openIfChanged current-reader writer)]
          (let [old-reader current-reader]
            (reset! reader-atom new-reader)
            (reset! searcher-atom (IndexSearcher. new-reader))
            ;; Close old reader in background after grace period
            (future
              (Thread/sleep 5000)
              (try (.close old-reader) (catch Exception _))))))
      (catch Exception e
        (log/log-warn (str "NRT refresh failed: " (.getMessage e))))))

  (force-commit! [this]
    (when (compare-and-set! commit-scheduled?-atom false true)
      (try
        (let [pending @pending-docs-atom]
          (when (pos? pending)
            (.commit writer)
            (reset! pending-docs-atom 0)
            (reset! last-commit-time-atom (System/currentTimeMillis))
            (force-refresh! this)
            (log/log-info (str "Committed " pending " documents"))))
        (finally
          (reset! commit-scheduled?-atom false)))))

  (get-stats [_]
    (let [config @config-atom]
      {:pending-docs @pending-docs-atom
       :last-commit-time @last-commit-time-atom
       :num-docs (when-let [r @reader-atom] (.numDocs r))
       :config config}))

  (set-config! [_ new-config]
    (swap! config-atom merge new-config)))

(defn- start-background-tasks!
  "Start background tasks for periodic commit and refresh."
  [index]
  (let [{:keys [scheduler config-atom pending-docs-atom
                last-commit-time-atom running?-atom]} index]
    ;; Periodic commit check
    (.scheduleWithFixedDelay
     scheduler
     (fn []
       (when @running?-atom
         (try
           (let [config @config-atom
                 pending @pending-docs-atom
                 last-commit @last-commit-time-atom
                 now (System/currentTimeMillis)
                 elapsed (- now last-commit)]
             (when (and (pos? pending)
                        (> elapsed (:max-uncommitted-ms config)))
               (force-commit! index)))
           (catch Exception e
             (log/log-error (str "Background commit failed: " (.getMessage e)))))))
     100 100 TimeUnit/MILLISECONDS)

    ;; Periodic NRT refresh
    (.scheduleWithFixedDelay
     scheduler
     (fn []
       (when @running?-atom
         (try
           (force-refresh! index)
           (catch Exception e
             (log/log-warn (str "Background refresh failed: " (.getMessage e)))))))
     1000 (:refresh-interval-ms @config-atom) TimeUnit/MILLISECONDS)))

(defn create-nrt-index
  "Create a new NRT Lucene index with batch commits.

   Options:
   - :batch-size         - Documents before auto-commit (default 100)
   - :max-uncommitted-ms - Max time before forced commit (default 1000)
   - :refresh-interval-ms - NRT refresh interval (default 1000)
   - :ram-buffer-mb      - RAM buffer size (default 256.0)
   - :max-buffered-docs  - Max docs in memory (default 10000)"
  [index-dir & {:as opts}]
  (try
    (let [config (merge default-config opts)
          path (Paths/get index-dir (into-array String []))
          _ (log/log-info (str "Creating NRT Lucene index at: " path))
          directory (FSDirectory/open path)
          analyzer (StandardAnalyzer.)
          writer-config (doto (IndexWriterConfig. analyzer)
                          (.setOpenMode IndexWriterConfig$OpenMode/CREATE_OR_APPEND)
                          (.setRAMBufferSizeMB (:ram-buffer-mb config))
                          (.setMaxBufferedDocs (:max-buffered-docs config)))
          writer (IndexWriter. directory writer-config)
          reader (try
                   (DirectoryReader/open writer)
                   (catch IndexNotFoundException _
                     (log/log-warn "Creating new NRT index")
                     nil))
          searcher (when reader (IndexSearcher. reader))
          scheduler (Executors/newScheduledThreadPool 2)
          index (->LuceneNRTIndex
                 directory writer
                 (atom reader) (atom searcher)
                 (atom config)
                 (atom 0)
                 (atom (System/currentTimeMillis))
                 (AtomicBoolean. false)
                 scheduler
                 (atom true))]
      (start-background-tasks! index)
      (log/log-info "NRT Lucene index created successfully")
      index)
    (catch Exception e
      (log/log-error (str "Failed to create NRT index: " (.getMessage e)))
      nil)))

;; Utility functions
(defn ensure-searchable!
  "Ensure all pending documents are searchable.
   Use before queries that need strong consistency."
  [index]
  (force-commit! index))

(defn index-batch!
  "Index a batch of documents efficiently."
  [index docs]
  (doseq [doc docs]
    (protocol/index-document index doc))
  (force-commit! index)
  (count docs))

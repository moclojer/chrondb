(ns chrondb.index.lucene
  (:require [chrondb.index.protocol :as protocol]
            [clojure.data.json :as json]
            [clojure.string :as str]
            [chrondb.util.logging :as log])
  (:import [org.apache.lucene.store Directory FSDirectory]
           [org.apache.lucene.analysis Analyzer CharArraySet]
           [org.apache.lucene.analysis.standard StandardAnalyzer]
           [org.apache.lucene.analysis.pt PortugueseAnalyzer]
           [org.apache.lucene.analysis.br BrazilianAnalyzer]
           [org.apache.lucene.document Document StringField TextField Field$Store]
           [org.apache.lucene.index IndexWriter IndexWriterConfig IndexWriterConfig$OpenMode DirectoryReader Term IndexNotFoundException]
           [org.apache.lucene.search IndexSearcher Query ScoreDoc TopDocs]
           [org.apache.lucene.queryparser.classic QueryParser ParseException]
           [java.nio.file Paths]
           [java.io Closeable]))

(defn- create-lucene-doc
  "Creates a Lucene Document from a Clojure map, using different analyzers."
  [doc ^Analyzer fts-analyzer ^Analyzer default-analyzer]
  (let [ldoc (Document.)]
    (.add ldoc (StringField. "id" (:id doc) Field$Store/YES))
    (doseq [[k v] (dissoc doc :id)]
      (when v
        (let [field-name (name k)
              field-value (str v)
              ;; Normalize value - remove accents for text fields
              normalized-value (-> field-value
                                   (java.text.Normalizer/normalize java.text.Normalizer$Form/NFD)
                                   (str/replace #"[\p{InCombiningDiacriticalMarks}]" ""))]
          ;; Add as normal field with original value
          (.add ldoc (TextField. field-name field-value Field$Store/YES))

          ;; For fields that can be searched via FTS (like name, description, etc.)
          ;; create an additional field with _fts suffix using normalized value
          (when (or (= field-name "name")
                    (= field-name "description")
                    (= field-name "content")
                    (= field-name "text")
                    (= field-name "location")
                    (str/ends-with? field-name "_fts"))
            ;; Create a copy of the field for FTS with normalized value
            (.add ldoc (TextField. (str field-name "_fts") normalized-value Field$Store/YES))))))
    ldoc))

(defn- doc->map
  "Converts a Lucene Document back to a Clojure map by taking the 'id' field and looking up the original in storage.
   NOTE: This function is now UNUSED because the search will return only IDs."
  [^Document doc]
  {:id (.get doc "id")})

(defn- refresh-reader
  "Attempts to get a new IndexReader if the index has been modified."
  [^DirectoryReader reader ^IndexWriter writer]
  (let [new-reader (DirectoryReader/openIfChanged reader writer)]
    (if new-reader
      (do
        (.close reader)
        new-reader)
      reader)))

(defrecord LuceneIndex [^Directory directory
                        ^Analyzer fts-analyzer
                        ^Analyzer default-analyzer
                        ^IndexWriter writer
                        reader-atom
                        searcher-atom]
  protocol/Index
  (index-document [this doc]
    (try
      (let [lucene-doc (create-lucene-doc doc fts-analyzer default-analyzer)
            id-term (Term. "id" (:id doc))]
        (.updateDocument writer id-term lucene-doc)
        (.commit writer)
        (let [current-reader @reader-atom
              new-reader (refresh-reader current-reader writer)]
          (when-not (identical? new-reader current-reader)
            (reset! reader-atom new-reader)
            (reset! searcher-atom (IndexSearcher. new-reader)))))
      doc
      (catch Exception e
        (log/log-error (str "Error indexing document " (:id doc) ": " (.getMessage e)))
        nil)))

  (delete-document [this id]
    (try
      (.deleteDocuments writer (into-array Term [(Term. "id" id)]))
      (.commit writer)
      (let [current-reader @reader-atom
            new-reader (refresh-reader current-reader writer)]
        (when-not (identical? new-reader current-reader)
          (reset! reader-atom new-reader)
          (reset! searcher-atom (IndexSearcher. new-reader))))
      true
      (catch Exception e
        (log/log-error (str "Error deleting document " id ": " (.getMessage e)))
        false)))

  (search [this field query-string branch]
    (log/log-info (str "Starting Lucene search - Field: " field ", Query: '" query-string "' Branch: " branch))
    (try
      (if-let [^DirectoryReader reader @reader-atom]
        (let [^IndexSearcher searcher @searcher-atom
              analyzer (if (str/ends-with? field "_fts") fts-analyzer default-analyzer)
              parser (QueryParser. field analyzer)
              max-results 100]
          (try
            ;; Configure parser to allow wildcards at the beginning (like *term)
            (.setAllowLeadingWildcard parser true)

            ;; Adjust the maximum number of clauses for queries with many wildcards
            (when (or (str/includes? query-string "*") (str/includes? query-string "?"))
              (try
                (let [max-clause-class (Class/forName "org.apache.lucene.search.BooleanQuery")
                      max-clause-method (.getDeclaredMethod max-clause-class "setMaxClauseCount" (into-array Class [Integer/TYPE]))]
                  (.invoke max-clause-method nil (object-array [4096])))
                (catch Exception e
                  (log/log-warn (str "Could not adjust MaxClauseCount: " (.getMessage e))))))

            (let [;; Normalize the search term
                  normalized-query (-> query-string
                                       (java.text.Normalizer/normalize java.text.Normalizer$Form/NFD)
                                       (str/replace #"[\p{InCombiningDiacriticalMarks}]" "")
                                       (str/lower-case))
                  _ (log/log-info (str "Normalized term: '" normalized-query "'"))

                  ;; Add wildcards as needed
                  lucene-term (cond
                                ;; If already has wildcards, keep as is
                                (or (str/includes? normalized-query "*")
                                    (str/includes? normalized-query "?"))
                                normalized-query

                                ;; If it's a short term, consider it as part of a word
                                (< (count normalized-query) 4)
                                (str "*" normalized-query "*")

                                ;; Otherwise, add * at the end for prefix search
                                :else (str normalized-query "*"))
                  _ (log/log-info (str "Term adapted for Lucene: '" lucene-term "'"))

                  ^Query lucene-query (.parse parser lucene-term)
                  ^TopDocs top-docs (.search searcher lucene-query max-results)]
              (log/log-info (str "Lucene query: " lucene-query " found: " (.-totalHits top-docs) " hits."))
              (mapv #(.get (.doc searcher (.-doc ^ScoreDoc %)) "id") (.-scoreDocs top-docs)))
            (catch ParseException pe
              (log/log-error (str "Error parsing FTS query: '" query-string "' on field '" field "' - " (.getMessage pe)))
              [])
            (catch Exception e
              (log/log-error (str "Error in Lucene search for query '" query-string "' on field '" field "': " (.getMessage e)))
              [])))
        (do (log/log-warn "IndexReader not available for search.")
            []))
      (catch IndexNotFoundException _
        (log/log-info "Lucene index not found, returning empty search.")
        [])
      (catch Exception e
        (log/log-error (str "Unexpected error trying to search the index: " (.getMessage e)))
        [])))

  Closeable
  (close [this]
    (log/log-info "Closing Lucene index...")
    (try (.close writer) (catch Exception e (log/log-error (str "Error closing IndexWriter: " (.getMessage e)))))
    (try (.close ^DirectoryReader @reader-atom) (catch Exception e (log/log-error (str "Error closing DirectoryReader: " (.getMessage e)))))
    (try (.close directory) (catch Exception e (log/log-error (str "Error closing Directory: " (.getMessage e)))))
    (log/log-info "Lucene index closed.")))

(defn create-lucene-index
  "Creates a new instance of the LuceneIndex component."
  [index-dir]
  (try
    (let [path (Paths/get index-dir (into-array String []))
          _ (log/log-info (str "Opening/creating Lucene index directory at: " path))
          directory (FSDirectory/open path)
          ;; Use StandardAnalyzer for both normal and FTS
          ;; We normalize terms manually during indexing and search
          standard-analyzer (StandardAnalyzer.)
          config (doto (IndexWriterConfig. standard-analyzer)
                   (.setOpenMode IndexWriterConfig$OpenMode/CREATE_OR_APPEND))
          writer (IndexWriter. directory config)
          reader (try (DirectoryReader/open writer)
                      (catch IndexNotFoundException _
                        (log/log-warn "Creating new Lucene index, as none was found.")
                        nil)
                      (catch Exception e
                        (log/log-error (str "Error opening initial DirectoryReader: " (.getMessage e)))
                        nil))
          searcher (when reader (IndexSearcher. reader))]
      (log/log-info "LuceneIndex instance created with StandardAnalyzer and manual accent normalization.")
      (->LuceneIndex directory standard-analyzer standard-analyzer writer (atom reader) (atom searcher)))
    (catch Exception e
      (log/log-error (str "Failed to create LuceneIndex component in '" index-dir "': " (.getMessage e)))
      nil)))
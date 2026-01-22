(ns chrondb.index.lucene
  (:require [chrondb.index.protocol :as protocol]
            [chrondb.storage.protocol :as storage]
            [clojure.string :as str]
            [chrondb.util.logging :as log])
  (:import [org.apache.lucene.store Directory FSDirectory]
           [org.apache.lucene.analysis Analyzer]
           [org.apache.lucene.analysis.standard StandardAnalyzer]
           [org.apache.lucene.document Document StringField TextField Field$Store LongPoint DoublePoint NumericDocValuesField DoubleDocValuesField]
           [org.apache.lucene.index IndexWriter IndexWriterConfig IndexWriterConfig$OpenMode DirectoryReader Term IndexNotFoundException]
           [org.apache.lucene.search IndexSearcher Query ScoreDoc TopDocs BooleanQuery$Builder BooleanClause$Occur MatchAllDocsQuery TermQuery WildcardQuery TermRangeQuery Sort SortField SortField$Type TopFieldCollector TopScoreDocCollector]
           [org.apache.lucene.queryparser.classic QueryParser ParseException]
           [java.nio.file Paths]))

(defn- normalize-field
  "Normalizes a field name for full-text search support."
  [field]
  (let [field-name (name field)]
    (if (str/ends-with? field-name "_fts")
      field-name
      (str field-name "_fts"))))

(defn- sanitize-sort-field
  [{:keys [field direction type]}]
  (let [sort-field (name field)
        direction (if (= :desc direction) :desc :asc)
        requested-type (keyword type)
        detected-type (cond
                        (and requested-type (not= requested-type :string)) requested-type
                        (or (= sort-field "age")
                            (= sort-field "price")
                            (= sort-field "count")
                            (= sort-field "score")
                            (= sort-field "timestamp")
                            (str/ends-with? sort-field "_id")
                            (str/ends-with? sort-field "_count")
                            (str/ends-with? sort-field "_score")) :long
                        :else :string)
        type detected-type]
    {:field sort-field
     :direction direction
     :type (keyword type)}))

(defn- sort-field->lucene
  [{:keys [field direction type]}]
  (let [reverse (= :desc direction)]
    (case type
      :score (SortField. SortField/FIELD_SCORE reverse)
      :doc   (SortField. SortField/FIELD_DOC reverse)
      :long  (SortField. field SortField$Type/LONG reverse)
      :double (SortField. field SortField$Type/DOUBLE reverse)
      (SortField. field SortField$Type/STRING reverse))))

(defn- build-sort
  [sort-descriptors]
  (when (seq sort-descriptors)
    (->> sort-descriptors
         (map sanitize-sort-field)
         (map sort-field->lucene)
         (into-array SortField)
         Sort.)))

(defn- parse-long-safe [s]
  (try
    (Long/parseLong s)
    (catch Exception _ nil)))

(defn- parse-double-safe [s]
  (try
    (Double/parseDouble s)
    (catch Exception _ nil)))

(defn- create-lucene-doc
  "Creates a Lucene Document from a Clojure map, using different analyzers."
  [doc _]
  (let [ldoc (Document.)]
    (.add ldoc (StringField. "id" (:id doc) Field$Store/YES))
    (doseq [[k v] (dissoc doc :id)]
      (when v
        (let [field-name (name k)
              field-value (str v)
              long-val (parse-long-safe field-value)
              double-val (when (nil? long-val)
                           (parse-double-safe field-value))]
          (.add ldoc (TextField. field-name field-value Field$Store/YES))

          (cond
            long-val
            (let [long-arr (long-array [long-val])]
              (.add ldoc (LongPoint. field-name long-arr))
              (.add ldoc (NumericDocValuesField. field-name long-val)))

            double-val
            (let [double-arr (double-array [double-val])]
              (.add ldoc (DoublePoint. field-name double-arr))
              (.add ldoc (DoubleDocValuesField. field-name double-val))))

          (when (or (= field-name "name")
                    (= field-name "description")
                    (= field-name "content")
                    (= field-name "text")
                    (= field-name "location")
                    (str/ends-with? field-name "_fts"))
            (.add ldoc (TextField. (normalize-field field-name) field-value Field$Store/YES))))))
    ldoc))

(defmulti ast->query (fn [_ clause] (:type clause)))

(def ^:dynamic *lucene-context* nil)

(defn- current-analyzer
  ([] (:default-analyzer *lucene-context*))
  ([preferred]
   (or preferred (:default-analyzer *lucene-context*))))

(defn- current-fts-analyzer []
  (:fts-analyzer *lucene-context*))

(defmethod ast->query :match-all
  [_ _]
  (MatchAllDocsQuery.))

(defmethod ast->query :term
  [_ {:keys [field value]}]
  (when (and field value)
    (TermQuery. (Term. field (str value)))))

(defmethod ast->query :wildcard
  [_ {:keys [field value]}]
  (when (and field value)
    (WildcardQuery. (Term. field (str/lower-case value)))))

(defmethod ast->query :fts
  [_ {:keys [field value analyzer]}]
  (let [normalized-field (normalize-field field)
        actual-analyzer (if (instance? org.apache.lucene.analysis.Analyzer analyzer)
                          analyzer
                          (current-fts-analyzer))
        parser (QueryParser. normalized-field (or actual-analyzer (current-analyzer)))]
    (.setAllowLeadingWildcard parser true)
    (try
      (.parse parser (str value))
      (catch Exception e
        (log/log-error (str "Error creating FTS query for field " normalized-field ": " (.getMessage e)))
        (throw e)))))

(defmethod ast->query :range
  [_ {:keys [field lower upper include-lower? include-upper? value-type]}]
  (let [lower (when lower (str lower))
        upper (when upper (str upper))]
    (case value-type
      :long (TermRangeQuery. field lower upper (boolean include-lower?) (boolean include-upper?))
      :double (TermRangeQuery. field lower upper (boolean include-lower?) (boolean include-upper?))
      (TermRangeQuery. field lower upper (boolean include-lower?) (boolean include-upper?)))))

(defmethod ast->query :exists
  [_ {:keys [field]}]
  ;; Use TermRangeQuery with open bounds to match any value
  ;; This works for both string and text fields, unlike DocValuesFieldExistsQuery
  ;; which only works for fields with doc values (numeric fields)
  (TermRangeQuery. field nil nil true true))

(defmethod ast->query :missing
  [_ {:keys [field]}]
  ;; Match all documents that do NOT have any value in the field
  (let [builder (BooleanQuery$Builder.)]
    (.add builder (MatchAllDocsQuery.) BooleanClause$Occur/MUST)
    (.add builder (TermRangeQuery. field nil nil true true) BooleanClause$Occur/MUST_NOT)
    (.build builder)))

(defmethod ast->query :boolean
  [ast {:keys [must should must-not filter]}]
  (let [builder (BooleanQuery$Builder.)]
    (doseq [clause must]
      (when-let [q (ast->query ast clause)]
        (.add builder q BooleanClause$Occur/MUST)))
    (doseq [clause should]
      (when-let [q (ast->query ast clause)]
        (.add builder q BooleanClause$Occur/SHOULD)))
    (doseq [clause must-not]
      (when-let [q (ast->query ast clause)]
        (.add builder q BooleanClause$Occur/MUST_NOT)))
    (doseq [clause filter]
      (when-let [q (ast->query ast clause)]
        (.add builder q BooleanClause$Occur/FILTER)))
    (.build builder)))

(defmethod ast->query :default
  [_ _]
  (MatchAllDocsQuery.))

(defn- normalize-clauses [clauses]
  (cond
    (nil? clauses) []
    (sequential? clauses) (vec clauses)
    :else [clauses]))

(defn- create-lucene-query
  [{:keys [clauses] :as ast}]
  (let [clauses (normalize-clauses clauses)]
    (if (seq clauses)
      (ast->query ast {:type :boolean :must clauses})
      (ast->query ast {:type :match-all}))))

(defn- normalize-query-opts [query-map opts]
  {:limit (or (:limit opts) (:limit query-map) 100)
   :offset (or (:offset opts) (:offset query-map) 0)
   :sort (or (:sort opts) (:sort query-map))
   :after (or (:after opts) (:after query-map))})

(defn- execute-query
  [^IndexSearcher searcher ^Query query {:keys [limit offset sort after]}]
  (let [limit (max 1 (or limit 100))
        offset (max 0 (or offset 0))
        lucene-sort (when sort (build-sort sort))
        after-score-doc (when (and after (map? after))
                          (let [doc (:doc after)
                                score (:score after)]
                            (log/log-info (str "Preparing search-after cursor with doc=" doc " (" (class doc) ") score=" score " (" (class score) ")"))
                            (when (and (number? doc) (number? score))
                              (ScoreDoc. (int doc) (float score)))))
        format-results (fn [top-docs]
                         (let [score-docs (.-scoreDocs top-docs)
                               ids (mapv (fn [^ScoreDoc sd]
                                           (.get (.doc searcher (.-doc sd)) "id"))
                                         score-docs)
                               next-cursor (last score-docs)]
                           (log/log-info (str "Query executed: found " (.-totalHits top-docs)
                                              " total hits, returning " (count ids) " IDs: " (pr-str ids)))
                           {:ids ids
                            :total (.-totalHits top-docs)
                            :limit limit
                            :offset offset
                            :sort sort
                            :next-cursor next-cursor}))]
    (log/log-info (str "Executing query: " query ", limit=" limit ", offset=" offset ", sort=" lucene-sort))
    (try
      (if after-score-doc
        (let [top-docs (if lucene-sort
                         (.searchAfter searcher after-score-doc query limit lucene-sort)
                         (.searchAfter searcher after-score-doc query limit))]
          (format-results top-docs))
        (let [collector (if lucene-sort
                          (TopFieldCollector/create lucene-sort (+ limit offset) Integer/MAX_VALUE)
                          (TopScoreDocCollector/create (+ limit offset) Integer/MAX_VALUE))]
          (.search ^IndexSearcher searcher query collector)
          (let [top-docs (.topDocs collector offset limit)]
            (format-results top-docs))))
      (catch Exception e
        (let [sw (java.io.StringWriter.)
              pw (java.io.PrintWriter. sw)]
          (.printStackTrace e pw)
          (log/log-error (str "Error executing search-query: " (.getMessage e) "\n" (.toString sw))))
        {:ids [] :total 0 :limit limit :offset offset :sort sort :next-cursor nil}))))

;; This function is currently unused but might be needed later
#_(defn- doc->map
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

(defonce ^:private maintenance-task (atom nil))

(defn stop-index-maintenance-task
  "Stops the background maintenance task if it is running."
  []
  (when-let [task @maintenance-task]
    (future-cancel task)
    (reset! maintenance-task nil)))

(defn- ensure-searcher-updated!
  "Ensures the IndexSearcher attached to the LuceneIndex reflects the latest commits."
  [index]
  (let [reader-atom (:reader-atom index)
        searcher-atom (:searcher-atom index)
        ^IndexWriter writer (:writer index)]
    (when (and reader-atom searcher-atom writer)
      (try
        (if-let [^DirectoryReader current @reader-atom]
          (let [^DirectoryReader refreshed (refresh-reader current writer)]
            (when (and refreshed (not (identical? refreshed current)))
              (reset! reader-atom refreshed)
              (reset! searcher-atom (IndexSearcher. refreshed))))
          (when-let [^DirectoryReader initial (DirectoryReader/open writer)]
            (reset! reader-atom initial)
            (reset! searcher-atom (IndexSearcher. initial))))
        (catch Exception e
          (log/log-warn (str "Failed to refresh Lucene IndexSearcher: " (.getMessage e))))))))

(defn start-index-maintenance-task
  "Starts a background task that periodically commits and refreshes the Lucene index.
  The task wakes up every `interval-minutes` to ensure readers observe the latest writes."
  [index interval-minutes]
  (stop-index-maintenance-task)
  (let [interval-ms (-> (or interval-minutes 60)
                        (max 1)
                        (long)
                        (* 60 1000))
        task (future
               (try
                 (loop []
                   (Thread/sleep interval-ms)
                   (try
                     (when-let [^IndexWriter writer (:writer index)]
                       (.commit writer)
                       (ensure-searcher-updated! index)
                       (log/log-info "Lucene index maintenance cycle completed."))
                     (catch InterruptedException ie
                       (throw ie))
                     (catch Exception e
                       (log/log-error (str "Lucene index maintenance cycle failed: " (.getMessage e)))))
                   (recur))
                 (catch InterruptedException _
                   (log/log-info "Lucene index maintenance task interrupted."))
                 (catch Exception e
                   (log/log-error (str "Lucene index maintenance task stopped due to error: " (.getMessage e))))))]
    (reset! maintenance-task task)
    task))

(defn ensure-index-populated
  "Indexes existing documents from storage into the Lucene index.
  Returns a future when run asynchronously."
  ([index storage branch]
   (ensure-index-populated index storage branch {:async? true}))
  ([index storage branch {:keys [async?] :or {async? true}}]
   (let [branch-name (or branch "main")
         populate! (fn []
                     (try
                       (log/log-info (str "Ensuring Lucene index is populated for branch '" branch-name "'."))
                       (let [docs (->> (storage/get-documents-by-prefix storage "" branch-name)
                                       (remove nil?)
                                       (vec))]
                         (log/log-info (str "Found " (count docs) " document(s) to index for branch '" branch-name "'."))
                         (doseq [doc docs]
                           (when-let [doc-id (:id doc)]
                             (try
                               (protocol/delete-document index doc-id)
                               (protocol/index-document index doc)
                               (catch Exception e
                                 (log/log-error (str "Failed to index document '" doc-id "': " (.getMessage e)))))))
                         (ensure-searcher-updated! index)
                         (log/log-info (str "Lucene index population completed for branch '" branch-name "'.")))
                       (catch Exception e
                         (log/log-error (str "Error populating Lucene index for branch '" branch-name "': " (.getMessage e)))
                         (throw e))))]
     (if async?
       (future (populate!))
       (populate!)))))

(defrecord LuceneIndex [^Directory directory
                        ^Analyzer fts-analyzer
                        ^Analyzer default-analyzer
                        ^IndexWriter writer
                        reader-atom
                        searcher-atom]
  protocol/Index
  (index-document [_ doc]
    (try
      #_(log/log-info "Indexing document" {:id (:id doc) :branch (:branch doc)})
      (let [lucene-doc (create-lucene-doc doc nil)]
        (.addDocument writer lucene-doc)
        (.commit writer)
        (reset! reader-atom (DirectoryReader/open writer))
        (reset! searcher-atom (IndexSearcher. @reader-atom)))
      (catch Exception e
        (log/log-error (str "Error indexing document" {:id (:id doc) :error (.getMessage e)})))))

  (delete-document [_ id]
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

  (search [_ field query-string branch]
    (log/log-info (str "Starting Lucene search - Field: " field ", Query: '" query-string "' Branch: " branch))
    (try
      (if-let [_ @reader-atom]
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

  (search-query [_ query-map _branch opts]
    (let [query-opts (normalize-query-opts query-map opts)
          context {:fts-analyzer fts-analyzer
                   :default-analyzer default-analyzer}]
      (try
        (if-let [_ @reader-atom]
          (let [^IndexSearcher searcher @searcher-atom
                lucene-query (binding [*lucene-context* context]
                               (create-lucene-query query-map))
                _ (log/log-info (str "Executing search-query with clauses " (:clauses query-map) " and opts " query-opts))
                result (execute-query searcher lucene-query query-opts)]
            (log/log-info (str "Lucene search produced IDs: " (:ids result) ", total=" (:total result)))
            {:ids (:ids result)
             :total (:total result)
             :limit (:limit result)
             :offset (:offset result)
             :sort (:sort result)
             :next-cursor (:next-cursor result)})
          {:ids [] :total 0 :limit 0 :offset 0})
        (catch Exception e
          (let [sw (java.io.StringWriter.)
                pw (java.io.PrintWriter. sw)]
            (.printStackTrace e pw)
            (log/log-error (str "Error executing search-query: " (.getMessage e) "\n" (.toString sw))))
          {:ids [] :total 0 :limit 0 :offset 0 :sort (:sort query-opts) :next-cursor nil}))))

  (close [_]
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
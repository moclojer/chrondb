(ns chrondb.index.lucene
  (:require [chrondb.index.protocol :as protocol]
            [clojure.data.json :as json])
  (:import [org.apache.lucene.store FSDirectory]
           [org.apache.lucene.analysis.standard StandardAnalyzer]
           [org.apache.lucene.document Document StringField TextField Field$Store]
           [org.apache.lucene.index IndexWriter IndexWriterConfig DirectoryReader Term]
           [org.apache.lucene.search IndexSearcher BooleanQuery$Builder BooleanClause$Occur ScoreDoc]
           [org.apache.lucene.queryparser.classic QueryParser]
           [java.nio.file Paths]
           [org.apache.lucene.index IndexWriterConfig$OpenMode]))

(defn- create-document [doc]
  (let [lucene-doc (Document.)]
    (.add lucene-doc (StringField. "id" (:id doc) Field$Store/YES))
    (doseq [[k v] doc]
      (when (and v (not= k :id))
        (.add lucene-doc (TextField. (name k) (str v) Field$Store/YES))))
    (.add lucene-doc (TextField. "content" (json/write-str doc) Field$Store/YES))
    lucene-doc))

(defn- doc->map [^Document doc]
  (json/read-str (.get doc "content") :key-fn keyword))

(defn- create-query [query-str]
  (if (or (nil? query-str) (empty? query-str))
    nil
    (let [builder (BooleanQuery$Builder.)]
      (doseq [field ["id" "name" "content"]]
        (.add builder
              (.parse (QueryParser. field (StandardAnalyzer.)) query-str)
              BooleanClause$Occur/SHOULD))
      (.build builder))))

(defn- refresh-reader [^DirectoryReader reader ^IndexWriter writer]
  (let [new-reader (DirectoryReader/openIfChanged reader writer)]
    (if new-reader
      (do
        (.close reader)
        new-reader)
      reader)))

(defrecord LuceneIndex [directory analyzer writer reader-atom searcher-atom]
  protocol/Index
  (index-document [_ doc]
    (let [lucene-doc (create-document doc)]
      (.updateDocument writer (Term. "id" (:id doc)) lucene-doc)
      (.commit writer)
      (let [current-reader @reader-atom
            new-reader (refresh-reader current-reader writer)]
        (when-not (identical? new-reader current-reader)
          (reset! reader-atom new-reader)
          (reset! searcher-atom (IndexSearcher. new-reader))))
      doc))

  (delete-document [_ id]
    (.deleteDocuments writer (into-array Term [(Term. "id" id)]))
    (.commit writer)
    (let [current-reader @reader-atom
          new-reader (refresh-reader current-reader writer)]
      (when-not (identical? new-reader current-reader)
        (reset! reader-atom new-reader)
        (reset! searcher-atom (IndexSearcher. new-reader)))))

  (search [_ query-str]
    (if-let [query (create-query query-str)]
      (let [hits (.search @searcher-atom query 10)
            docs (for [^ScoreDoc hit (.scoreDocs hits)]
                   (-> (.doc @searcher-atom (.doc hit))
                       doc->map))]
        docs)
      []))

  (close [_]
    (.close writer)
    (.close @reader-atom)
    (.close directory)
    nil))

(defn create-lucene-index [index-dir]
  (let [directory (FSDirectory/open (Paths/get index-dir (into-array String [])))
        analyzer (StandardAnalyzer.)
        config (doto (IndexWriterConfig. analyzer)
                 (.setOpenMode IndexWriterConfig$OpenMode/CREATE_OR_APPEND))
        writer (IndexWriter. directory config)
        reader (DirectoryReader/open writer)
        searcher (IndexSearcher. reader)]
    (->LuceneIndex directory analyzer writer (atom reader) (atom searcher))))
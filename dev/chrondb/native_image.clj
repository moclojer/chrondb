(ns chrondb.native-image
  (:require [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clojure.java.shell :as shell]
            [clojure.string :as string])
  (:import [java.util.jar JarFile]))

(def base-build-time-classes
  ["java.security.SecureRandom"
   "org.apache.lucene.analysis.standard.StandardAnalyzer"
   "org.apache.lucene.analysis.CharArraySet"
   "org.apache.lucene.analysis.CharArrayMap$EmptyCharArrayMap"
   "org.apache.lucene.analysis.Analyzer$1"
   "org.apache.lucene.index.IndexWriterConfig"
   "org.apache.lucene.index.IndexWriter"
   "org.apache.lucene.index.IndexWriter$4"
   "org.apache.lucene.index.IndexWriter$5"
   "org.apache.lucene.index.DirectoryReader"
   "org.apache.lucene.index.ConcurrentMergeScheduler"
   "org.apache.lucene.index.ConcurrentMergeScheduler$2"
   "org.apache.lucene.search.IndexSearcher"
   "org.apache.lucene.search.similarities.BM25Similarity"
   "org.apache.lucene.search.similarities.SimilarityBase"
   "org.apache.lucene.search.similarities.Similarity"
   "org.apache.lucene.search.LRUQueryCache"
   "org.apache.lucene.search.LRUQueryCache$MinSegmentSizePredicate"
   "org.apache.lucene.search.UsageTrackingQueryCachingPolicy"
   "org.apache.lucene.store.MMapDirectory"
   "org.apache.lucene.store.NIOFSDirectory"
   "org.apache.lucene.store.MappedByteBufferIndexInputProvider"
   "org.apache.lucene.store.ByteBufferGuard$BufferCleaner"
   "org.apache.lucene.util.RamUsageEstimator"
   "org.apache.lucene.util.FrequencyTrackingRingBuffer"
   "org.apache.lucene.util.FrequencyTrackingRingBuffer$IntBag"
   "org.apache.lucene.util.Constants"
   "org.apache.lucene.util.ArrayUtil"
   "org.apache.lucene.index.Term"
   "org.apache.lucene.index.SegmentReader"
   "org.apache.lucene.index.SegmentReader$3"
   "org.apache.lucene.internal.tests.TestSecrets"
   "clojure.asm.Type"
   "clojure.asm.commons.Method"
   "clojure.lang.MethodImplCache"
   "clojure.lang.MethodImplCache$Entry"
   "org.slf4j.simple.SimpleLoggerConfiguration"
   "org.slf4j.simple.SimpleServiceProvider"
   "org.slf4j.simple.SimpleLoggerFactory"
   "org.slf4j.simple.SimpleLogger"
   "org.slf4j.simple.SimpleFormatter"
   "org.slf4j.simple.OutputChoice"
   "org.slf4j.simple.OutputChoice$OutputChoiceType"
   "org.slf4j.helpers.SubstituteServiceProvider"
   "org.slf4j.helpers.SubstituteLoggerFactory"
   "org.slf4j.helpers.SubstituteLogger"
   "org.slf4j.helpers.NOP_FallbackServiceProvider"
   "org.slf4j.helpers.NOPLoggerFactory"
   "org.slf4j.helpers.NOPLogger"
   "org.slf4j.LoggerFactory"
   "org.eclipse.jetty.util.log.Slf4jLog"
   "org.eclipse.jetty.util.log.Slf4jLog$1"
   "org.eclipse.jetty.util.log.Slf4jLog$LoggerCache"
   "org.eclipse.jetty.util.log.JettyAwareLogger"
   "org.eclipse.jetty.util.log.Log"
   "org.eclipse.jetty.util.Uptime$DefaultImpl"
   "org.eclipse.jetty.util.Uptime"
   "org.eclipse.jetty.util.StringUtil"
   "org.eclipse.jetty.util.component.AbstractLifeCycle"
   "org.eclipse.jetty.util.BufferUtil"
   "org.eclipse.jetty.util.ArrayTrie"
   "org.eclipse.jetty.util.ArrayTernaryTrie"
   "org.eclipse.jetty.util.ArrayTernaryTrie$GT"
   "org.eclipse.jetty.http.PreEncodedHttpField"
   "org.eclipse.jetty.http.PreEncodedHttpField$1"
   "org.eclipse.jetty.http.HttpHeader"
   "org.eclipse.jetty.http.HttpMethod"
   "org.eclipse.jetty.http.HttpScheme"
   "org.eclipse.jetty.http.HttpVersion"
   "org.eclipse.jetty.http.DateGenerator"
   "org.eclipse.jetty.http.DateGenerator$1"
   "org.eclipse.jetty.http.HttpFieldPreEncoder"
   "org.eclipse.jetty.http.Http1FieldPreEncoder"
   "org.eclipse.jetty.server.Response"
   "org.eclipse.jetty.server.Server"
   "org.eclipse.jetty.server.HttpOutput"
   "org.eclipse.jetty.server.HttpOutput$AsyncFlush"
   "org.eclipse.jetty.server.HttpOutput$State"
   "org.eclipse.jetty.server.handler.ContextHandler"
   "org.eclipse.jetty.server.HttpConfiguration"
   "org.eclipse.jetty.server.AbstractConnector"
   "org.eclipse.jetty.server.AbstractNetworkConnector"
   "org.eclipse.jetty.server.ServerConnector"
   "org.eclipse.jetty.util.ssl.SslContextFactory"
   "org.eclipse.jetty.util.ssl.SslContextFactory$X509ExtendedTrustManagerWrapper"
   "org.eclipse.jetty.util.ssl.SslContextFactory$X509ExtendedManager"
   "org.eclipse.jetty.util.ssl.SslContextFactory$X509ExtendedKeyManagerWrapper"
   "org.eclipse.jetty.util.ssl.SslContextFactory$KeyStoreData"
   "org.eclipse.jetty.util.ssl.SslContextFactory$TrustManager"
   "org.eclipse.jetty.util.Jetty"
   "org.slf4j.simple.OutputChoice$1"
   "org.eclipse.jgit.util.RawParseUtils"
   "org.eclipse.jgit.lib.ObjectId"
   "org.eclipse.jgit.treewalk.CanonicalTreeParser"
   "org.eclipse.jgit.lib.Constants"
   "chrondb.lib.ChronDBLib"])

(def base-run-time-classes
  ["org.eclipse.jgit.util.FileUtils"
   "org.eclipse.jgit.internal.storage.file.FileReftableStack"
   "org.eclipse.jgit.internal.storage.file.FileReftableDatabase"
   "com.fasterxml.jackson.core.JsonFactory"
   "com.fasterxml.jackson.core.io.SerializedString"])

(def jar-path "target/chrondb.jar")

(defn- jar-classes-by-prefix
  "Scans the uberjar for .class files under the given path prefix.
   Returns a sorted vector of fully-qualified class names."
  [path-prefix pkg-prefix]
  (let [jar-file (io/file jar-path)]
    (if (and (.exists jar-file) (.isFile jar-file))
      (with-open [jar (JarFile. jar-file)]
        (->> (.entries jar)
             enumeration-seq
             (keep (fn [entry]
                     (let [name (.getName entry)]
                       (when (and (string/starts-with? name path-prefix)
                                  (string/ends-with? name ".class"))
                         (let [relative (subs name (count path-prefix))
                               no-ext (string/replace relative #"\.class$" "")
                               fqn   (str pkg-prefix (string/replace no-ext #"/" "."))]
                           fqn)))))
             (into (sorted-set))
             vec))
      [])))

(defn- clojure-lang-classes
  []
  (jar-classes-by-prefix "clojure/lang/" "clojure.lang."))

(defn- chrondb-classes
  "Returns all AOT-compiled chrondb classes from the uberjar."
  []
  (jar-classes-by-prefix "chrondb/" "chrondb."))

(defn- clojure-data-classes
  "Returns all clojure.data.* classes (e.g. clojure.data.json) from the uberjar."
  []
  (jar-classes-by-prefix "clojure/data/" "clojure.data."))

(defn- clojure-initializers
  [classes]
  (let [targets #{}]
    (vec (filter targets classes))))

(def base-reflect-config
  [{:name "org.eclipse.jgit.internal.JGitText"
    :allDeclaredConstructors true
    :allDeclaredMethods true
    :allDeclaredFields true}
   {:name "org.eclipse.jgit.nls.NLS"
    :allDeclaredConstructors true
    :allDeclaredMethods true}
   {:name "org.eclipse.jgit.util.SystemReader"
    :allDeclaredConstructors true
    :allDeclaredMethods true
    :allDeclaredFields true}
   {:name "org.eclipse.jgit.lib.CoreConfig$HideDotFiles"
    :allDeclaredConstructors true
    :allPublicConstructors true
    :allDeclaredMethods true
    :allPublicMethods true}
   {:name "org.eclipse.jgit.lib.CoreConfig$TrustPackedRefsStat"
    :allDeclaredMethods true
    :allPublicMethods true}
   {:name "org.eclipse.jgit.lib.CoreConfig$LogRefUpdates"
    :allDeclaredMethods true
    :allPublicMethods true}
   {:name "org.eclipse.jgit.util.sha1.SHA1$Sha1Implementation"
    :allDeclaredMethods true
    :allPublicMethods true}
   {:name "org.eclipse.jgit.lib.PersonIdent"
    :allDeclaredConstructors true
    :allPublicConstructors true
    :allDeclaredMethods true
    :allPublicMethods true}
   {:name "org.apache.lucene.index.ConcurrentMergeScheduler"
    :allDeclaredConstructors true
    :allPublicConstructors true
    :allDeclaredMethods true
    :allPublicMethods true}
   {:name "org.apache.lucene.internal.tests.TestSecrets"
    :allDeclaredConstructors true
    :allDeclaredMethods true}
   {:name "org.apache.lucene.search.IndexSearcher"
    :allDeclaredConstructors true
    :allPublicConstructors true
    :allDeclaredMethods true
    :allPublicMethods true}
   {:name "org.eclipse.jetty.server.Server"
    :allDeclaredConstructors true
    :allPublicConstructors true
    :allDeclaredMethods true
    :allPublicMethods true}])

(defn- macos-sdk-root
  []
  (try
    (let [{:keys [exit out]} (shell/sh "xcrun" "--sdk" "macosx" "--show-sdk-path")]
      (when (zero? exit)
        (string/trim out)))
    (catch Exception _ nil)))

(defn- ensure-dir!
  ^java.io.File [dir]
  (.mkdirs dir)
  dir)

(defn prepare-files
  "Generate helper files required by GraalVM native-image."
  []
  (println "Generating GraalVM native-image configuration for ChronDB")
  (let [target-dir (ensure-dir! (io/file "target"))
        native-config-dir (ensure-dir! (io/file target-dir "native-config"))
        report-dir (ensure-dir! (io/file "report"))
        sdk-root (or (System/getenv "SDKROOT") (macos-sdk-root))
        linker-args (concat (when sdk-root
                              [(str "-H:NativeLinkerOption=-L" (io/file sdk-root "usr/lib"))])
                            ["-H:NativeLinkerOption=-lz"])
        base-args ["-Dorg.slf4j.simpleLogger.defaultLogLevel=info"
                   "-Dorg.slf4j.simpleLogger.log.org.eclipse.jetty.server=warn"
                   "--features=clj_easy.graal_build_time.InitClojureClasses"]
        path-sep (System/getProperty "path.separator")
        classpath (string/join path-sep ["target/classes" jar-path])
        clj-classes (clojure-lang-classes)
        clj-initializers (clojure-initializers clj-classes)
        runtime-set (set clj-initializers)
        build-time-clj (remove runtime-set clj-classes)
        chrondb-clj (chrondb-classes)
        data-clj (clojure-data-classes)
        build-time-classes (->> (concat base-build-time-classes build-time-clj
                                        chrondb-clj data-clj)
                                distinct
                                vec)
        run-time-classes (->> (concat base-run-time-classes clj-initializers)
                              distinct
                              vec)
        build-time-arg (str "--initialize-at-build-time=" (string/join "," build-time-classes))
        run-time-args (map #(str "--initialize-at-run-time=" %) run-time-classes)
        startup-args ["-cp" classpath
                      "-jar" jar-path
                      "-H:Class=chrondb.core"]
        tail-args (concat ["--enable-all-security-services"
                           build-time-arg]
                          run-time-args
                          [(str "-H:ConfigurationFileDirectories=" (.getPath native-config-dir))
                           (str "-H:ReflectionConfigurationFiles=" (.getPath (io/file native-config-dir "reflect-config.json")))
                           (str "-H:ResourceConfigurationFiles=" (.getPath (io/file native-config-dir "resource-config.json")))
                           "-H:EnableURLProtocols=http,https"
                           "-H:DashboardDump=report/chrondb"
                           "-H:+ReportExceptionStackTraces"
                           "-H:+DashboardHeap"
                           "-H:+DashboardCode"
                           "-H:+DashboardBgv"
                           "-H:+DashboardJson"
                           "-H:-CheckToolchain"
                           "-O0"
                           "--no-fallback"
                           "--verbose"])]
    (spit (io/file target-dir "filter.json") (json/write-str {:rules []}))
    (let [trace-classes ["org.apache.lucene.analysis.Analyzer$1"
                         "org.apache.lucene.analysis.CharArrayMap$EmptyCharArrayMap"
                         "org.apache.lucene.util.RamUsageEstimator"
                         "org.apache.lucene.analysis.core.SimpleAnalyzer"
                         "org.apache.lucene.search.similarities.BM25Similarity"
                         "org.apache.lucene.search.similarities.ClassicSimilarity"
                         "org.apache.lucene.search.similarities.SimilarityBase"
                         "org.apache.lucene.search.similarities.Similarity"
                         "org.slf4j.simple.SimpleLoggerConfiguration"
                         "org.slf4j.simple.SimpleServiceProvider"
                         "org.slf4j.simple.SimpleLoggerFactory"
                         "org.slf4j.helpers.SubstituteServiceProvider"
                         "org.slf4j.helpers.NOP_FallbackServiceProvider"
                         "org.slf4j.helpers.SubstituteLoggerFactory"
                         "org.slf4j.helpers.SubstituteLogger"
                         "org.slf4j.helpers.NOPLoggerFactory"
                         "org.slf4j.helpers.NOPLogger"
                         "org.eclipse.jetty.util.Uptime$DefaultImpl"
                         "org.eclipse.jetty.util.log.Slf4jLog"
                         "org.eclipse.jetty.util.log.Slf4jLog$1"
                         "org.eclipse.jetty.util.log.Slf4jLog$LoggerCache"
                         "org.eclipse.jetty.util.log.Log"
                         "org.eclipse.jetty.server.Server"
                         "org.eclipse.jetty.server.handler.ContextHandler"]
          trace-object-args (map #(str "--trace-object-instantiation=" %) trace-classes)]
      (spit (io/file target-dir "native-image-args")
            (string/join "\n" (concat base-args
                                      startup-args
                                      linker-args
                                      trace-object-args
                                      tail-args))))
    (.mkdirs report-dir)
    (let [reflect-config (into base-reflect-config
                               (for [cls clj-classes]
                                 {:name cls
                                  :allDeclaredConstructors true
                                  :allDeclaredMethods true
                                  :allDeclaredFields true}))
          resource-config {:resources {:includes [{:pattern "org/eclipse/jgit/internal/JGitText.*"}
                                                  {:pattern "org/eclipse/jgit/internal/JGitText_.*"}
                                                  {:pattern "clojure/.*"}
                                                  {:pattern "META-INF/services/.*"}
                                                  {:pattern "META-INF/native-image/clojure/.*"}]}}]
      (spit (io/file native-config-dir "reflect-config.json") (json/write-str reflect-config))
      (spit (io/file native-config-dir "resource-config.json") (json/write-str resource-config)))
    (doseq [[fname contents] [["reflect-config.json" "[]"]
                              ["resource-config.json" (json/write-str {:resources {:includes []}})]]]
      (let [file (io/file native-config-dir fname)]
        (when-not (.exists file)
          (spit file contents))))
    nil))

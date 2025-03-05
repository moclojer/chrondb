(ns dev.chrondb.build
  (:require [clojure.tools.build.api :as b]))

(def lib 'chrondb/chrondb)
(def version (format "0.1.%s" (b/git-count-revs nil)))
(def class-dir "target/classes")
(def basis (b/create-basis {:project "deps.edn"}))
(def uber-file (format "target/%s-%s-standalone.jar" (name lib) version))
(def native-image-name "chrondb")

(defn clean [_]
  (b/delete {:path "target"}))

(defn uber [_]
  (clean nil)
  (b/copy-dir {:src-dirs ["src" "resources"]
               :target-dir class-dir})
  (b/compile-clj {:basis basis
                  :src-dirs ["src"]
                  :class-dir class-dir
                  :ns-compile '[chrondb.core]})
  (b/uber {:class-dir class-dir
           :uber-file uber-file
           :basis basis
           :main 'chrondb.core}))

(defn native-image
  "Build a native image for ChronDB.

   Options:
   - :verbose - Boolean flag to enable verbose output (default: false)
   - :static - Boolean flag to build a statically linked executable (default: false)
   - :output - String specifying the output file name (default: 'chrondb')
   - :extra_flags - Vector of additional flags to pass to native-image
   - :initialize_at_run_time - Vector of classes to initialize at runtime
   - :enable_url_protocols - String of comma-separated URL protocols to enable
   - :clj_easy - Boolean flag to use clj-easy/graalvm-clojure approach (default: true)"
  [{:keys [verbose static output extra_flags initialize_at_run_time enable_url_protocols clj_easy]
    :or {verbose false
         static false
         output native-image-name
         extra_flags []
         initialize_at_run_time ["org.eclipse.jetty.server.Server"
                                 "org.eclipse.jetty.util.thread.QueuedThreadPool"
                                 "org.eclipse.jgit.lib.internal.WorkQueue"
                                 "java.security.SecureRandom"
                                 "org.eclipse.jgit.transport.HttpAuthMethod"
                                 "org.eclipse.jgit.internal.storage.file.WindowCache"
                                 "org.eclipse.jgit.util.FileUtils"
                                 "sun.security.jca.JCAUtil$CachedSecureRandomHolder"
                                 "org.eclipse.jgit.util.sha1.SHA1"
                                 "org.eclipse.jgit.lib.RepositoryCache"]
         enable_url_protocols "http,https"
         clj_easy true}}]
  (println "Building native image...")
  (let [base-command ["native-image"
                      "--no-fallback"
                      "--report-unsupported-elements-at-runtime"]
        init-at-runtime (if clj_easy
                          [(str "--initialize-at-run-time=" (clojure.string/join "," initialize_at_run_time))]
                          (mapcat #(vector "--initialize-at-run-time=" %) initialize_at_run_time))
        init-at-buildtime (if clj_easy
                            ["--initialize-at-build-time=clojure"
                             "--initialize-at-build-time=org.slf4j"
                             "--initialize-at-build-time=ch.qos.logback"]
                            [])
        url-protocols (str "-H:EnableURLProtocols=" enable_url_protocols)
        reflection-config "-H:ReflectionConfigurationFiles=graalvm-config/reflect-config.json"
        resource-config "-H:ResourceConfigurationFiles=graalvm-config/resource-config.json"
        clj-easy-flags (if clj_easy
                         ["-H:+RemoveSaturatedTypeFlows"]
                         [])
        command (cond-> (vec (concat base-command
                                     extra_flags
                                     init-at-buildtime
                                     init-at-runtime
                                     [url-protocols reflection-config resource-config]
                                     clj-easy-flags))
                  verbose (conj "--verbose")
                  static (conj "--static")
                  true (concat ["-H:+ReportExceptionStackTraces"
                                "-H:-CheckToolchain"
                                "-H:ConfigurationFileDirectories=graalvm-config"
                                "-H:+PrintClassInitialization"
                                "-H:+AllowIncompleteClasspath"
                                "-H:+AddAllCharsets"
                                "-H:+UnlockExperimentalVMOptions"
                                "-jar" uber-file output]))]
    (println "Running command:" (clojure.string/join " " command))
    (b/process {:command-args command}))
  (println "Native image built successfully:" (str "./" output)))
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
   - :enable_url_protocols - String of comma-separated URL protocols to enable"
  [{:keys [verbose static output extra_flags enable_url_protocols]
    :or {verbose false
         static false
         output native-image-name
         extra_flags []
         enable_url_protocols "http,https"}}]
  (println "Building native image...")

  ;; Configurações básicas para o GraalVM
  (let [graal-configs ["--no-fallback"
                       "--report-unsupported-elements-at-runtime"
                       "-H:-CheckToolchain"
                       "-Dlucene.tests.security.manager=false"
                       "-Dlucene.tests.fail.on.unsupported.codec=false"
                       "--features=clj_easy.graal_build_time.InitClojureClasses"
                       "--allow-incomplete-classpath"
                       (str "-H:EnableURLProtocols=" enable_url_protocols)
                       "-H:ReflectionConfigurationFiles=graalvm-config/reflect-config.json"
                       "-H:ResourceConfigurationFiles=graalvm-config/resource-config.json"
                       "-H:+ReportExceptionStackTraces"
                       "-H:+RemoveSaturatedTypeFlows"
                       "-H:+AddAllCharsets"]

        ;; Adicionar flags condicionais
        command (cond-> (vec (concat graal-configs
                                     extra_flags))
                  verbose (conj "--verbose")
                  static (conj "--static")
                  true (concat ["-jar" uber-file output]))]

    (println "Running command:" (clojure.string/join " " command))
    (b/process {:command-args (into ["native-image"] command)}))

  (println "Native image built successfully:" (str "./" output)))
(ns chrondb.build
  (:refer-clojure :exclude [test])
  (:require [clojure.java.io :as io]
            [clojure.string :as string]
            [clojure.tools.build.api :as b]
            [chrondb.native-image :as native-image]))

(def class-dir "target/classes")
(def jar-file "target/chrondb.jar")

(set! *warn-on-reflection* true)

(defmacro with-err-str
  [& body]
  `(let [s# (new java.io.StringWriter)]
     (binding [*err* s#]
       ~@body
       (str s#))))

(defn project-version
  []
  (or (System/getenv "CHROND_VERSION")
      (System/getenv "CI_COMMIT_SHA")
      "0.1.0-SNAPSHOT"))

(defn build-basis
  []
  (b/create-basis {:project "deps.edn"}))

(defn build-options
  [basis]
  {:class-dir  class-dir
   :lib        'chrondb/chrondb
   :main       'chrondb.core
   :basis      basis
   :version    (project-version)
   :ns-compile '[chrondb.core]
   :uber-file  jar-file
   :jar-file   jar-file
   :target     "target"
   :src-dirs   (:paths basis)
   :pom-data   [[:description "ChronDB - Chronological key/value database"]
                [:url "https://github.com/moclojer/chrondb"]
                [:licenses
                 [:license
                  [:name "GNU GPLv3"]
                  [:url "https://www.gnu.org/licenses/gpl-3.0.en.html"]]]
                [:scm
                 [:url "https://github.com/moclojer/chrondb"]
                 [:connection "scm:git:https://github.com/moclojer/chrondb.git"]
                 [:developerConnection "scm:git:ssh:git@github.com:moclojer/chrondb.git"]
                 [:tag "HEAD"]]]
   :exclude    ["docs/*" "META-INF/*" "test/*" "target/*"]})

(defn clean-target!
  []
  (println "Clearing target directory")
  (b/delete {:path "target"}))

(defn write-pom!
  [options]
  (println "Writing pom")
  (->> (b/write-pom options)
       with-err-str
       string/split-lines
       (remove #(re-matches #"^Skipping coordinate: \{:local/root .*target/(lib1|lib2|graal-build-time).jar.*" %))
       (run! println)))

(defn copy-sources!
  [options]
  (b/copy-dir {:src-dirs (:src-dirs options)
               :target-dir class-dir}))

(defn compile-sources!
  [options]
  (println "Compile sources to classes")
  (b/compile-clj options))

(defn package-jar!
  [options uberjar?]
  (println "Building" (if uberjar? "uberjar" "jar"))
  (if uberjar?
    (b/uber options)
    (b/jar options)))

(defn prepare-native-dir!
  []
  (let [native-dir (io/file "target" "native")]
    (when (.exists native-dir)
      (b/delete {:path (.getPath native-dir)}))
    (.mkdirs native-dir)))

(defn -main
  [& args]
  (let [uberjar? (boolean (some #(= % "--uberjar") args))
        basis (build-basis)
        options (build-options basis)]
    (clean-target!)
    (write-pom! options)
    (copy-sources! options)
    (compile-sources! options)
    (package-jar! options uberjar?)
    (prepare-native-dir!)
    (native-image/prepare-files)))

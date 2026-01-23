(ns chrondb.shared-library
  "Build script for producing the ChronDB shared library via GraalVM native-image --shared.
   This script:
   1. Builds the uberjar (reusing chrondb.build)
   2. Compiles the Java CEntryPoint class
   3. Generates a native-image args file configured for --shared output

   Usage: clojure -M:shared-lib"
  (:require [chrondb.build :as build]
            [chrondb.native-image :as native-image]
            [clojure.java.io :as io]
            [clojure.java.shell :as shell]
            [clojure.string :as string]))

(def java-src-dir "java")
(def java-class-dir "target/shared-classes")
(def shared-args-file "target/shared-image-args")

(defn- find-graalvm-home
  "Locates GraalVM home from GRAALVM_HOME or JAVA_HOME."
  []
  (or (System/getenv "GRAALVM_HOME")
      (System/getenv "JAVA_HOME")))

(defn- svm-jar-path
  "Returns the path to the GraalVM SVM jar for compilation."
  [graalvm-home]
  (let [candidates [(io/file graalvm-home "lib" "svm" "builder" "svm.jar")
                    (io/file graalvm-home "lib" "graalvm" "svm-driver.jar")
                    (io/file graalvm-home "lib" "svm" "library-support.jar")]]
    (->> candidates
         (filter #(.exists %))
         first)))

(defn- find-svm-jars
  "Finds all SVM-related jars needed for compiling CEntryPoint annotations."
  [graalvm-home]
  (let [svm-dir (io/file graalvm-home "lib" "svm" "builder")
        svm-lib (io/file graalvm-home "lib" "svm")
        graalvm-dir (io/file graalvm-home "lib" "graalvm")]
    (->> (concat (when (.isDirectory svm-dir) (.listFiles svm-dir))
                 (when (.isDirectory svm-lib) (.listFiles svm-lib))
                 (when (.isDirectory graalvm-dir) (.listFiles graalvm-dir)))
         (filter #(and (.isFile %) (string/ends-with? (.getName %) ".jar")))
         (map #(.getAbsolutePath %))
         vec)))

(defn- compile-java!
  "Compiles the ChronDBLib.java file with GraalVM SVM on the classpath."
  [graalvm-home]
  (println "Compiling Java CEntryPoint sources...")
  (let [class-dir-f (io/file java-class-dir)]
    (.mkdirs class-dir-f)
    (let [svm-jars (find-svm-jars graalvm-home)
          uberjar-path (.getAbsolutePath (io/file build/jar-file))
          classpath (string/join (System/getProperty "path.separator")
                                 (concat svm-jars [uberjar-path]))
          javac (str (or graalvm-home "") "/bin/javac")
          javac-cmd (if (.exists (io/file javac)) javac "javac")
          java-file (.getAbsolutePath (io/file java-src-dir "chrondb" "lib" "ChronDBLib.java"))
          {:keys [exit out err]} (shell/sh javac-cmd
                                           "-cp" classpath
                                           "-d" java-class-dir
                                           "-source" "11"
                                           "-target" "11"
                                           java-file)]
      (when (seq out) (println out))
      (when (seq err) (println err))
      (when-not (zero? exit)
        (throw (ex-info "javac compilation failed" {:exit exit})))
      (println "Java compilation successful."))))

(defn- generate-shared-args!
  "Generates the native-image args file for --shared mode."
  [graalvm-home]
  (println "Generating shared-image-args...")
  ;; First generate the standard native-image config
  (native-image/prepare-files)

  ;; Now read the generated args and modify for --shared
  ;; The args file has one argument per line. -cp and -jar are followed by
  ;; their value on the NEXT line. We need to:
  ;;   - Remove -jar + its value line
  ;;   - Remove -H:Class=...
  ;;   - Append shared-classes dir to the classpath value line
  (let [original-args (slurp (io/file "target" "native-image-args"))
        lines (vec (string/split-lines original-args))
        path-sep (System/getProperty "path.separator")
        shared-classes-abs (.getAbsolutePath (io/file java-class-dir))
        ;; Process lines with index awareness to handle -cp/-jar pairs
        processed (loop [i 0
                         result []]
                    (if (>= i (count lines))
                      result
                      (let [line (nth lines i)]
                        (cond
                          ;; Skip -jar and its following value line
                          (= line "-jar")
                          (recur (+ i 2) result)

                          ;; Skip -H:Class= and dashboard args
                          (or (string/starts-with? line "-H:Class=")
                              (string/starts-with? line "-H:DashboardDump=")
                              (string/starts-with? line "-H:+DashboardHeap")
                              (string/starts-with? line "-H:+DashboardCode")
                              (string/starts-with? line "-H:+DashboardBgv")
                              (string/starts-with? line "-H:+DashboardJson"))
                          (recur (inc i) result)

                          ;; -cp: keep it, and modify the next line (classpath value)
                          (= line "-cp")
                          (let [cp-value (nth lines (inc i))
                                new-cp (str cp-value path-sep shared-classes-abs)]
                            (recur (+ i 2) (conj result "-cp" new-cp)))

                          ;; Everything else: keep as-is
                          :else
                          (recur (inc i) (conj result line))))))
        ;; Add --shared and library name at the beginning
        shared-args (concat ["--shared"
                             "-H:Name=libchrondb"
                             (str "-H:Path=" (.getAbsolutePath (io/file "target")))]
                            processed)
        args-content (string/join "\n" shared-args)]
    (spit (io/file shared-args-file) args-content)
    (println (str "Shared library args written to: " shared-args-file))
    (println)
    (println "To build the shared library, run:")
    (println (str "  native-image @" shared-args-file))
    (println)
    (println "Output files will be in target/:")
    (println "  libchrondb.dylib (macOS) / libchrondb.so (Linux)")
    (println "  libchrondb.h")
    (println "  libchrondb_dynamic.h")
    (println "  graal_isolate.h")
    (println "  graal_isolate_dynamic.h")))

(defn -main
  [& _args]
  (println "=== ChronDB Shared Library Build ===")
  (println)

  ;; Step 1: Build uberjar
  (println "Step 1: Building uberjar...")
  (build/-main "--uberjar")
  (println)

  ;; Step 2: Compile Java CEntryPoint
  (println "Step 2: Compiling Java CEntryPoint...")
  (let [graalvm-home (find-graalvm-home)]
    (when-not graalvm-home
      (println "WARNING: GRAALVM_HOME / JAVA_HOME not set. Attempting javac from PATH."))
    (compile-java! graalvm-home)
    (println)

    ;; Step 3: Generate native-image args for --shared
    (println "Step 3: Generating native-image configuration...")
    (generate-shared-args! graalvm-home))

  (println)
  (println "=== Build preparation complete ==="))

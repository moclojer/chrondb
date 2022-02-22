(ns chrondb.api-v1
  "
  JGit javadoc: https://download.eclipse.org/jgit/site/6.0.0.202111291000-r/apidocs/index.html
  "
  (:refer-clojure :exclude [select-keys])
  (:require [clojure.java.io :as io]
            [clojure.spec.alpha :as s]
            [clojure.data.json :as json]
            [clojure.string :as string])
  (:import (java.io ByteArrayOutputStream File InputStream OutputStream)
           (java.lang AutoCloseable)
           (java.nio.charset StandardCharsets)
           (java.time Instant)
           (org.eclipse.jgit.api Git)
           (org.eclipse.jgit.internal.storage.dfs InMemoryRepository InMemoryRepository$Builder)
           (org.eclipse.jgit.lib AnyObjectId CommitBuilder CommitBuilder Constants FileMode PersonIdent
             #_RefUpdate$Result ObjectId Repository TreeFormatter BaseRepositoryBuilder)
           (org.eclipse.jgit.revwalk RevWalk RevTree)
           (org.eclipse.jgit.storage.file FileRepositoryBuilder)
           (org.eclipse.jgit.treewalk TreeWalk)
           (java.net URI)
           (org.eclipse.jgit.treewalk.filter PathFilter)))
;; TODO: support InMemoryRepository

(set! *warn-on-reflection* true)

(defn any-object-id?
  [x]
  (instance? AnyObjectId x))
(s/def ::tree any-object-id?)

(defn file?
  [x]
  (instance? File x))
(s/def ::db-dir file?)


(defmulti db-uri->repository-destroyer ::scheme)


(defn parse-db-uri
  [db-uri]
  (let [uri (URI/create db-uri)
        scheme (.getScheme uri)
        uri (URI/create (.getRawSchemeSpecificPart uri))]
    (when-not (= scheme "chrondb")
      (throw (ex-info (str "Only uri's starting with 'chrondb' are supported. Actual: " scheme)
               {:cognitect.anomalies/category :cognitect.anomalies/unsupported
                ::db-uri                      db-uri})))
    {::scheme (.getScheme uri)
     ::db-uri db-uri
     ::path   (subs (.getRawSchemeSpecificPart uri) 2)}))


(defn delete-database
  [db-uri]
  (db-uri->repository-destroyer (parse-db-uri db-uri)))

(defonce *memory-repository
  (atom {}))

(defmethod db-uri->repository-destroyer "mem"
  [{::keys [path]}]
  (swap! *memory-repository
    (fn [memory-repository]
      (when-let [^Repository repository (get memory-repository path)]
        (.close repository))
      (dissoc memory-repository path))))



(defmulti ^BaseRepositoryBuilder db-uri->repository-builder ::scheme)

(defmethod db-uri->repository-builder "mem"
  [{::keys [path]}]
  (-> *memory-repository
    (swap! (fn [memory-repository]
             (if (contains? memory-repository path)
               memory-repository
               (assoc memory-repository path (InMemoryRepository$Builder.)))))
    (get path)))

(defmethod db-uri->repository-builder "file"
  [{::keys [path]}]
  (-> (FileRepositoryBuilder.)
    (doto (.setGitDir (apply io/file (string/split path #"/"))))))


(defn create-database
  [db-uri]
  (let [uri (parse-db-uri db-uri)
        repository (-> (db-uri->repository-builder uri)
                     (doto (.setInitialBranch "main"))
                     (.build)
                     (doto (.create true)))]
    (with-open [object-inserter (.newObjectInserter repository)]
      (let [^ByteArrayOutputStream created-at-blob (with-open [baos (ByteArrayOutputStream.)
                                                               w (io/writer baos)]
                                                     (json/write (str (Instant/now)) w)
                                                     baos)
            created-at-id (.insert object-inserter Constants/OBJ_BLOB
                            (.toByteArray created-at-blob))
            db-tree-formatter (doto (TreeFormatter.)
                                (.append "created-at" FileMode/REGULAR_FILE created-at-id))
            db-tree-formatter-id (.insert object-inserter db-tree-formatter)
            root-tree-formatter (doto (TreeFormatter.)
                                  (.append "db" FileMode/TREE db-tree-formatter-id))
            root-tree-id (.insert object-inserter root-tree-formatter)
            commit (doto (CommitBuilder.)
                     (.setAuthor (PersonIdent. "chrondb" "chrondb@localhost"))
                     (.setCommitter (PersonIdent. "chrondb" "chrondb@localhost"))
                     (.setTreeId root-tree-id)
                     (.setMessage "init"))
            commit-id (.insert object-inserter commit)]
        (.flush object-inserter)
        (-> (.updateRef repository Constants/HEAD)
          (doto (.setExpectedOldObjectId (ObjectId/zeroId))
                (.setNewObjectId commit-id)
                (.setRefLogMessage "World" false))
          (.update))))
    repository))


(defn connect
  [db-uri]
  (let [uri (parse-db-uri db-uri)
        repository (-> (db-uri->repository-builder uri)
                     (.build))
        _ (when-not (.exists (.getDirectory repository))
            (throw (ex-info (str "Can't connect to " db-uri ": directory do not exists")
                     {:cognitect.anomalies/category :cognitect.anomalies/not-found
                      ::db-uri                      db-uri})))
        ;; Every git repo has a HEAD file
        ;; This file points to a ref file, that contains a sha of a commit
        ;; This commit has a reference to a tree
        branch (.resolve repository Constants/HEAD)]
    {::repository   repository
     ::value-reader (fn [^InputStream in]
                      (io/reader in))
     ::read-value   (fn [rdr]
                      (json/read rdr :key-fn keyword))
     ::value-writer (fn [^OutputStream out]
                      (io/writer out))
     ::write-value  (fn [x writer]
                      (json/write x writer))
     ::branch       branch}))

(defn db
  [{::keys [^Repository repository]
    :as    chronn}]
  (let [commit (.parseCommit repository (.resolve repository Constants/HEAD))
        tree (.getTree commit)]
    (assoc chronn
      ::tree tree)))

(defn select-keys
  [{::keys [^Repository repository ^RevTree tree
            value-reader
            read-value]}
   ks]
  (let [reader (.newObjectReader repository)
        f (PathFilter/create (str (first ks)))
        tw (doto (TreeWalk. repository reader)
             (.setFilter f)
             (.reset tree))]
    (loop []
      (when (.next tw)
        (when (.isSubtree tw)
          (.enterSubtree tw)
          (recur))))
    (merge {}
      (when tw
        (let [obj (.getObjectId tw 0)]
          (when-not (= (ObjectId/zeroId) obj)
            (with-open [in ^AutoCloseable (value-reader (.openStream (.open repository obj)))]
              {(first ks) (read-value in)})))))))

(defn save
  [{::keys [^Repository repository ^AnyObjectId branch value-writer write-value]
    :as    chronn} k v]

  (with-open [object-inserter (.newObjectInserter repository)
              rw (RevWalk. repository)]
    (let [^ByteArrayOutputStream baos (with-open [baos (ByteArrayOutputStream.)
                                                  w ^AutoCloseable (value-writer baos)]
                                        (write-value v w)
                                        baos)
          {::keys [^RevTree tree]} (db chronn)

          ;; tree 0001 db ->

          ;; tree 0001 db
          blob (.toByteArray baos)
          object-id (.insert object-inserter Constants/OBJ_BLOB blob)
          tree-walk (doto (TreeWalk. repository)
                      (.reset tree))
          tree-formatter (doto (TreeFormatter.)
                           (.append (str k) FileMode/REGULAR_FILE object-id))
          _ (loop [vs []]
              (if (.next tree-walk)
                (doseq [n (range (.getTreeCount tree-walk))]
                  (.append tree-formatter
                    (.getNameString tree-walk)
                    (FileMode/fromBits (.getRawMode tree-walk n))
                    (.getObjectId tree-walk n)))
                vs))

          tree-id (.insert object-inserter tree-formatter)
          next-tree (.parseTree rw tree-id)
          commit (doto (CommitBuilder.)
                   (.setAuthor (PersonIdent. "chrondb" "chrondb@localhost"))
                   (.setCommitter (PersonIdent. "chrondb" "chrondb@localhost"))
                   (.setTreeId next-tree)
                   (.setParentId branch)
                   (.setMessage "Hello!"))
          commit-id (.insert object-inserter commit)]
      (.flush object-inserter)
      (let [ru (doto (.updateRef repository Constants/HEAD)
                 (.setExpectedOldObjectId branch)
                 (.setNewObjectId commit-id)
                 (.setRefLogMessage "World" false))
            status (.update ru)]
        (case (str status)
          "FAST_FORWARD"
          {:db-after (assoc chronn
                       ::tree next-tree)}
          ;; RefUpdate$Result/NEW
          "NEW"
          {:db-after (assoc chronn
                       ::tree next-tree)}
          ;; RefUpdate$Result/FORCED
          "FORCED"
          {:db-after (assoc chronn
                       ::tree next-tree)})))))

(defmethod db-uri->repository-destroyer "file"
  [{::keys [db-uri]}]
  (try
    (let [chronn (connect db-uri)
          db (db chronn)
          created-at (get (select-keys db ["db/created-at"])
                       "db/created-at")]
      (when-not created-at
        (throw (ex-info (str "Can't find a db at " (pr-str db-uri))
                 {:cognitect.anomalies/category :cognitect.anomalies/incorrect
                  ::db-uri                      db-uri})))
      (doseq [^File f (reverse (file-seq (.getDirectory ^Repository (::repository chronn))))]
        (.delete f)))
    (catch Exception ex
      (let [data (ex-data ex)]
        (if (and (= (::db-uri data)
                   db-uri)
              (= :cognitect.anomalies/not-found
                (:cognitect.anomalies/category data)))
          false
          (throw ex))))))


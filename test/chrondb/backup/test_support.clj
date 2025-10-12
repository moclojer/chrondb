(ns chrondb.backup.test-support
  (:require [chrondb.config :as config]
            [chrondb.storage.git.core :as git-core]
            [chrondb.storage.protocol :as protocol]
            [chrondb.test-helpers :as helpers])
  (:import [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]))

(def ^:private default-config-base
  {:git {:default-branch "main"
         :committer-name "Test User"
         :committer-email "test@example.com"
         :push-enabled false}
   :logging {:level :warn}})

(defn- temp-dir []
  (let [dir (Files/createTempDirectory "chrondb-backup-test" (make-array FileAttribute 0))]
    (.. dir toFile getAbsolutePath)))

(defn with-storage [f]
  (let [repo-path (temp-dir)
        data-path (str repo-path "/data")
        backup-path (temp-dir)
        cfg (assoc-in default-config-base [:storage :data-dir] data-path)]
    (with-redefs [config/load-config (constantly cfg)]
      (let [storage (git-core/create-git-storage repo-path data-path)
            ctx {:repository (:repository storage)
                 :data-dir data-path
                 :backup-dir backup-path}]
        (try
          (f {:storage storage
              :ctx (assoc ctx :storage storage)})
          (finally
            (protocol/close storage)
            (helpers/delete-directory repo-path)
            (helpers/delete-directory data-path)
            (helpers/delete-directory backup-path)))))))

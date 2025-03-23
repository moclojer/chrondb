(ns chrondb.tools.dump
  "Tool for dumping all documents from ChronDB storage.
   Useful for debugging and exploring data."
  (:require [chrondb.storage.protocol :as storage]
            [chrondb.storage.git :as git]
            [chrondb.util.logging :as log]
            [clojure.pprint :as pprint]
            [clojure.string :as str]
            [clojure.data.json :as json]
            [chrondb.config :as config]))

(defn -main
  "Entry point for the dump tool. Lists and prints all documents."
  [& args]
  (let [config-map (config/load-config)
        repository-dir (or (get-in config-map [:storage :repository-dir]) "data")
        data-dir (get-in config-map [:storage :data-dir])
        storage (git/create-git-storage repository-dir data-dir)
        prefix (first args)]

    (log/log-info "Connecting to Git repository")

    (try
      (if (and prefix (not (str/blank? prefix)))
        (let [table-name (if (str/ends-with? prefix ":")
                           (subs prefix 0 (dec (count prefix)))
                           prefix)
              _ (log/log-info (str "Listing documents with prefix: '" prefix "'"))
              docs (if (str/ends-with? prefix ":")
                     ;; If ending with ":", it's a table
                     (storage/get-documents-by-table storage table-name)
                     ;; Otherwise, it's a common prefix
                     (storage/get-documents-by-prefix storage prefix))]
          (log/log-info (str "Found " (count docs) " documents"))
          (doseq [doc docs]
            (println)
            (println "-----------------------------------")
            (println "ID:" (:id doc))
            (println (json/write-str doc))))

        ;; If no prefix is provided, list all documents
        (let [_ (log/log-info "Listing documents with prefix: ''")
              docs (storage/get-documents-by-prefix storage "")]
          (log/log-info (str "Found " (count docs) " documents"))
          (doseq [doc docs]
            (println)
            (println "-----------------------------------")
            (println "ID:" (:id doc))
            (println (json/write-str doc)))))

      (finally
        (storage/close storage)
        (log/log-info "Resources released")))))
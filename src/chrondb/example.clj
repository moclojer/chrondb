(ns chrondb.example
  (:require [chrondb.storage.git.core :as git-core]
            [chrondb.index.lucene :as lucene]
            [chrondb.storage.protocol :as storage]
            [chrondb.index.protocol :as index]
            [chrondb.util.logging :as log]
            [clojure.java.io :as io]))

(defn create-chrondb []
  (println "\nCriando ChronDB...")
  (println "Criando diretório data...")
  (io/make-parents "data/index/.keep")

  (println "Criando storage...")
  (let [storage (git-core/create-git-storage "data")
        _ (println "Criando index...")
        index (lucene/create-lucene-index "data/index")]
    {:storage storage
     :index index}))

(defn save-user [chrondb user]
  (let [saved (storage/save-document (:storage chrondb) user)]
    (index/index-document (:index chrondb) saved)
    saved))

(defn search-users [chrondb query]
  (index/search-query (:index chrondb) {:field "name" :value query :analyzer :fts} "main" {:limit 100}))

(defn get-user [chrondb id]
  (storage/get-document (:storage chrondb) id))

(defn delete-user [chrondb id]
  (when (storage/delete-document (:storage chrondb) id)
    (index/delete-document (:index chrondb) id)))

(defn -main [& _]
  (log/init! {:min-level :debug})

  (let [chrondb (create-chrondb)]
    (println "\nGerando e salvando 10 usuários aleatórios...")
    (doseq [i (range 10)]
      (let [user {:id (str "user:" i)
                  :name (str "User " i)
                  :email (str "user" i "@example.com")
                  :age (+ 20 (rand-int 50))}]
        (log/log-info (str "Salvando usuário " i))
        (save-user chrondb user)))

    (println "\nBuscando usuários...")
    (let [results (search-users chrondb "User")]
      (doseq [user results]
        (log/log-info (str "Encontrado: " user))))

    (println "\nRecuperando usuário específico...")
    (when-let [user (get-user chrondb "user:5")]
      (log/log-info (str "Usuário 5: " user)))

    (println "\nDeletando usuário 5...")
    (delete-user chrondb "user:5")

    (println "\nVerificando se usuário 5 foi deletado...")
    (if-let [user (get-user chrondb "user:5")]
      (log/log-warn "Usuário ainda existe!" user)
      (log/log-info "Usuário deletado com sucesso!"))

    (.close (:storage chrondb))
    (.close (:index chrondb))))
# ChronDB Clojure Package Examples

This document provides detailed examples for using the ChronDB native Clojure API, which is the most direct way to interact with ChronDB.

## Setup

First, add ChronDB as a dependency in your project:

### deps.edn

```clojure
{:deps {com.github.moclojer/chrondb {:git/tag "v0.1.0"
                                     :git/sha "..."}}}
```

### Leiningen (project.clj)

```clojure
:dependencies [[com.github.moclojer/chrondb "0.1.0"]]
```

## Basic Usage

### Creating a Database

```clojure
(ns my-app.core
  (:require [chrondb.core :as chrondb]))

;; Create with default configuration (in-memory database)
(def db (chrondb/create-chrondb))

;; Create with custom configuration
(def config {:git {:committer-name "My App"
                  :committer-email "app@example.com"}
             :storage {:data-dir "/path/to/storage"}})
(def db (chrondb/create-chrondb config))
```

### CRUD Operations

```clojure
;; Create/Update documents
(chrondb/save db "user:1" {:name "Alice"
                          :email "alice@example.com"
                          :roles ["admin"]})

(chrondb/save db "user:2" {:name "Bob"
                          :email "bob@example.com"
                          :roles ["user"]})

;; Read a document
(def user (chrondb/get db "user:1"))
(println user) ;; => {:name "Alice", :email "alice@example.com", :roles ["admin"]}

;; Update a document (merge with existing data)
(chrondb/save db "user:1" (assoc user :age 30))

;; Delete a document
(chrondb/delete db "user:2")
(println (chrondb/get db "user:2")) ;; => nil
```

## Searching

ChronDB uses Lucene for powerful search capabilities:

```clojure
;; Basic search by field
(def results (chrondb/search db "name:Alice"))

;; Search with multiple conditions
(def results (chrondb/search db "name:Alice AND age:[25 TO 35]"))

;; Wildcard search
(def results (chrondb/search db "email:*@example.com"))

;; Search with paging
(def page1 (chrondb/search db "roles:admin" {:limit 10 :offset 0}))
(def page2 (chrondb/search db "roles:admin" {:limit 10 :offset 10}))
```

## Version Control Features

### Working with Document History

```clojure
;; Get the history of a document
(def history (chrondb/history db "user:1"))
(println history)
;; => [{:timestamp "2023-05-10T14:30:00Z"
;;      :data {:name "Alice" :email "alice@example.com" :roles ["admin"]}}
;;     {:timestamp "2023-05-11T09:15:00Z"
;;      :data {:name "Alice" :email "alice@example.com" :roles ["admin"] :age 30}}]

;; Get a document at a specific point in time
(def old-version (chrondb/get-at db "user:1" "2023-05-10T14:30:00Z"))
(println old-version) ;; => Version without :age field

;; Compare document versions
(def diff (chrondb/diff db "user:1" "2023-05-10T14:30:00Z" "2023-05-11T09:15:00Z"))
(println diff) ;; => Shows what changed between versions
```

### Working with Branches

```clojure
;; Create a new branch for testing
(def test-db (chrondb/create-branch db "test"))

;; Make changes in the test branch
(chrondb/save test-db "user:1" {:name "Alice (Test)"
                               :email "alice.test@example.com"})

;; The changes are isolated to the test branch
(println (chrondb/get db "user:1")) ;; => Original version
(println (chrondb/get test-db "user:1")) ;; => Modified version

;; Switch the current database to a different branch
(def switched-db (chrondb/switch-branch db "test"))

;; Merge changes from one branch to another
(def merged-db (chrondb/merge-branch db "test" "main"))
```

## Transactions

ChronDB supports atomic transactions:

```clojure
;; Execute multiple operations atomically
(chrondb/with-transaction [db]
  (chrondb/save db "order:1" {:items [{:id "item1" :qty 2}]
                             :total 50.0
                             :status "pending"})

  ;; Update inventory
  (let [inventory (chrondb/get db "inventory:item1")]
    (chrondb/save db "inventory:item1"
                 (update inventory :stock - 2)))

  ;; Update user orders
  (let [user (chrondb/get db "user:1")]
    (chrondb/save db "user:1"
                 (update user :orders conj "order:1"))))

;; If any operation fails, all changes are rolled back
```

## Advanced Features

### Custom Hooks

```clojure
;; Register a pre-save hook for validation
(chrondb/register-hook db :pre-save
  (fn [doc]
    (when (and (contains? doc :email)
               (not (re-matches #".*@.*\..+" (:email doc))))
      (throw (ex-info "Invalid email format" {:doc doc})))
    doc))

;; Register a post-save hook for notifications
(chrondb/register-hook db :post-save
  (fn [doc]
    (println "Document saved:" doc)
    doc))
```

### Database Statistics and Maintenance

```clojure
;; Get database statistics
(def stats (chrondb/stats db))
(println stats)
;; => {:documents 100, :branches 2, :size "10MB", ...}

;; Perform database health check
(def health (chrondb/health-check db))
(println health)
;; => {:status "healthy", :issues []}

;; Create a backup
(chrondb/backup db "/path/to/backup.tar.gz")
```

## Integration with Other Systems

### Creating a REST API Server

```clojure
(ns my-app.server
  (:require [chrondb.core :as chrondb]
            [ring.adapter.jetty :as jetty]
            [compojure.core :refer [defroutes GET POST DELETE]]
            [compojure.route :as route]
            [ring.middleware.json :refer [wrap-json-response wrap-json-body]]
            [ring.util.response :refer [response]]))

(def db (chrondb/create-chrondb))

(defroutes app-routes
  (GET "/documents/:key" [key]
       (response (chrondb/get db key)))

  (POST "/documents/:key" [key :as req]
        (let [doc (:body req)]
          (chrondb/save db key doc)
          (response {:status "success"})))

  (DELETE "/documents/:key" [key]
          (chrondb/delete db key)
          (response {:status "success"}))

  (GET "/search" [q]
       (response (chrondb/search db q)))

  (route/not-found (response {:status "error" :message "Not found"})))

(def app
  (-> app-routes
      wrap-json-response
      (wrap-json-body {:keywords? true})))

(defn start-server []
  (jetty/run-jetty app {:port 3000 :join? false}))
```

## Performance Tips

- Use the appropriate branch strategy for your application
- For large datasets, consider indexing only the fields you search frequently
- Use transactions for operations that need to be atomic
- For read-heavy workloads, consider using a caching layer
- Monitor disk usage regularly, as historical data will grow over time
- Use the compact operation periodically to optimize storage

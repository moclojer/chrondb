(ns chrondb.validation.storage-test
  "Unit tests for validation schema storage operations"
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [clojure.java.io :as io]
            [chrondb.config :as config]
            [chrondb.storage.git.core :as git-core]
            [chrondb.storage.protocol :as protocol]
            [chrondb.validation.storage :as storage])
  (:import [java.io File]))

(def test-repo-path "test-validation-repo")

(def test-config
  {:git {:default-branch "main"
         :committer-name "Test User"
         :committer-email "test@example.com"
         :push-enabled false}
   :logging {:level :info
             :file "test.log"}
   :storage {:data-dir "data"}})

(def test-schema
  {"type" "object"
   "required" ["id" "email"]
   "properties" {"id" {"type" "string"}
                 "email" {"type" "string" "format" "email"}}})

(defn delete-directory [^File directory]
  (when (.exists directory)
    (doseq [file (reverse (file-seq directory))]
      (.delete file))))

(defn clean-test-repo [f]
  (delete-directory (io/file test-repo-path))
  (with-redefs [config/load-config (constantly test-config)]
    (f)))

(use-fixtures :each clean-test-repo)

(deftest test-validation-schema-path
  (testing "Generate correct path for namespace"
    (is (= "_schema/validation/users.json"
           (storage/validation-schema-path "users")))
    (is (= "_schema/validation/myapp.json"
           (storage/validation-schema-path "MyApp")))))

(deftest test-save-and-get-validation-schema
  (testing "Save and retrieve validation schema"
    (let [git-storage (git-core/create-git-storage test-repo-path)
          repository (:repository git-storage)]
      (try
        ;; Save schema
        (let [saved (storage/save-validation-schema
                     repository "users" test-schema :strict "test-user" nil)]
          (is (= "users" (:namespace saved)))
          (is (= 1 (:version saved)))
          (is (= "strict" (:mode saved)))
          (is (= test-schema (:schema saved))))

        ;; Retrieve schema
        (let [retrieved (storage/get-validation-schema repository "users" nil)]
          (is (some? retrieved))
          (is (= "users" (:namespace retrieved)))
          (is (= 1 (:version retrieved)))
          (is (= "strict" (:mode retrieved))))

        (finally
          (protocol/close git-storage))))))

(deftest test-update-validation-schema-increments-version
  (testing "Updating schema increments version"
    (let [git-storage (git-core/create-git-storage test-repo-path)
          repository (:repository git-storage)]
      (try
        ;; Save initial schema
        (storage/save-validation-schema repository "products" test-schema :strict nil nil)

        ;; Update schema
        (let [updated-schema (assoc test-schema "additionalProperties" false)
              saved (storage/save-validation-schema
                     repository "products" updated-schema :warning nil nil)]
          (is (= 2 (:version saved)))
          (is (= "warning" (:mode saved))))

        ;; Verify retrieval
        (let [retrieved (storage/get-validation-schema repository "products" nil)]
          (is (= 2 (:version retrieved))))

        (finally
          (protocol/close git-storage))))))

(deftest test-delete-validation-schema
  (testing "Delete validation schema"
    (let [git-storage (git-core/create-git-storage test-repo-path)
          repository (:repository git-storage)]
      (try
        ;; Save schema
        (storage/save-validation-schema repository "orders" test-schema :strict nil nil)

        ;; Verify it exists
        (is (some? (storage/get-validation-schema repository "orders" nil)))

        ;; Delete schema
        (let [deleted (storage/delete-validation-schema repository "orders" nil)]
          (is deleted))

        ;; Verify it's gone
        (is (nil? (storage/get-validation-schema repository "orders" nil)))

        (finally
          (protocol/close git-storage)))))

  (testing "Delete non-existent schema returns false"
    (let [git-storage (git-core/create-git-storage test-repo-path)
          repository (:repository git-storage)]
      (try
        (is (not (storage/delete-validation-schema repository "nonexistent" nil)))
        (finally
          (protocol/close git-storage))))))

(deftest test-list-validation-schemas
  (testing "List all validation schemas"
    (let [git-storage (git-core/create-git-storage test-repo-path)
          repository (:repository git-storage)]
      (try
        ;; Save multiple schemas
        (storage/save-validation-schema repository "users" test-schema :strict nil nil)
        (storage/save-validation-schema repository "products" test-schema :warning nil nil)
        (storage/save-validation-schema repository "orders" test-schema :disabled nil nil)

        ;; List schemas
        (let [schemas (storage/list-validation-schemas repository nil)]
          (is (= 3 (count schemas)))
          (is (= #{"users" "products" "orders"}
                 (set (map :namespace schemas)))))

        (finally
          (protocol/close git-storage))))))

(deftest test-validation-schema-exists
  (testing "Check if schema exists"
    (let [git-storage (git-core/create-git-storage test-repo-path)
          repository (:repository git-storage)]
      (try
        (is (not (storage/validation-schema-exists? repository "users" nil)))

        (storage/save-validation-schema repository "users" test-schema :strict nil nil)

        (is (storage/validation-schema-exists? repository "users" nil))
        (is (not (storage/validation-schema-exists? repository "products" nil)))

        (finally
          (protocol/close git-storage))))))

(deftest test-get-validation-schema-history
  (testing "Get schema change history"
    (let [git-storage (git-core/create-git-storage test-repo-path)
          repository (:repository git-storage)]
      (try
        ;; Save schema multiple times
        (storage/save-validation-schema repository "users" test-schema :strict nil nil)
        (storage/save-validation-schema repository "users" test-schema :warning nil nil)
        (storage/save-validation-schema repository "users" test-schema :strict nil nil)

        ;; Get history
        (let [history (storage/get-validation-schema-history repository "users" nil)]
          (is (= 3 (count history)))
          (is (every? :commit-id history))
          (is (every? :timestamp history))
          (is (every? :message history)))

        (finally
          (protocol/close git-storage))))))

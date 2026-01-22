(ns chrondb.validation.core-test
  "Unit tests for validation core orchestration"
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [chrondb.config :as config]
            [chrondb.storage.git.core :as git-core]
            [chrondb.storage.protocol :as protocol]
            [chrondb.validation.core :as validation]
            [chrondb.validation.storage :as storage])
  (:import [java.io File]))

(def test-repo-path "test-validation-core-repo")

(def test-config
  {:git {:default-branch "main"
         :committer-name "Test User"
         :committer-email "test@example.com"
         :push-enabled false}
   :logging {:level :info
             :file "test.log"}
   :storage {:data-dir "data"}})

(def user-schema
  {"type" "object"
   "required" ["id" "email"]
   "properties" {"id" {"type" "string"}
                 "email" {"type" "string" "format" "email"}
                 "name" {"type" "string"}
                 "age" {"type" "integer" "minimum" 0}}})

(defn delete-directory [^File directory]
  (when (.exists directory)
    (doseq [file (reverse (file-seq directory))]
      (.delete file))))

(defn clean-test-repo [f]
  (delete-directory (io/file test-repo-path))
  (with-redefs [config/load-config (constantly test-config)]
    (f)))

(use-fixtures :each clean-test-repo)

(deftest test-extract-namespace
  (testing "Extract from :_table"
    (is (= "users" (validation/extract-namespace {:_table "users" :id "1"}))))

  (testing "Extract from :id prefix"
    (is (= "users" (validation/extract-namespace {:id "users:123"}))))

  (testing "Returns nil for document without namespace"
    (is (nil? (validation/extract-namespace {:id "simple-id" :name "test"})))))

(deftest test-validation-enabled
  (testing "Validation disabled when no schema exists"
    (let [git-storage (git-core/create-git-storage test-repo-path)
          repository (:repository git-storage)]
      (try
        (is (not (validation/validation-enabled? repository "users" nil)))
        (finally
          (protocol/close git-storage)))))

  (testing "Validation enabled when schema exists with strict mode"
    (let [git-storage (git-core/create-git-storage test-repo-path)
          repository (:repository git-storage)]
      (try
        (storage/save-validation-schema repository "users" user-schema :strict nil nil)
        (is (validation/validation-enabled? repository "users" nil))
        (finally
          (protocol/close git-storage)))))

  (testing "Validation disabled when schema exists with disabled mode"
    (let [git-storage (git-core/create-git-storage test-repo-path)
          repository (:repository git-storage)]
      (try
        (storage/save-validation-schema repository "users" user-schema :disabled nil nil)
        (is (not (validation/validation-enabled? repository "users" nil)))
        (finally
          (protocol/close git-storage))))))

(deftest test-validate-document
  (testing "Valid document passes validation"
    (let [git-storage (git-core/create-git-storage test-repo-path)
          repository (:repository git-storage)]
      (try
        (storage/save-validation-schema repository "users" user-schema :strict nil nil)
        (let [doc {:_table "users" :id "users:1" :email "test@example.com" :name "Test"}
              result (validation/validate-document repository doc nil)]
          (is (:valid? result))
          (is (empty? (:errors result))))
        (finally
          (protocol/close git-storage)))))

  (testing "Invalid document fails validation"
    (let [git-storage (git-core/create-git-storage test-repo-path)
          repository (:repository git-storage)]
      (try
        (storage/save-validation-schema repository "users" user-schema :strict nil nil)
        (let [doc {:_table "users" :id "users:1" :name "Test"} ;; missing email
              result (validation/validate-document repository doc nil)]
          (is (not (:valid? result)))
          (is (seq (:errors result))))
        (finally
          (protocol/close git-storage)))))

  (testing "Document without namespace schema passes"
    (let [git-storage (git-core/create-git-storage test-repo-path)
          repository (:repository git-storage)]
      (try
        (let [doc {:_table "products" :id "products:1" :name "Widget"}
              result (validation/validate-document repository doc nil)]
          (is (:valid? result)))
        (finally
          (protocol/close git-storage))))))

(deftest test-validate-if-enabled
  (testing "Returns nil when no schema exists"
    (let [git-storage (git-core/create-git-storage test-repo-path)
          repository (:repository git-storage)]
      (try
        (let [doc {:_table "products" :id "products:1" :name "Widget"}
              result (validation/validate-if-enabled repository doc nil)]
          (is (nil? result)))
        (finally
          (protocol/close git-storage)))))

  (testing "Returns validation result when schema exists"
    (let [git-storage (git-core/create-git-storage test-repo-path)
          repository (:repository git-storage)]
      (try
        (storage/save-validation-schema repository "users" user-schema :strict nil nil)
        (let [doc {:_table "users" :id "users:1" :email "test@example.com"}
              result (validation/validate-if-enabled repository doc nil)]
          (is (some? result))
          (is (:valid? result)))
        (finally
          (protocol/close git-storage))))))

(deftest test-validate-and-throw
  (testing "Does not throw for valid document"
    (let [git-storage (git-core/create-git-storage test-repo-path)
          repository (:repository git-storage)]
      (try
        (storage/save-validation-schema repository "users" user-schema :strict nil nil)
        (let [doc {:_table "users" :id "users:1" :email "test@example.com"}]
          ;; Should not throw
          (validation/validate-and-throw repository doc nil))
        (finally
          (protocol/close git-storage)))))

  (testing "Throws for invalid document in strict mode"
    (let [git-storage (git-core/create-git-storage test-repo-path)
          repository (:repository git-storage)]
      (try
        (storage/save-validation-schema repository "users" user-schema :strict nil nil)
        (let [doc {:_table "users" :id "users:1" :name "Test"}] ;; missing email
          (is (thrown-with-msg? clojure.lang.ExceptionInfo
                                #"Document validation failed"
                                (validation/validate-and-throw repository doc nil))))
        (finally
          (protocol/close git-storage)))))

  (testing "Does not throw for invalid document in warning mode"
    (let [git-storage (git-core/create-git-storage test-repo-path)
          repository (:repository git-storage)]
      (try
        (storage/save-validation-schema repository "users" user-schema :warning nil nil)
        (let [doc {:_table "users" :id "users:1" :name "Test"}] ;; missing email
          ;; Should not throw in warning mode
          (validation/validate-and-throw repository doc nil))
        (finally
          (protocol/close git-storage))))))

(deftest test-dry-run-validate
  (testing "Dry run validates document without saving"
    (let [git-storage (git-core/create-git-storage test-repo-path)
          repository (:repository git-storage)]
      (try
        (storage/save-validation-schema repository "users" user-schema :strict nil nil)

        ;; Valid document
        (let [result (validation/dry-run-validate
                      repository "users"
                      {:id "users:1" :email "test@example.com"}
                      nil)]
          (is (:valid result))
          (is (empty? (:errors result))))

        ;; Invalid document
        (let [result (validation/dry-run-validate
                      repository "users"
                      {:id "users:1" :name "No Email"}
                      nil)]
          (is (not (:valid result)))
          (is (seq (:errors result))))

        (finally
          (protocol/close git-storage)))))

  (testing "Dry run with JSON string document"
    (let [git-storage (git-core/create-git-storage test-repo-path)
          repository (:repository git-storage)]
      (try
        (storage/save-validation-schema repository "users" user-schema :strict nil nil)

        (let [result (validation/dry-run-validate
                      repository "users"
                      "{\"id\": \"users:1\", \"email\": \"test@example.com\"}"
                      nil)]
          (is (:valid result)))

        (finally
          (protocol/close git-storage)))))

  (testing "Dry run returns error for non-existent schema"
    (let [git-storage (git-core/create-git-storage test-repo-path)
          repository (:repository git-storage)]
      (try
        (let [result (validation/dry-run-validate
                      repository "nonexistent"
                      {:id "1"}
                      nil)]
          (is (not (:valid result)))
          (is (some #(str/includes? (:message %) "No validation schema") (:errors result))))
        (finally
          (protocol/close git-storage))))))

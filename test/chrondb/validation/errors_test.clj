(ns chrondb.validation.errors-test
  "Unit tests for validation error formatting"
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.string :as str]
            [chrondb.validation.errors :as errors]
            [chrondb.validation.schema :as schema]))

(def test-schema
  {"type" "object"
   "required" ["id" "email"]
   "properties" {"id" {"type" "string"}
                 "email" {"type" "string"}}})

(deftest test-format-validation-error
  (testing "Format validation error from actual validator"
    ;; Create a real validation error using the schema validator
    (let [validator (schema/parse-json-schema test-schema)
          doc {:id "1"} ;; missing email
          errors (schema/validate-against-schema validator doc)]
      (is (seq errors))
      (let [formatted (first errors)]
        (is (string? (:path formatted)))
        (is (string? (:message formatted)))
        (is (some? (:keyword formatted)))))))

(deftest test-validation-exception
  (testing "Create validation exception"
    (let [errors [{:path "$.email" :message "required" :keyword "required"}]
          ex (errors/validation-exception "users" "user:1" errors :strict)]
      (is (instance? clojure.lang.ExceptionInfo ex))
      (is (= "Document validation failed" (.getMessage ex)))
      (let [data (ex-data ex)]
        (is (= :validation-error (:type data)))
        (is (= "users" (:namespace data)))
        (is (= "user:1" (:document-id data)))
        (is (= :strict (:mode data)))
        (is (= errors (:violations data)))))))

(deftest test-validation-error-response
  (testing "Create validation error response for REST"
    (let [errors [{:path "$.email" :message "required" :keyword "required"}]
          response (errors/validation-error-response "users" "user:1" errors :strict)]
      (is (= "VALIDATION_ERROR" (:error response)))
      (is (= "users" (:namespace response)))
      (is (= "user:1" (:document_id response)))
      (is (= "strict" (:mode response)))
      (is (= errors (:violations response))))))

(deftest test-format-redis-error
  (testing "Format validation error for Redis protocol"
    (let [errors [{:path "$.email" :message "required property 'email' not found" :keyword "required"}
                  {:path "$.age" :message "must be >= 0" :keyword "minimum"}]
          formatted (errors/format-redis-error "users" errors)]
      (is (string? formatted))
      (is (str/includes? formatted "users"))
      (is (str/includes? formatted "email")))))

(deftest test-format-sql-error
  (testing "Format validation error for SQL protocol"
    (let [errors [{:path "$.email" :message "required property 'email' not found" :keyword "required"}]
          formatted (errors/format-sql-error "users" errors)]
      (is (string? formatted))
      (is (str/includes? formatted "Validation failed"))
      (is (str/includes? formatted "users"))
      (is (str/includes? formatted "email")))))

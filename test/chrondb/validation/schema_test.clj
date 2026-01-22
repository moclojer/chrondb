(ns chrondb.validation.schema-test
  "Unit tests for JSON Schema parsing and validation"
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.string :as str]
            [chrondb.validation.schema :as schema]))

(def simple-schema
  {"type" "object"
   "required" ["id" "email"]
   "properties" {"id" {"type" "string"}
                 "email" {"type" "string" "format" "email"}
                 "name" {"type" "string"}}})

(def schema-with-draft
  {"$schema" "http://json-schema.org/draft-07/schema#"
   "type" "object"
   "required" ["id"]
   "properties" {"id" {"type" "string"}}})

(deftest test-parse-json-schema
  (testing "Parse simple schema from map"
    (let [validator (schema/parse-json-schema simple-schema)]
      (is (some? validator))))

  (testing "Parse schema from JSON string"
    (let [json-str "{\"type\": \"object\", \"required\": [\"id\"]}"
          validator (schema/parse-json-schema json-str)]
      (is (some? validator))))

  (testing "Parse schema with explicit draft version"
    (let [validator (schema/parse-json-schema schema-with-draft)]
      (is (some? validator)))))

(deftest test-validate-against-schema
  (testing "Valid document passes validation"
    (let [validator (schema/parse-json-schema simple-schema)
          doc {:id "user:1" :email "test@example.com" :name "Test User"}
          errors (schema/validate-against-schema validator doc)]
      (is (empty? errors))))

  (testing "Missing required field fails validation"
    (let [validator (schema/parse-json-schema simple-schema)
          doc {:id "user:1" :name "Test User"}
          errors (schema/validate-against-schema validator doc)]
      (is (seq errors))
      (is (= 1 (count errors)))
      (is (some #(str/includes? (:message %) "email") errors))))

  (testing "Multiple validation errors"
    (let [validator (schema/parse-json-schema simple-schema)
          doc {:name "Test User"}
          errors (schema/validate-against-schema validator doc)]
      (is (seq errors))
      (is (= 2 (count errors))))))

(deftest test-schema-cache
  (testing "Schema caching works correctly"
    (let [schema-def {:schema simple-schema :version 1}]
      ;; Clear cache first
      (schema/invalidate-cache "test-namespace" "main")

      ;; First call should compile and cache
      (let [validator1 (schema/get-or-compile-schema "test-namespace" "main" schema-def)]
        (is (some? validator1)))

      ;; Second call should return cached validator
      (let [validator2 (schema/get-or-compile-schema "test-namespace" "main" schema-def)]
        (is (some? validator2)))

      ;; Clean up
      (schema/invalidate-cache "test-namespace" "main"))))

(deftest test-schema-cache-invalidation
  (testing "Cache invalidation works"
    (let [schema-def {:schema simple-schema :version 1}]
      ;; Compile and cache
      (schema/get-or-compile-schema "test-cache" "main" schema-def)

      ;; Invalidate
      (schema/invalidate-cache "test-cache" "main")

      ;; Should recompile after invalidation
      (let [validator (schema/get-or-compile-schema "test-cache" "main" schema-def)]
        (is (some? validator)))

      ;; Clean up
      (schema/invalidate-cache "test-cache" "main"))))

(deftest test-complex-schema-validation
  (testing "Validate with nested objects"
    (let [nested-schema {"type" "object"
                         "required" ["user"]
                         "properties" {"user" {"type" "object"
                                               "required" ["name"]
                                               "properties" {"name" {"type" "string"}
                                                             "age" {"type" "integer" "minimum" 0}}}}}
          validator (schema/parse-json-schema nested-schema)
          valid-doc {:user {:name "John" :age 30}}
          invalid-doc {:user {:age -5}}]

      (is (empty? (schema/validate-against-schema validator valid-doc)))
      (let [errors (schema/validate-against-schema validator invalid-doc)]
        (is (seq errors)))))

  (testing "Validate with array items"
    (let [array-schema {"type" "object"
                        "properties" {"tags" {"type" "array"
                                              "items" {"type" "string"}}}}
          validator (schema/parse-json-schema array-schema)
          valid-doc {:tags ["a" "b" "c"]}
          invalid-doc {:tags [1 2 3]}]

      (is (empty? (schema/validate-against-schema validator valid-doc)))
      (let [errors (schema/validate-against-schema validator invalid-doc)]
        (is (seq errors))))))

(ns chrondb.validation.errors
  "Error formatting and exception handling for JSON Schema validation"
  (:require [clojure.string :as str]))

(defn format-validation-error
  "Format a single validation error from json-schema-validator.
   Parameters:
   - error: A ValidationMessage from the validator
   Returns: A map with :path, :message, and :keyword"
  [error]
  {:path (.getPath error)
   :message (.getMessage error)
   :keyword (.getType error)})

(defn format-validation-errors
  "Format a collection of validation errors.
   Parameters:
   - errors: Collection of ValidationMessage objects
   Returns: Vector of formatted error maps"
  [errors]
  (mapv format-validation-error errors))

(defn validation-exception
  "Create a structured ex-info for validation errors.
   This exception can be caught by protocol handlers (REST, Redis, SQL)
   and formatted appropriately for each wire protocol.
   Parameters:
   - namespace: The table/namespace that failed validation
   - document-id: The ID of the document that failed
   - errors: Vector of formatted error maps
   - mode: The validation mode (:strict or :warning)
   Returns: An ex-info exception"
  [namespace document-id errors mode]
  (ex-info "Document validation failed"
           {:type :validation-error
            :namespace namespace
            :document-id document-id
            :mode mode
            :violations errors}))

(defn validation-error-response
  "Build a response map for validation errors.
   Useful for REST API responses.
   Parameters:
   - namespace: The table/namespace that failed validation
   - document-id: The ID of the document that failed
   - errors: Vector of formatted error maps
   - mode: The validation mode
   Returns: A map suitable for JSON response"
  [namespace document-id errors mode]
  {:error "VALIDATION_ERROR"
   :namespace namespace
   :document_id document-id
   :mode (name mode)
   :violations errors})

(defn format-redis-error
  "Format validation errors for Redis RESP protocol.
   Parameters:
   - namespace: The table/namespace that failed validation
   - errors: Vector of formatted error maps
   Returns: A string suitable for Redis error response"
  [namespace errors]
  (let [first-error (first errors)
        msg (if first-error
              (str (:message first-error) " at " (:path first-error))
              "validation failed")]
    (str "VALIDATION_ERROR " namespace ": " msg)))

(defn format-sql-error
  "Format validation errors for PostgreSQL protocol.
   Parameters:
   - namespace: The table/namespace that failed validation
   - errors: Vector of formatted error maps
   Returns: A string suitable for SQL error message"
  [namespace errors]
  (let [error-details (->> errors
                           (map #(str (:path %) " - " (:message %)))
                           (str/join "; "))]
    (str "Validation failed for table '" namespace "': " error-details)))

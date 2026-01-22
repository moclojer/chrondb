(ns chrondb.validation.core
  "Core validation orchestration - validates documents against JSON Schemas"
  (:require [chrondb.validation.storage :as storage]
            [chrondb.validation.schema :as schema]
            [chrondb.validation.errors :as errors]
            [chrondb.util.logging :as log]
            [clojure.string :as str]
            [clojure.data.json :as json]))

(defn extract-namespace
  "Extract the namespace (table) from a document.
   Uses :_table if present, otherwise extracts from :id prefix.
   Parameters:
   - document: The document map
   Returns: The namespace string or nil"
  [document]
  (or (:_table document)
      (when-let [id (:id document)]
        (let [parts (str/split (str id) #":")]
          (when (> (count parts) 1)
            (first parts))))))

(defn validation-enabled?
  "Check if validation is enabled for a namespace.
   Parameters:
   - repository: The Git repository
   - namespace: The table/namespace name
   - branch: The git branch
   Returns: true if validation schema exists and mode is not :disabled"
  [repository namespace branch]
  (when-let [schema-def (storage/get-validation-schema repository namespace branch)]
    (not= (:mode schema-def) "disabled")))

(defn get-validation-mode
  "Get the validation mode for a namespace.
   Parameters:
   - repository: The Git repository
   - namespace: The table/namespace name
   - branch: The git branch
   Returns: Keyword :strict, :warning, :disabled, or nil if no schema"
  [repository namespace branch]
  (when-let [schema-def (storage/get-validation-schema repository namespace branch)]
    (keyword (:mode schema-def))))

(defn validate-document
  "Validate a document against its namespace's schema.
   Parameters:
   - repository: The Git repository
   - document: The document to validate
   - branch: The git branch
   Returns: Map with :valid?, :errors, :namespace, and :mode"
  [repository document branch]
  (let [namespace (extract-namespace document)]
    (if-not namespace
      {:valid? true
       :errors []
       :namespace nil
       :mode nil}
      (if-let [schema-def (storage/get-validation-schema repository namespace branch)]
        (let [mode (keyword (:mode schema-def))
              validator (schema/get-or-compile-schema namespace branch schema-def)
              validation-errors (schema/validate-against-schema validator document)]
          {:valid? (empty? validation-errors)
           :errors validation-errors
           :namespace namespace
           :mode mode})
        {:valid? true
         :errors []
         :namespace namespace
         :mode nil}))))

(defn validate-if-enabled
  "Validate a document only if validation is enabled for its namespace.
   This is the main entry point for document.clj integration.
   Parameters:
   - repository: The Git repository
   - document: The document to validate
   - branch: The git branch
   Returns: Validation result map or nil if no validation configured"
  [repository document branch]
  (let [namespace (extract-namespace document)]
    (when (and namespace (storage/validation-schema-exists? repository namespace branch))
      (validate-document repository document branch))))

(defn validate-and-throw
  "Validate a document and throw if invalid in strict mode.
   This is a convenience function for use in save operations.
   Parameters:
   - repository: The Git repository
   - document: The document to validate
   - branch: The git branch
   Throws: ExceptionInfo if validation fails in strict mode
   Returns: nil if valid or warning mode, throws otherwise"
  [repository document branch]
  (when-let [result (validate-if-enabled repository document branch)]
    (when (and (not (:valid? result)) (= :strict (:mode result)))
      (throw (errors/validation-exception
               (:namespace result)
               (:id document)
               (:errors result)
               :strict)))
    (when (and (not (:valid? result)) (= :warning (:mode result)))
      (log/log-warn (str "Document validation warning for " (:namespace result)
                         "/" (:id document) ": "
                         (->> (:errors result)
                              (map #(str (:path %) " - " (:message %)))
                              (str/join "; ")))))))

(defn dry-run-validate
  "Perform a dry-run validation without saving.
   Useful for the validate endpoint.
   Parameters:
   - repository: The Git repository
   - namespace: The table/namespace name
   - document: The document to validate (can be JSON string or map)
   - branch: The git branch
   Returns: Map with :valid and :errors"
  [repository namespace document branch]
  (if-let [schema-def (storage/get-validation-schema repository namespace branch)]
    (let [validator (schema/get-or-compile-schema namespace branch schema-def)
          doc (if (string? document)
                (json/read-str document :key-fn keyword)
                document)
          validation-errors (schema/validate-against-schema validator doc)]
      {:valid (empty? validation-errors)
       :errors validation-errors
       :namespace namespace
       :mode (:mode schema-def)})
    {:valid false
     :errors [{:path "$"
               :message (str "No validation schema found for namespace: " namespace)
               :keyword "schema-not-found"}]
     :namespace namespace
     :mode nil}))

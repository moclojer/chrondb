(ns chrondb.validation.schema
  "JSON Schema parsing, caching, and validation using networknt/json-schema-validator"
  (:require [clojure.data.json :as json]
            [clojure.string :as str]
            [chrondb.config :as config]
            [chrondb.validation.errors :as errors]
            [chrondb.util.logging :as log])
  (:import [com.networknt.schema JsonSchemaFactory SpecVersion$VersionFlag]
           [com.fasterxml.jackson.databind ObjectMapper]))

;; Cache for compiled JSON Schema validators
;; Key: [namespace branch] -> {:schema JsonSchema :version int :updated-at Instant}
(defonce ^:private schema-cache (atom {}))

(def ^:private object-mapper (ObjectMapper.))

(defn- detect-schema-version
  "Detect the JSON Schema version from the $schema field.
   Returns a SpecVersion$VersionFlag or defaults to DRAFT_07."
  [schema-map]
  (let [schema-uri (get schema-map "$schema" "")]
    (cond
      (str/includes? schema-uri "draft-07") SpecVersion$VersionFlag/V7
      (str/includes? schema-uri "draft/2019-09") SpecVersion$VersionFlag/V201909
      (str/includes? schema-uri "draft/2020-12") SpecVersion$VersionFlag/V202012
      :else SpecVersion$VersionFlag/V7)))

(defn parse-json-schema
  "Parse a JSON Schema string or map into a compiled JsonSchema validator.
   Parameters:
   - schema: Either a JSON string or a Clojure map representing the schema
   Returns: A compiled JsonSchema validator"
  [schema]
  (let [schema-map (if (string? schema)
                     (json/read-str schema)
                     schema)
        version (detect-schema-version schema-map)
        factory (JsonSchemaFactory/getInstance version)
        schema-json (.readTree object-mapper (json/write-str schema-map))]
    (.getSchema factory schema-json)))

(defn validate-against-schema
  "Validate a document against a compiled JSON Schema.
   Parameters:
   - schema: A compiled JsonSchema validator
   - document: The document to validate (map or JSON string)
   Returns: Sequence of formatted error maps, empty if valid"
  [schema document]
  (try
    (let [doc-json (if (string? document)
                     (.readTree object-mapper document)
                     (.readTree object-mapper (json/write-str document)))
          validation-result (.validate schema doc-json)]
      (if (empty? validation-result)
        []
        (errors/format-validation-errors validation-result)))
    (catch Exception e
      (log/log-error (str "Error during schema validation: " (.getMessage e)))
      [{:path "$"
        :message (str "Validation error: " (.getMessage e))
        :keyword "error"}])))

(defn cache-key
  "Generate a cache key for a namespace and branch.
   Parameters:
   - namespace: The table/namespace name
   - branch: The git branch
   Returns: A vector [namespace branch]"
  [namespace branch]
  (let [default-branch (get-in (config/load-config) [:git :default-branch] "main")]
    [(or namespace "") (or branch default-branch)]))

(defn get-cached-schema
  "Get a cached schema validator if available.
   Parameters:
   - namespace: The table/namespace name
   - branch: The git branch
   Returns: The cached entry or nil"
  [namespace branch]
  (get @schema-cache (cache-key namespace branch)))

(defn cache-schema
  "Cache a compiled schema validator.
   Parameters:
   - namespace: The table/namespace name
   - branch: The git branch
   - schema-validator: The compiled JsonSchema
   - version: Schema version number
   Returns: The cached entry"
  [namespace branch schema-validator version]
  (let [key (cache-key namespace branch)
        entry {:schema schema-validator
               :version version
               :updated-at (java.time.Instant/now)}]
    (swap! schema-cache assoc key entry)
    entry))

(defn invalidate-cache
  "Remove a schema from the cache.
   Parameters:
   - namespace: The table/namespace name
   - branch: The git branch (optional, invalidates all branches if nil)"
  ([namespace]
   (swap! schema-cache
          (fn [cache]
            (into {} (remove (fn [[[ns _] _]] (= ns namespace)) cache)))))
  ([namespace branch]
   (swap! schema-cache dissoc (cache-key namespace branch))))

(defn clear-cache
  "Clear the entire schema cache."
  []
  (reset! schema-cache {}))

(defn get-or-compile-schema
  "Get a cached schema or compile and cache a new one.
   Parameters:
   - namespace: The table/namespace name
   - branch: The git branch
   - schema-def: The schema definition map (with :schema and :version keys)
   Returns: The compiled JsonSchema validator"
  [namespace branch schema-def]
  (let [cached (get-cached-schema namespace branch)
        schema-version (or (:version schema-def) 1)]
    (if (and cached (= (:version cached) schema-version))
      (:schema cached)
      (let [compiled (parse-json-schema (:schema schema-def))]
        (cache-schema namespace branch compiled schema-version)
        compiled))))

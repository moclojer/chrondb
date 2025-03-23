(ns chrondb.tools.diagnose
  (:require [clojure.string :as str]
            [chrondb.util.logging :as log]))

(defn- encode-path
  "Encode document ID for safe use in file paths."
  [id]
  (-> id
      (str/replace ":" "_COLON_")
      (str/replace "/" "_SLASH_")
      (str/replace "?" "_QMARK_")
      (str/replace "*" "_STAR_")
      (str/replace "\\" "_BSLASH_")
      (str/replace "<" "_LT_")
      (str/replace ">" "_GT_")
      (str/replace "|" "_PIPE_")
      (str/replace "\"" "_QUOTE_")
      (str/replace "%" "_PERCENT_")
      (str/replace "#" "_HASH_")
      (str/replace "&" "_AMP_")
      (str/replace "=" "_EQ_")
      (str/replace "+" "_PLUS_")
      (str/replace "@" "_AT_")
      (str/replace " " "_SPACE_")))

(defn- decode-path
  "Decode file path back to document ID."
  [path]
  (-> path
      (str/replace #"\.json$" "")
      (str/replace "_COLON_" ":")
      (str/replace "_SLASH_" "/")
      (str/replace "_QMARK_" "?")
      (str/replace "_STAR_" "*")
      (str/replace "_BSLASH_" "\\")
      (str/replace "_LT_" "<")
      (str/replace "_GT_" ">")
      (str/replace "_PIPE_" "|")
      (str/replace "_QUOTE_" "\"")
      (str/replace "_PERCENT_" "%")
      (str/replace "_HASH_" "#")
      (str/replace "_AMP_" "&")
      (str/replace "_EQ_" "=")
      (str/replace "_PLUS_" "+")
      (str/replace "_AT_" "@")
      (str/replace "_SPACE_" " ")))

(defn- check-prefix
  "Verifica se um caminho decodificado começa com um prefixo."
  [path prefix]
  (let [encoded-path (encode-path path)
        encoded-file (str encoded-path ".json")
        decoded-path (decode-path encoded-file)]
    (println "Original path:" path)
    (println "Encoded path:" encoded-path)
    (println "Encoded file:" encoded-file)
    (println "Decoded path:" decoded-path)
    (println "Prefix:" prefix)
    (println "startsWith result:" (.startsWith decoded-path prefix))))

(defn -main
  "Utilitário de diagnóstico para verificar caminhos no repositório Git.
   Args:
   - path: O ID do documento para testar
   - prefix: O prefixo para verificar"
  [& args]
  (log/init! {:min-level :info})

  (if (< (count args) 2)
    (println "Uso: clojure -M -m chrondb.tools.diagnose <path> <prefix>")
    (let [path (first args)
          prefix (second args)]
      (println "\n=== Teste de codificação/decodificação de caminho ===")
      (check-prefix path prefix))))
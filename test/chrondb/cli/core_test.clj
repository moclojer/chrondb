(ns chrondb.cli.core-test
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.data.json :as json]
            [chrondb.cli.core :as core]
            [chrondb.cli.http :as http]))

(deftest run-init-prints-info
  (testing "run-init prints JSON response"
    (let [response {:status 200 :body {:default-branch "main"}}
          output (with-out-str
                   (with-redefs [http/init! (fn [cfg]
                                              (is (= "127.0.0.1" (:host cfg)))
                                              response)]
                     (core/run-init {:host "127.0.0.1"} [] {})))]
      (is (re-find #"default-branch" output)))))

(deftest run-put-with-data
  (testing "run-put sends document with ID"
    (let [sent (atom nil)
          response {:status 200 :body {:id "user:1" :name "Alice"}}
          payload (json/write-str {:name "Alice"})
          output (with-out-str
                   (with-redefs [http/put-document (fn [_cfg doc params]
                                                     (reset! sent [doc params])
                                                     response)]
                     (core/run-put {:host "localhost"}
                                   ["user:1"]
                                   {:data payload})))]
      (is (= [{:id "user:1" :name "Alice"} {:branch nil}] @sent))
      (is (re-find #"Alice" output)))))

(deftest run-export-to-file
  (testing "run-export writes file when --output is provided"
    (let [tmp (java.io.File/createTempFile "chrondb-cli-test" ".json")
          response {:status 200 :body {:count 1 :documents [{:id "a"}]}}
          output (with-out-str
                   (with-redefs [http/export-documents (fn [_ _] response)]
                     (core/run-export {:host "localhost"} [] {:output (.getAbsolutePath tmp)})))]
      (is (.exists tmp))
      (is (re-find #"Export written" output))
      (.delete tmp))))

(deftest run-import-missing-documents
  (testing "run-import exits when no documents are provided"
    (with-redefs [core/exit! (fn [status msg]
                               (throw (ex-info msg {:status status})))]
      (is (thrown-with-msg? clojure.lang.ExceptionInfo
                            #"No documents provided"
                            (core/run-import {:host "localhost"} [] {}))))))

(deftest run-import-with-stdin
  (testing "run-import reads stdin when available"
    (let [docs (atom nil)
          response {:status 200 :body {:imported 1}}
          payload "[{\"id\":\"user:1\",\"name\":\"Alice\"}]"]
      (with-redefs [http/import-documents (fn [_cfg documents _params]
                                            (reset! docs documents)
                                            response)]
        (binding [*in* (java.io.StringReader. payload)]
          (let [output (with-out-str (core/run-import {:host "localhost"} [] {}))]
            (is (= [{:id "user:1" :name "Alice"}] @docs))
            (is (re-find #"imported" output))))))))

(ns chrondb.storage.git-test
  "Tests for git-based storage implementation.
   This file loads all the modular tests for each component."
  (:require [chrondb.storage.git.core-test]
            [chrondb.storage.git.path-test]
            [chrondb.storage.git.document-test]
            [chrondb.storage.git.commit-test]
            [chrondb.storage.git.history-test]
            [clojure.test :refer [deftest is testing run-tests]]))

(deftest test-verify-all-tests-loaded
  (testing "This is just a placeholder. The actual tests are in the required namespaces."
    (is true)))
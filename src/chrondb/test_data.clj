(ns chrondb.test-data
  "Test data generation for ChronDB.
   Provides sample data and test user information for testing purposes."
  (:require [talltale.core :as faker]))

(def test-search-username
  "A randomly generated username for testing search functionality.
   This username is used as a known value that can be searched for in tests."
  ((faker/person :en) :username))

(def sample-data
  "A collection of sample user data for testing.
   Each user has a randomly generated username and age."
  [{:username ((faker/person :en) :username)
    :age      ((faker/person :en) :age)}
   {:username ((faker/person :en) :username)
    :age      ((faker/person :en) :age)}
   {:username ((faker/person :en) :username)
    :age      ((faker/person :en) :age)}
   {:username ((faker/person :en) :username)
    :age      ((faker/person :en) :age)}
   {:username ((faker/person :en) :username)
    :age      ((faker/person :en) :age)}])

(def sample-data-with-test-user
  "Sample data extended with a test user.
   Includes all sample data plus an additional user with the test search username."
  (conj sample-data
        {:username test-search-username
         :age      ((faker/person :en) :age)}))
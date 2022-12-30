(ns prpr.test-test
  (:require
   [prpr.test :as sut :refer [deftest testing is]]))

(deftest deftest-test
  (testing "a deftest does test"
    (is (= true true))))

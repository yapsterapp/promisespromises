(ns prpr.promise.platform-test
  (:require
   [prpr.promise.platform :as sut]
   [cljs.test :as t :refer-macros [deftest is testing run-tests async]]))

(deftest pr-branch-success-test
  (testing "success branch is called"
    (async done
           (sut/pr-chain
            (sut/pr-branch
             (sut/pr-success 0)
             inc
             identity)
            (fn [v]
              (is (= 1 v))
              (done))))))

(deftest pr-branch-error-test
  (testing "error branch is called"
    (async done
           (sut/pr-chain
            (sut/pr-branch
             (sut/pr-error ::oops)
             inc
             (fn [e] [::caught e]))
            (fn [v]
              (is (= [::caught ::oops] v))
              (done))))))

(ns prpr.promise.retry-test
  (:require
   [clojure.test :refer [deftest testing is]]
   [prpr.test :as prpr.test]
   [manifold.deferred :as d]
   [prpr.promise.retry :as sut]))

(deftest retry-test
  (prpr.test/with-log-level :error
    (testing "returns an immediately successful result"
      (let [f (fn [] (d/success-deferred ::ok))
            r @(sut/retry f ::retry-test 1 0)]
        (is (= ::ok r))))
    (testing "works with 0 retries"
      (let [f (fn [] (d/success-deferred ::ok))
            r @(sut/retry f ::retry-test 0 0)]
        (is (= ::ok r))))
    (testing "retries as specified"
      (let [cnt (atom 0)
            f (fn [] (if (> (swap! cnt inc) 2)
                      (d/success-deferred ::ok)
                      (d/error-deferred (ex-info "boo" {:boo "boo"}))))
            r @(sut/retry f ::retry-test 2 0)]
        (is (= ::ok r))))
    (testing "fails when out of retries"
      (let [x (ex-info "boo" {:boo "boo"})
            cnt (atom 0)
            f (fn [] (if (> (swap! cnt inc) 2)
                      (d/success-deferred ::ok)
                      (d/error-deferred x)))
            r-d (sut/retry f ::retry-test 1 0)]
        (is (= x (d/error-value r-d ::no-error)))))))

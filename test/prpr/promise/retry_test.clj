(ns prpr.promise.retry-test
  (:require
   [clojure.test :refer [deftest testing is]]
   [prpr.test :as prpr.test]
   [manifold.deferred :as d]
   [prpr.promise.retry :as sut]))

(deftest retry-n-test
  (prpr.test/with-log-level :error
    (testing "returns an immediately successful result"
      (let [f (fn [n]
                (is (= n 0))
                (d/success-deferred ::ok))

            r @(sut/retry-n f ::retry-test 1 0)]
        (is (= ::ok r))))
    (testing "works with 0 retries"
      (let [f (fn [n]
                (is (= n 0))
                (d/success-deferred ::ok))
            r @(sut/retry-n f ::retry-test 0 0)]
        (is (= ::ok r))))
    (testing "retries as specified"
      (let [ns (atom #{0 1 2})
            cnt (atom 0)
            f (fn [n]
                (swap! ns disj n)
                (if (> (swap! cnt inc) 2)
                  (d/success-deferred ::ok)
                  (d/error-deferred (ex-info "boo" {:boo "boo"}))))
            r @(sut/retry-n f ::retry-test 2 0)]
        (is (empty? @ns))
        (is (= ::ok r))))
    (testing "fails when out of retries"
      (let [x (ex-info "boo" {:boo "boo"})
            ns (atom #{0 1})
            cnt (atom 0)
            f (fn [n]
                (swap! ns disj n)
                (if (> (swap! cnt inc) 2)
                  (d/success-deferred ::ok)
                  (d/error-deferred x)))
            r-d (sut/retry-n f ::retry-test 1 0)]
        (is (empty? @ns))
        (is (= x (d/error-value r-d ::no-error)))))))

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

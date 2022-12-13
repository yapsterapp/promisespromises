(ns prpr.promise.retry-test
  (:require
   [prpr.test :refer [deftest testing is with-log-level]]
   [promesa.core :as pr]
   [prpr.promise.retry :as sut]))

(defn merge-pr
  [v]
  (pr/handle
   v
   (fn [v e]
     (if (some? e)
       [::error e]
       [::ok v]))))

(deftest retry-n-test
  (with-log-level :error
    (testing "returns an immediately successful result"
      (let [f (fn [n]
                (is (= n 0))
                (pr/resolved ::ok))]

        (pr/let [r (sut/retry-n f ::retry-test 1 0)]
          (is (= ::ok r)))))

    (testing "works with 0 retries"
      (let [f (fn [n]
                (is (= n 0))
                (pr/resolved ::ok))]

        (pr/let [r (sut/retry-n f ::retry-test 0 0)]
          (is (= ::ok r)))))

    (testing "retries as specified"
      (let [ns (atom #{0 1 2})
            cnt (atom 0)
            f (fn [n]
                (swap! ns disj n)
                (if (> (swap! cnt inc) 2)
                  (pr/resolved ::ok)
                  (pr/rejected (ex-info "boo" {:boo "boo"}))))]

        (pr/let [r (sut/retry-n f ::retry-test 2 0)]
          (is (empty? @ns))
          (is (= ::ok r)))))

    (testing "fails when out of retries"
      (let [x (ex-info "boo" {:boo "boo"})
            ns (atom #{0 1})
            cnt (atom 0)
            f (fn [n]
                (swap! ns disj n)
                (if (> (swap! cnt inc) 2)
                  (pr/resolved ::ok)
                  (pr/rejected x)))]

        (pr/let [[r-k r-v] (merge-pr (sut/retry-n f ::retry-test 1 0))]
          (is (empty? @ns))
          (is (= ::error r-k))
          (is (= x r-v)))))))

(deftest retry-test
  (with-log-level :error

    (testing "returns an immediately successful result"
      (let [f (fn [] (pr/resolved ::ok))]
        (pr/let [r (sut/retry f ::retry-test 1 0)]
          (is (= ::ok r)))))

    (testing "works with 0 retries"
      (let [f (fn [] (pr/resolved ::ok))]
        (pr/let [r (sut/retry f ::retry-test 0 0)]
          (is (= ::ok r)))))

    (testing "retries as specified"
      (let [cnt (atom 0)
            f (fn [] (if (> (swap! cnt inc) 2)
                      (pr/resolved ::ok)
                      (pr/rejected (ex-info "boo" {:boo "boo"}))))]
        (pr/let [r (sut/retry f ::retry-test 2 0)]
          (is (= ::ok r)))))

    (testing "fails when out of retries"
      (let [x (ex-info "boo" {:boo "boo"})
            cnt (atom 0)
            f (fn [] (if (> (swap! cnt inc) 2)
                      (pr/resolved ::ok)
                      (pr/rejected x)))
            ]
        (pr/let [[r-k r-v] (merge-pr (sut/retry f ::retry-test 1 0))]
          (is (= ::error r-k))
          (is (= x r-v)))))))

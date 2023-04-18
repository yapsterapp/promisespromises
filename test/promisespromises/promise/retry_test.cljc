(ns promisespromises.promise.retry-test
  (:require
   [promisespromises.test :refer [deftest testing is with-log-level]]
   [promesa.core :as pr]
   [promisespromises.promise :as prpr]
   [promisespromises.promise.retry :as sut]))

(deftest retry-n-test
  (with-log-level :info
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
          (is (= ::ok r))))
      (let [x (ex-info "boo" {:boo "boo"})
            f (fn [n]
                (is (= n 0))
                (pr/rejected x))]

        (pr/let [[k r] (prpr/merge-always
                        (sut/retry-n f ::retry-test 0 0))]
          (is (= ::prpr/error k))
          (is (= {:boo "boo"} (ex-data r))))))

    (testing "retries as specified"
      (let [ns (atom #{0 1 2})
            cnt (atom 0)
            f (fn [n]
                (swap! ns disj n)
                (if (> (swap! cnt inc) 2)
                  (pr/resolved ::ok)
                  (pr/rejected (ex-info "boo" {:boo "boo"}))))]

        (pr/let [r (sut/retry-n f ::retry-test 3 0)]
          (is (empty? @ns))
          (is (= ::ok r)))))

    (testing "fails when out of retries"
      (let [x (ex-info "boo" {:boo "boo"})
            ns (atom #{4 5 6})
            cnt (atom 0)
            f (fn [n]
                (swap! ns disj (+ n 4))
                (if (> (swap! cnt inc) 2)
                  (pr/resolved ::ok)
                  (pr/rejected x)))]

        (pr/let [[r-k r-v] (prpr/merge-always
                            (sut/retry-n f ::retry-test 1 0))]
          (is (= #{6} @ns))
          (is (= ::prpr/error r-k))
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
          (is (= ::ok r))))
      (let [x (ex-info "boo" {:boo "boo"})
            f (fn [] (pr/rejected x))]
        (pr/let [[k r] (prpr/merge-always
                        (sut/retry f ::retry-test 0 0))]
          (is (= ::prpr/error k))
          (is (= {:boo "boo"} (ex-data r))))))

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
                       (pr/rejected x)))]
        (pr/let [[r-k r-v] (prpr/merge-always
                            (sut/retry f ::retry-test 1 0))]
          (is (= ::prpr/error r-k))
          (is (= x r-v)))))))

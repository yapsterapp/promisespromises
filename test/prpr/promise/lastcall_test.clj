(ns prpr.promise.lastcall-test
  (:require
   [prpr.test :refer [deftest testing is]]
   [promesa.core :as pr]
   [prpr.error :as err]
   [prpr.promise.lastcall :as sut]))

(defn merge-pr
  [p]
  (pr/handle
   p
   (fn [v e]
     (if (some? e)
       [::error e]
       [::ok v]))))

(deftest lastcall-fn-test
  (testing "single invocation returns value"
    (let [f (sut/lastcall-fn ::foo-fn [x] (pr/resolved x))]
      (is (= :foo @(f :foo)))))

  (testing "multi-call returns last, rejects earlier"
    (let [r1 (pr/delay 1000 :delayed-foo)
          r2 (pr/resolved :bar)
          f (sut/lastcall-fn ::foo-fn [r] r)
          fr1 (f r1)
          fr2 (f r2)]

      (pr/let [[fr1-tag
                {fr1-fn :fn
                 fr1-params :params
                 :as _fr1-val}] (merge-pr fr1)

               fr2val (merge-pr fr2)]

        (is (= :cancelled fr1-tag))
        (is (= ::foo-fn fr1-fn))
        (is (= [r1] fr1-params))
        (is (= :bar fr2val)))))

  (testing "winning call rejection is returned"
    (let [r1 (pr/delay 1000 :foo)
          r2 (pr/rejected (err/ex-info :ook {:reason :happenstance}))
          f (sut/lastcall-fn ::foo-fn [r] r)
          fr1 (f r1)
          fr2 (f r2)]

      (pr/let [[fr1-tag
                {fr1-fn :fn
                 fr1-params :params
                 :as _fr1-val}] (merge-pr fr1)

               fr2val (merge-pr fr2)]

        (is (= :cancelled fr1-tag))
        (is (= ::foo-fn fr1-fn))
        (is (= [r1] fr1-params))
        (is (= [:ook {:reason :happenstance}]
               fr2val)))))

  (testing "explicit cancellation"
    (let [r1 (pr/delay 1000 :delayed-foo)
          f (sut/lastcall-fn ::foo-fn [r] r)
          fr1 (f r1)
          cr (f)]

      (pr/let [[fr1-tag
                {fr1-fn :fn
                 fr1-params :params
                 :as _fr1-val}] (merge-pr fr1)

               crval (merge-pr cr)]

        (is (= :cancelled fr1-tag))
        (is (= ::foo-fn fr1-fn))
        (is (= [r1] fr1-params))
        (is (= true crval))))))

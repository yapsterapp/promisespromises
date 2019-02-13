(ns prpr.promise.lastcall-test
  (:require
   [prpr.promise.lastcall :as sut]
   [prpr.promise :as p]
   [clojure.test :as t :refer [deftest testing is are use-fixtures]]))

(deftest lastcall-fn-test
  (testing "single invocation returns value"
    (let [f (sut/lastcall-fn ::foo-fn [x] (p/success-pr x))]
      (is (= :foo @(f :foo)))))

  (testing "multi-call returns last, rejects earlier"
    (let [r1 (p/delay-pr 1000 :delayed-foo)
          r2 (p/success-pr :bar)
          f (sut/lastcall-fn ::foo-fn [r] r)
          fr1 (f r1)
          fr2 (f r2)

          [fr1-tag
           {fr1-fn :fn
            fr1-params :params
            :as fr1-val}] @(p/catch-error fr1)]
      (is (= :cancelled fr1-tag))
      (is (= ::foo-fn fr1-fn))
      (is (= [r1] fr1-params))
      (is (= :bar @fr2))))

  (testing "winning call rejection is returned"
    (let [r1 (p/delay-pr 1000 :foo)
          r2 (p/error-pr [:ook {:reason :happenstance}])
          f (sut/lastcall-fn ::foo-fn [r] r)
          fr1 (f r1)
          fr2 (f r2)

          [fr1-tag
           {fr1-fn :fn
            fr1-params :params
            :as fr1-val}] @(p/catch-error fr1)]
      (is (= :cancelled fr1-tag))
      (is (= ::foo-fn fr1-fn))
      (is (= [r1] fr1-params))
      (is (= [:ook {:reason :happenstance}]
             @(p/catch-error fr2))))))

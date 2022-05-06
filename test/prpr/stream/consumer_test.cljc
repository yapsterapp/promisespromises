(ns prpr.stream.consumer-test
  (:require
   [promesa.core :as pr]
   #?(:clj [prpr.test
            :refer [deftest testing is]]
      :cljs [prpr.test
             :refer-macros [deftest testing is]])
   [prpr.stream.protocols :as pt]
   [prpr.stream.types :as types]
   [prpr.stream.impl :as impl]
   [prpr.stream.consumer :as sut]))

(defn put-all-and-close!
  [s vals]
  (pr/let [_ (impl/put-all! s vals)]
    (impl/close! s)))

(deftest chunk-zip-test
  (testing "zips plain value streams"
    (let [a (impl/stream)
          _ (put-all-and-close! a [0 1 2])
          b (impl/stream)
          _ (put-all-and-close! b [::foo ::bar])

          out (sut/chunk-zip a b)]

      (pr/let [r1 (impl/take! out ::closed)
               r2 (impl/take! out ::closed)
               r3 (impl/take! out ::closed)
               ]
        (is (= [0 ::foo] r1))
        (is (= [1 ::bar] r2))
        (is (= ::closed r3)))))
  (testing "zips chunked streams")
  (testing "zips mixed streams")
  (testing "propagates errors"))

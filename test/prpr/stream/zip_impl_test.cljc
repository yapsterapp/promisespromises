(ns prpr.stream.zip-impl-test
  (:require
   [promesa.core :as pr]
   [prpr.test :refer [deftest testing is]]
   [prpr.stream.protocols :as pt]
   [prpr.stream.types :as types]
   [prpr.stream.transport :as transport]
   [prpr.stream.zip-impl :as sut]))

(deftest chunk-zip-test
  (testing "zips plain value streams"
    (let [a (transport/stream)
          _ (transport/put-all-and-close! a [0 1 2])
          b (transport/stream)
          _ (transport/put-all-and-close! b [::foo ::bar])

          out (sut/chunk-zip a b)]

      (pr/let [r1 (transport/take! out ::closed)
               r2 (transport/take! out ::closed)
               r3 (transport/take! out ::closed)
               ]
        (is (= [0 ::foo] r1))
        (is (= [1 ::bar] r2))
        (is (= ::closed r3)))))

  (testing "zips chunked streams"

    (testing "streams of different lengths"
      (let [a (transport/stream)
            _ (transport/put-all-and-close! a [(types/stream-chunk [0 1 2])])
            b (transport/stream)
            _ (transport/put-all-and-close! b [(types/stream-chunk [::foo ::bar])])

            out (sut/chunk-zip a b)]

        (pr/let [r1 (transport/take! out ::closed)
                 r2 (transport/take! out ::closed)]
          (is (types/stream-chunk? r1))
          (is (= [[0 ::foo]
                  [1 ::bar]]
                 (pt/-chunk-values r1)))
          (is (= ::closed r2)))))

    (testing "chunks of different sizes"
      (let [a (transport/stream)
            _ (transport/put-all-and-close! a [(types/stream-chunk [0 1 2])
                                     (types/stream-chunk [3])])
            b (transport/stream)
            _ (transport/put-all-and-close! b [(types/stream-chunk [::foo ::bar])
                                     (types/stream-chunk [::baz ::blah])])

            out (sut/chunk-zip a b)]

        (pr/let [r1 (transport/take! out ::closed)
                 r2 (transport/take! out ::closed)
                 r3 (transport/take! out ::closed)
                 r4 (transport/take! out ::closed)]
          (is (types/stream-chunk? r1))
          (is (types/stream-chunk? r2))
          (is (types/stream-chunk? r3))
          (is (= [[0 ::foo]
                  [1 ::bar]] (pt/-chunk-values r1)))
          (is (= [[2 ::baz]] (pt/-chunk-values r2)))
          (is (= [[3 ::blah]] (pt/-chunk-values r3)))
          (is (= ::closed r4))))))

  (testing "zips mixed streams"
    (let [a (transport/stream)
          _ (transport/put-all-and-close! a [0 (types/stream-chunk [1 2])])
          b (transport/stream)
          _ (transport/put-all-and-close! b [(types/stream-chunk [::foo ::bar])])

          out (sut/chunk-zip a b)]

      (pr/let [r1 (transport/take! out ::closed)
               r2 (transport/take! out ::closed)
               r3 (transport/take! out ::closed)]
        (is (= [0 ::foo] r1))
        (is (types/stream-chunk? r2))
        (is (= [[1 ::bar]] (pt/-chunk-values r2)))
        (is (= ::closed r3)))))

  (testing "propagates errors"
    (let [a (transport/stream)
          _ (transport/put-all-and-close! a [(types/stream-chunk [0 1])
                                   (types/stream-error
                                    (ex-info "boo!"
                                             {:boo "boo!"}))])
          b (transport/stream)
          _ (transport/put-all-and-close! b [::foo (types/stream-chunk [::bar])])

          out (sut/chunk-zip a b)]

      (pr/let [r1 (transport/take! out ::closed)
               r2 (transport/take! out ::closed)
               [k3 r3] (pr/handle
                        (transport/take! out ::closed)
                        (fn [succ err]
                          (if (some? err)
                            [::error err]
                            [::ok succ])))]
        (is (= [0 ::foo] r1))
        (is (types/stream-chunk? r2))
        (is (= [[1 ::bar]] (pt/-chunk-values r2)))
        (is (= ::error k3))
        (is (= {:boo "boo!"}
               (-> r3
                   (transport/unwrap-platform-error)
                   (ex-data)))))))
  )

(ns prpr.nustream-test
  (:require
   #?(:clj [prpr.test :refer [deftest testing is]]
      :cljs [prpr.test :refer-macros [deftest testing is]])
   [clojure.test.check :as tc]
   [clojure.test.check.generators :as gen]
   [clojure.test.check.properties :as prop]
   [clojure.test.check.clojure-test :refer [defspec]]
   [promesa.core :as pr]
   [prpr.stream.protocols :as pt]
   [prpr.stream.types :as types]
   [prpr.stream.impl :as impl]
   [prpr.nustream :as sut]))

(defn put-all-and-close!
  [s vs]
  (pr/chain
   (impl/put-all! s vs)
   (fn [_] (sut/close! s))))

(defn safe-take!
  [s & args]
  (pr/catch
      (pr/chain
       (apply sut/take! s args)
       (fn [v]
         [::ok v]))
      (fn [e]
        [::error e])))

(deftest realize-each-test
  (testing "does nothing to non-promise values"
    (let [s (sut/stream)
          t (sut/realize-each s)
          _ (put-all-and-close! s [0 1 2 3])]

      (pr/let [vs (->> (range 0 5)
                       (map (fn [_](sut/take! t ::closed)))
                       (pr/all))]
        (is (= [0 1 2 3 ::closed] vs)))))

  (testing "realizes promise values"
    (let [s (sut/stream)
          t (sut/realize-each s)
          _ (put-all-and-close! s (map pr/resolved [0 1 2 3]))]

      (pr/let [vs (->> (range 0 5)
                       (map (fn [_](sut/take! t ::closed)))
                       (pr/all))]
        (is (= [0 1 2 3 ::closed] vs)))))

  (testing "correctly propagates nil values"
    (let [s (sut/stream)
          t (sut/realize-each s)
          _ (put-all-and-close!
              s
              [(pr/resolved 0)
               (pr/resolved nil)
               2
               nil])]

      (pr/let [vs (->> (range 0 5)
                       (map (fn [_](sut/take! t ::closed)))
                       (pr/all))]
        (is (= [0 nil 2 nil ::closed] vs))))))

(deftest safe-chunk-xform-test
  (testing "simple map transform with no error"
    (let [out (sut/stream)]
      (is (= [1 2 3]
             (into [] (sut/safe-chunk-xform (map inc) out) [0 1 2])))
      (is (not (pt/-closed? out)))

      (is (= []
             (into [] (sut/safe-chunk-xform (map inc) out) [])))

      (is (not (pt/-closed? out)))))

  (testing "stateful transducer with no errors"
    (let [out (sut/stream)]
      (is (= [[0 2 4] [3 7] [8 10]]
             (into []
                   (sut/safe-chunk-xform
                    (partition-by odd?)
                    out)
                   [0 2 4 3 7 8 10])))
      (is (not (pt/-closed? out)))

      (is (= []
             (into []
                   (sut/safe-chunk-xform
                    (partition-by odd?)
                    out) [])))

      (is (not (pt/-closed? out)))))

  (testing "simple map transform with chunks and no error"
    (let [out (sut/stream)]
      (is (= [1 2 3]
             (into []
                   (sut/safe-chunk-xform (map inc) out)
                   [(types/stream-chunk [0 1 2])])))
      (is (not (pt/-closed? out)))

      (is (= [1 2 3]
             (into []
                   (sut/safe-chunk-xform (map inc) out)
                   [(types/stream-chunk [0])
                    (types/stream-chunk [1 2])])))
      (is (not (pt/-closed? out)))

      (is (= [1 2 3]
             (into []
                   (sut/safe-chunk-xform (map inc) out)
                   [0 (types/stream-chunk [1]) 2])))
      (is (not (pt/-closed? out)))))

  (testing "stateful transducer with chunks and no error"
    (let [out (sut/stream)]
      (is (= [[0 2] [3 5] [8]]
             (into []
                   (sut/safe-chunk-xform
                    (partition-by odd?)
                    out)
                   [(types/stream-chunk [0 2 3 5 8])])))
      (is (not (pt/-closed? out)))

      (is (= [[0 2] [3 5] [8]]
             (into []
                   (sut/safe-chunk-xform
                    (partition-by odd?)
                    out)
                   [(types/stream-chunk [0 2 3])
                    (types/stream-chunk [5 8])])))
      (is (not (pt/-closed? out)))

      (is (= [[0 2] [3 5] [8]]
             (into []
                   (sut/safe-chunk-xform
                    (partition-by odd?)
                    out)
                   [0
                    (types/stream-chunk [2])
                    3
                    (types/stream-chunk [5 8])])))
      (is (not (pt/-closed? out)))))

  (testing "simple map transformer with an error"

    ;; we expect an error in the into xform to throw immediately, and
    ;; also to error the out stream
    (let [out (sut/stream)
          [rk rv] (try
                    [:ok (into []
                               (sut/safe-chunk-xform
                                (map (fn [v] (if (odd? v)
                                              (throw (ex-info "boo" {:v v}))
                                              (inc v))))
                                out)
                               [0 2 3])]
                    (catch #?(:clj Throwable :cljs :default) e
                      [:error e]))

          [vk vv] @(pr/catch
                       (pr/let [r (sut/take! out ::closed)]
                         [:ok r])
                       (fn [e]
                         [:error e]))]

      (is (= :error rk))
      (is (= {:v 3} (ex-data rv)))

      (is (= :error vk))
      (is (= {:v 3} (ex-data vv)))
      (is (pt/-closed? out))))

  (testing "simple map transformer with chunk and error"

    ;; we expect an error in the into xform to throw immediately, and
    ;; also to error the out stream
    (let [out (sut/stream)
          [rk rv] (try
                    [:ok (into []
                               (sut/safe-chunk-xform
                                (map (fn [v] (if (odd? v)
                                              (throw (ex-info "boo" {:v v}))
                                              (inc v))))
                                out)
                               [0 2 (types/stream-chunk [3])])]
                    (catch #?(:clj Throwable :cljs :default) e
                      [:error e]))

          [vk vv] @(pr/catch
                       (pr/let [r (sut/take! out ::closed)]
                         [:ok r])
                       (fn [e]
                         [:error e]))]

      (is (= :error rk))
      (is (= {:v 3} (ex-data rv)))

      (is (= :error vk))
      (is (= {:v 3} (ex-data vv)))
      (is (pt/-closed? out)))))

(deftest transform-test)

(deftest map-test
  (testing "maps a stream"
    (let [s (sut/stream)
          t (sut/map inc s)
          _ (put-all-and-close! s [0 1 2 3])]

      (pr/let [vs (->> (range 0 5)
                       (map (fn [_](sut/take! t ::closed)))
                       (pr/all))]
        (is (= [1 2 3 4 ::closed] vs)))))

  (testing "maps multiple streams"
    (testing "maps multiple streams of the same size"
      (let [a (sut/stream)
            b (sut/stream)
            t (sut/map #(+ %1 %2) a b)
            _ (put-all-and-close! a [0 1 2 3])
            _ (put-all-and-close! b [0 1 2 3])]

        (pr/let [vs (->> (range 0 5)
                         (map (fn [_](sut/take! t ::closed)))
                         (pr/all))]
          (is (= [0 2 4 6 ::closed] vs))))))

  (testing "terminates the output when any of the inputs terminates"
    (let [a (sut/stream)
          b (sut/stream)
          t (sut/map #(+ %1 %2) a b)
          _ (put-all-and-close! a [0 1 2 3])
          _ (sut/put-all! b [0 1 2 3 4 5])]

      (pr/let [vs (->> (range 0 5)
                       (map (fn [_](sut/take! t ::closed)))
                       (pr/all))]
        (is (= [0 2 4 6 ::closed] vs)))))

  (testing "when receiving an error propagates it downstream"
    (testing "error propagation when mapping a single stream"
      (let [a (sut/stream)
            t (sut/map #(inc %1) a )
            _ (put-all-and-close!
               a
               [0 1 2 (types/stream-error (ex-info "boo" {:boo 100}))])]

        (pr/let [[a b c [ek ev]] (->> (range 0 4)
                                      (map (fn [_]
                                             (pr/catch
                                                 (sut/take! t ::closed)
                                                 (fn [e]
                                                   [::error e]))))
                                      (pr/all))]
          (is (= [1 2 3] [a b c]))
          (is (= ::error ek))
          (is {:boo 100} (-> ev ex-data)))))

    (testing "error propagation when mapping multiple streams"
      (let [a (sut/stream)
            b (sut/stream)
            t (sut/map #(+ %1 %2) a b)
            _ (put-all-and-close! a [0 1 2])
            _ (impl/put-all! b [0 1 2
                               (types/stream-error (ex-info "boo" {:boo 100}))
                               4])]

        (pr/let [[a b c [ek ev]] (->> (range 0 4)
                                      (map (fn [_]
                                             (pr/catch
                                                 (sut/take! t ::closed)
                                                 (fn [e]
                                                   [::error e]))))
                                      (pr/all))]
          (is (= [0 2 4] [a b c]))
          (is (= ek ::error))
          (is (= {:boo 100} (-> ev ex-data)))))))

  (testing "when receiving a nil wrapper sends nil to the mapping fn"
    (let [a (sut/stream)
          t (sut/map #(some-> %1 inc) a )
          _ (put-all-and-close! a [0 (types/stream-nil)])]

      (pr/let [[a b] (->> (range 0 2)
                          (map (fn [_] (sut/take! t ::closed)))
                          (pr/all))]
        (is (= [1 nil] [a b])))))
  #?(:cljs
     (testing "when mapping-fn returns a nil value, wraps it for the output"
       ;; nil-wrapping only happens on cljs
       (let [a (sut/stream)
             t (sut/map #(some-> %1 inc) a )
             _ (sut/put-all-and-close! a [0 (types/stream-nil)])]

         (pr/let [[a b] (->> (range 0 2)
                             (map (fn [_] (pt/-take! t ::closed)))
                             (pr/all))]
           (is (= [1 (types/stream-nil)] [a b]))))))

  (testing "catches mapping fn errors, errors the output and cleans up"
    (let [a (sut/stream)
          t (sut/map #(if (odd? %)
                        (throw (ex-info "boo" {:val %}))
                        (inc %)) a )
          _ (put-all-and-close! a [0 1])]

      (pr/let [[r1 r2 r3] (->> (range 0 3)
                               (map
                                (fn [_]
                                  (pr/catch
                                      (sut/take! t ::closed)
                                      (fn [e]
                                        [::error e]))))
                               (pr/all))]
        (is (= 1 r1))
        (let [[tag2 err2] r2]
          (is (= ::error tag2))
          (is (= {:val 1} (ex-data err2))))
        (is (= ::closed r3))))))

(deftest mapcon-test
  )

(deftest zip-test
  (testing "zips some streams"
    (testing "equal length streams"
      (let [[a b] (repeatedly 2 sut/stream)
            _ (put-all-and-close! a [0 1 2 3])
            _ (put-all-and-close! b [:a :b :c :d])

            s (sut/zip a b)]

        (pr/let [[r0 r1 r2 r3 r4] (->> (range 0 5)
                                       (map (fn [_] (sut/take! s ::closed)))
                                       (pr/all))]

          (is (= [0 :a] r0))
          (is (= [1 :b] r1))
          (is (= [2 :c] r2))
          (is (= [3 :d] r3))
          (is (= ::closed r4)))))

    (testing "different length streams"
      (let [[a b] (repeatedly 2 sut/stream)
            _ (put-all-and-close! a [0 1 2 3])
            _ (put-all-and-close! b [:a :b])

            s (sut/zip a b)]

        (pr/let [[r0 r1 r2] (->> (range 0 3)
                                 (map (fn [_] (sut/take! s ::closed)))
                                 (pr/all))]

          (is (= [0 :a] r0))
          (is (= [1 :b] r1))
          (is (= ::closed r2))))

      (testing "including a zero length stream"
        (let [[a b] (repeatedly 2 sut/stream)
              _ (put-all-and-close! a [])
              _ (put-all-and-close! b [:a :b])

              s (sut/zip a b)]

          (pr/let [[r0] (->> (range 0 1)
                             (map (fn [_] (sut/take! s ::closed)))
                             (pr/all))]

            (is (= ::closed r0))))))))

(deftest mapcat-test
  (testing "mapcats a stream")
  (testing "mapcats multiple streams"
    (testing "maps multiple streams of the same size")
    (testing "terminates the output when any of the inputs terminates"))
  (testing "when receiving an error propagates it to the downstream")
  (testing "when receiving a nil wrapper sends nil to the mapping fn")
  (testing "when mapping-fn returns a nil value, sends nothing to the output")
  (testing "catches mapping fn errors, errors the output and cleans up"))

(deftest filter-test
  (testing "filters a streams")
  (testing "catches filter fn errors, errors the output and cleans up")
  (testing "when receiving a nil wrapper sends nil to the filter fn"))

(deftest reductions-test

  (testing "returns reductions on the output stream"
    (testing "reductions of a stream of plain values"
      (testing "reductions of an empty stream"
        (let [s (sut/stream)
              _ (put-all-and-close! s [])
              t (sut/reductions ::reductions-empty-stream + s)]
          (pr/let [v (sut/take! t ::closed)]
            (is (= ::closed v )))))
      (testing "reductions of a single-element stream"
        (let [s (sut/stream)
              _ (put-all-and-close! s [5])
              t (sut/reductions ::reductions-empty-stream + s)]
          (pr/let [v1 (sut/take! t ::closed)
                   v2 (sut/take! t ::closed)]
            (is (= 5 v1))
            (is (= ::closed v2)))))
      (testing "reductions of a multi-element stream"
        (let [s (sut/stream)
              _ (put-all-and-close! s [1 2 3])
              t (sut/reductions ::reductions-empty-stream + s)]
          (pr/let [v1 (sut/take! t ::closed)
                   v2 (sut/take! t ::closed)
                   v3 (sut/take! t ::closed)
                   v4 (sut/take! t ::closed)]
            (is (= 1 v1))
            (is (= 3 v2))
            (is (= 6 v3))
            (is (= ::closed v4))))))

    (testing "reductions of a stream of chunks"
      (testing "reductions of a single-element stream"
        (let [s (sut/stream)
              _ (put-all-and-close! s [(types/stream-chunk [5])])
              t (sut/reductions ::reductions-empty-stream + s)]
          (pr/let [v1 (sut/take! t ::closed)
                   v2 (sut/take! t ::closed)]
            (is (= (types/stream-chunk [5]) v1))
            (is (= ::closed v2)))))
      (testing "reductions of a multi-element stream"
        (let [s (sut/stream)
              _ (put-all-and-close! s [(types/stream-chunk [1 2 3])])
              t (sut/reductions ::reductions-empty-stream + s)]
          (pr/let [v1 (sut/take! t ::closed)
                   v2 (sut/take! t ::closed)]
            (is (= (types/stream-chunk [1 3 6]) v1))
            (is (= ::closed v2))))))

    (testing "reductions of a mix of chunks and plain values"
      (let [s (sut/stream)
            _ (put-all-and-close! s [1 (types/stream-chunk [2 3]) 4])
            t (sut/reductions ::reductions-empty-stream + s)]
        (pr/let [v1 (sut/take! t ::closed)
                 v2 (sut/take! t ::closed)
                 v3 (sut/take! t ::closed)
                 v4 (sut/take! t ::closed)]
          (is (= 1 v1))
          (is (= (types/stream-chunk [3 6]) v2))
          (is (= 10 v3))
          (is (= ::closed v4))))))

  (testing "returns reducing function errors"
    (testing "with plain values"
      (let [s (sut/stream)
            _ (put-all-and-close! s [0 2 3])
            t (sut/reductions
               ::reductions-empty-stream
               (fn [a v]
                 (if (odd? v)
                   (throw (ex-info "boo" {:v v}))
                   (+ a v)))
               s)]
        (pr/let [[k1 v1] (safe-take! t ::closed)
                 [k2 v2] (safe-take! t ::closed)
                 [k3 v3] (safe-take! t ::closed)
                 [k4 v4] (safe-take! t ::closed)]
          (is (= ::ok k1)) (is (= 0 v1))
          (is (= ::ok k2)) (is (= 2 v2))
          (is (= ::error k3))
          (is (= {:v 3
                  ::sut/reduce-id ::reductions-empty-stream} (ex-data v3)))
          (is (= ::ok k4)) (is (= ::closed v4)))))
    (testing "with chunks"
      (let [s (sut/stream)
            _ (put-all-and-close! s [(types/stream-chunk [0 2 3])])
            t (sut/reductions
               ::reductions-empty-stream
               (fn [a v]
                 (if (odd? v)
                   (throw (ex-info "boo" {:v v}))
                   (+ a v)))
               s)]
        (pr/let [[k1 v1] (safe-take! t ::closed)
                 [k2 v2] (safe-take! t ::closed)]
          (is (= ::error k1))
          (is (= {:v 3
                  ::sut/reduce-id ::reductions-empty-stream} (ex-data v1)))
          (is (= ::ok k2)) (is (= ::closed v2))))))

  (testing "when receiving a nil wrapper sends nil to the reducing fn")
  (testing "deals with reduced"))

(deftest reduce-test
  (testing "reduces a stream"
    (testing "reduces an empty stream"
      (testing "with no initial value"
        (let [s (sut/stream)
              _ (put-all-and-close! s [])]
          (pr/let [r (sut/reduce ::reduces-a-stream + s)]
            (is (= 0 r)))))
      (testing "with an initial value"
        (let [s (sut/stream)
              _ (put-all-and-close! s [])]
          (pr/let [r (sut/reduce ::reduces-a-stream + 5 s)]
            (is (= 5 r))))))
    (testing "reduces a non-empty stream with plain values"
      (testing "with no initial value"
        (let [s (sut/stream)
              _ (put-all-and-close! s [1])]
          (pr/let [r (sut/reduce ::reduces-a-stream + s)]
            (is (= 1 r))))
        (let [s (sut/stream)
              _ (put-all-and-close! s [1 2])]
          (pr/let [r (sut/reduce ::reduces-a-stream + s)]
            (is (= 3 r))))
        (let [s (sut/stream)
              _ (put-all-and-close! s [1 2 3 4 5 6])]
          (pr/let [r (sut/reduce ::reduces-a-stream + s)]
            (is (= 21 r)))))

      (testing "with an initial value"
        (let [s (sut/stream)
              _ (put-all-and-close! s [1])]
          (pr/let [r (sut/reduce ::reduces-a-stream + 5 s)]
            (is (= 6 r))))
        (let [s (sut/stream)
              _ (put-all-and-close! s [1 2])]
          (pr/let [r (sut/reduce ::reduces-a-stream + 5 s)]
            (is (= 8 r))))
        (let [s (sut/stream)
              _ (put-all-and-close! s [1 2 3 4 5 6])]
          (pr/let [r (sut/reduce ::reduces-a-stream + 5 s)]
            (is (= 26 r))))))
    (testing "reduces a stream with chunks"
      (testing "with no initial value"
        (let [s (sut/stream)
              _ (put-all-and-close! s [(types/stream-chunk [1 2 3])
                                       (types/stream-chunk [4 5 6])])]
          (pr/let [r (sut/reduce ::reduces-a-stream + s)]
            (is (= 21 r)))))
      (testing "with an initial value"
        (let [s (sut/stream)
              _ (put-all-and-close! s [(types/stream-chunk [1 2 3])
                                       (types/stream-chunk [4 5 6])])]
          (pr/let [r (sut/reduce ::reduces-a-stream + 5 s)]
            (is (= 26 r))))))
    (testing "reduces a stream with mixed chunks and plain values"
      (testing "with no initial value"
        (let [s (sut/stream)
              _ (put-all-and-close! s [1
                                       (types/stream-chunk [2 3])
                                       4 5
                                       (types/stream-chunk [6])])]
          (pr/let [r (sut/reduce ::reduces-a-stream + s)]
            (is (= 21 r)))))
      (testing "with an initial value"
        (let [s (sut/stream)
              _ (put-all-and-close! s [1
                                       (types/stream-chunk [2 3])
                                       4 5
                                       (types/stream-chunk [6])])]
          (pr/let [r (sut/reduce ::reduces-a-stream + 5 s)]
            (is (= 26 r)))))))
  (testing "returns reducing function errors"
    (testing "returns errors on stream with plain values"
      (testing "with no initial value"
        (let [s (sut/stream)
              _ (put-all-and-close! s [0 1])]
          (pr/let [[k v] (->
                          (sut/reduce
                           ::reduce-error
                           (fn [a v] (if (odd? v)
                                      (throw (ex-info "boo" {:v v}))
                                      (+ a v)))
                           s)
                          (pr/chain (fn [v] [::ok v]))
                          (pr/catch (fn [e] [::error e])))]
            (is (= ::error k))
            (is (= {::sut/reduce-id ::reduce-error
                    :v 1} (ex-data v)))))
        (let [s (sut/stream)
              _ (put-all-and-close! s [0 2 3])]
          (pr/let [[k v] (->
                          (sut/reduce
                           ::reduce-error
                           (fn [a v] (if (odd? v)
                                      (throw (ex-info "boo" {:v v}))
                                      (+ a v)))
                           s)
                          (pr/chain (fn [v] [::ok v]))
                          (pr/catch (fn [e] [::error e])))]
            (is (= ::error k))
            (is (= {::sut/reduce-id ::reduce-error
                    :v 3} (ex-data v))))))
      (testing "with an initial value"
        (let [s (sut/stream)
              _ (put-all-and-close! s [1])]
          (pr/let [[k v] (->
                          (sut/reduce
                           ::reduce-error
                           (fn [a v] (if (odd? v)
                                      (throw (ex-info "boo" {:v v}))
                                      (+ a v)))
                           5
                           s)
                          (pr/chain (fn [v] [::ok v]))
                          (pr/catch (fn [e] [::error e])))]
            (is (= ::error k))
            (is (= {::sut/reduce-id ::reduce-error
                    :v 1} (ex-data v)))))
        (let [s (sut/stream)
              _ (put-all-and-close! s [0 2 3])]
          (pr/let [[k v] (->
                          (sut/reduce
                           ::reduce-error
                           (fn [a v] (if (odd? v)
                                      (throw (ex-info "boo" {:v v}))
                                      (+ a v)))
                           5
                           s)
                          (pr/chain (fn [v] [::ok v]))
                          (pr/catch (fn [e] [::error e])))]
            (is (= ::error k))
            (is (= {::sut/reduce-id ::reduce-error
                    :v 3} (ex-data v)))))))
    (testing "returns errors on stream with chunks"
      (testing "with no initial value"
        (let [s (sut/stream)
              _ (put-all-and-close! s [(types/stream-chunk [0 3])])]
          (pr/let [[k v] (->
                          (sut/reduce
                           ::reduce-error
                           (fn [a v] (if (odd? v)
                                      (throw (ex-info "boo" {:v v}))
                                      (+ a v)))
                           s)
                          (pr/chain (fn [v] [::ok v]))
                          (pr/catch (fn [e] [::error e])))]
            (is (= ::error k))
            (is (= {::sut/reduce-id ::reduce-error
                    :v 3} (ex-data v)))))
        (let [s (sut/stream)
              _ (put-all-and-close! s [(types/stream-chunk [0 2 3])])]
          (pr/let [[k v] (->
                          (sut/reduce
                           ::reduce-error
                           (fn [a v] (if (odd? v)
                                      (throw (ex-info "boo" {:v v}))
                                      (+ a v)))
                           s)
                          (pr/chain (fn [v] [::ok v]))
                          (pr/catch (fn [e] [::error e])))]
            (is (= ::error k))
            (is (= {::sut/reduce-id ::reduce-error
                    :v 3} (ex-data v))))))
      (testing "with an initial value"
        (let [s (sut/stream)
              _ (put-all-and-close! s [(types/stream-chunk [3])])]
          (pr/let [[k v] (->
                          (sut/reduce
                           ::reduce-error
                           (fn [a v] (if (odd? v)
                                      (throw (ex-info "boo" {:v v}))
                                      (+ a v)))
                           4
                           s)
                          (pr/chain (fn [v] [::ok v]))
                          (pr/catch (fn [e] [::error e])))]
            (is (= ::error k))
            (is (= {::sut/reduce-id ::reduce-error
                    :v 3} (ex-data v)))))
        (let [s (sut/stream)
              _ (put-all-and-close! s [(types/stream-chunk [0 2 3])])]
          (pr/let [[k v] (->
                          (sut/reduce
                           ::reduce-error
                           (fn [a v] (if (odd? v)
                                      (throw (ex-info "boo" {:v v}))
                                      (+ a v)))
                           4
                           s)
                          (pr/chain (fn [v] [::ok v]))
                          (pr/catch (fn [e] [::error e])))]
            (is (= ::error k))
            (is (= {::sut/reduce-id ::reduce-error
                    :v 3} (ex-data v)))))))
    (testing "returns errors on stream with mixed plain values and chunks"
      (testing "with no initial value"
        (let [s (sut/stream)
              _ (put-all-and-close! s [0 (types/stream-chunk [3])])]
          (pr/let [[k v] (->
                          (sut/reduce
                           ::reduce-error
                           (fn [a v] (if (odd? v)
                                      (throw (ex-info "boo" {:v v}))
                                      (+ a v)))
                           s)
                          (pr/chain (fn [v] [::ok v]))
                          (pr/catch (fn [e] [::error e])))]
            (is (= ::error k))
            (is (= {::sut/reduce-id ::reduce-error
                    :v 3} (ex-data v)))))
        (let [s (sut/stream)
              _ (put-all-and-close! s [0 2 (types/stream-chunk [3])])]
          (pr/let [[k v] (->
                          (sut/reduce
                           ::reduce-error
                           (fn [a v] (if (odd? v)
                                      (throw (ex-info "boo" {:v v}))
                                      (+ a v)))
                           s)
                          (pr/chain (fn [v] [::ok v]))
                          (pr/catch (fn [e] [::error e])))]
            (is (= ::error k))
            (is (= {::sut/reduce-id ::reduce-error
                    :v 3} (ex-data v))))))
      (testing "with an initial value"
        (let [s (sut/stream)
              _ (put-all-and-close! s [(types/stream-chunk [3])])]
          (pr/let [[k v] (->
                          (sut/reduce
                           ::reduce-error
                           (fn [a v] (if (odd? v)
                                      (throw (ex-info "boo" {:v v}))
                                      (+ a v)))
                           4
                           s)
                          (pr/chain (fn [v] [::ok v]))
                          (pr/catch (fn [e] [::error e])))]
            (is (= ::error k))
            (is (= {::sut/reduce-id ::reduce-error
                    :v 3} (ex-data v)))))
        (let [s (sut/stream)
              _ (put-all-and-close! s [0 2 (types/stream-chunk [3])])]
          (pr/let [[k v] (->
                          (sut/reduce
                           ::reduce-error
                           (fn [a v] (if (odd? v)
                                      (throw (ex-info "boo" {:v v}))
                                      (+ a v)))
                           4
                           s)
                          (pr/chain (fn [v] [::ok v]))
                          (pr/catch (fn [e] [::error e])))]
            (is (= ::error k))
            (is (= {::sut/reduce-id ::reduce-error
                    :v 3} (ex-data v))))))))

  (testing "when receiving a nil wrapper sends nil to the reducing fn"
    (let [s (sut/stream)
          _ (put-all-and-close! s [(types/stream-nil)])]
      (pr/let [r (sut/reduce
                  ::reduce-nil
                  (fn [a v]
                    (conj a v))
                  [] s)]
        (is (= [nil] r)))))

  (testing "deals with reduced"))

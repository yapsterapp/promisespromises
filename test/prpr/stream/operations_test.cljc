(ns prpr.stream.operations-test
  (:require
   [prpr.test :refer [deftest testing is with-log-level]]
   [promesa.core :as pr]
   [prpr.stream.protocols :as pt]
   [prpr.stream.types :as types]
   [prpr.stream.transport :as impl]
   [prpr.stream :as sut]))

(defn put-all-and-close!
  [s vs]
  (pr/chain
   (impl/put-all! s vs)
   (fn [_] (sut/close! s))))

(defn stream-of
  [vs]
  (let [s (sut/stream)]
    (put-all-and-close! s vs)
    s))

(defn safe-take!
  "take! from a stream returning
   Promise<[::ok <val>]> | Promise<[::error <err>]>"
  [s & args]
  (pr/catch
      (pr/chain
       (apply sut/take! s args)
       (fn [v]
         [::ok v]))
      (fn [e]
        [::error e])))

(defn safe-consume
  "keep safe-take! ing until ::closed"
  [s]
  #_{:clj-kondo/ignore [:loop-without-recur]}
  (pr/loop [r []]
    (pr/let [[_t v :as t-v] (safe-take! s ::closed)]
      (if (= ::closed v)
        (conj r t-v)
        (pr/recur (conj r t-v))))))

(comment
  ;; these things should be tested in every stream transform
  (testing "receiving a plain value behaviours")
  (testing "receiving a chunk behaviours")
  (testing "receiving a nil-value behaviours")
  (testing "receiving a stream-error behaviours")

  ;; and these should be testing in every transform with non-trivial function
  (testing "error in transform behaviours"))

(deftest realize-each-test
  (testing "receiving a plain value behaviours"
    (testing "does nothing to non-promise values"
      (let [s (stream-of [0 1 2 3])
            t (sut/realize-each s)]

        (pr/let [vs (safe-consume t)]
          (is (= [[::ok 0]
                  [::ok 1]
                  [::ok 2]
                  [::ok 3]
                  [::ok ::closed]]
                 vs)))))

    (testing "realizes promise values"
      (let [s (stream-of (map pr/resolved [0 1 2 3]))
            t (sut/realize-each s)]

        (pr/let [vs (safe-consume t)]
          (is (= [[::ok 0]
                  [::ok 1]
                  [::ok 2]
                  [::ok 3]
                  [::ok ::closed]]
                 vs))))))

  (testing "receiving a chunk behaviour"
    ;; TODO
    )

  (testing "receiving a nil-value behaviour"
    (testing "correctly propagates nil values"
      (let [s (stream-of
               [(pr/resolved 0)
                (pr/resolved nil)
                2
                nil])
            t (sut/realize-each s)]

        (pr/let [vs (safe-consume t)]
          (is (= [[::ok 0]
                  [::ok nil]
                  [::ok 2]
                  [::ok nil]
                  [::ok ::closed]]
                 vs))))))

  (testing "receiving a stream-error behaviour"
    (testing "propagates errors downstream"
      (let [s (stream-of
               [(pr/resolved 0)
                (types/stream-error (ex-info "boo" {:id 100}))])
            t (sut/realize-each s)]

        (pr/let [[r0 [r1t r1v :as _r1] r2] (safe-consume t)]

          (is (= [[::ok 0]
                  [::ok ::closed]]
                 [r0 r2]))
          (is (= ::error r1t))
          (is (= {:id 100} (ex-data r1v))))))))

(deftest safe-chunk-xform-test
  (testing "receiving a plain value behaviours"
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

        (is (not (pt/-closed? out))))))

  (testing "receiving a chunk behaviours"
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
        (is (not (pt/-closed? out))))))

  (testing "receiving a nil-value behaviours"
    ;; TODO
    )

  (testing "receiving a stream-error behaviours"
    (testing "simple map transform with a StreamError"
      ;; TODO
      ))

  (testing "error in transform behaviours"
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
        (is (pt/-closed? out))))))

(deftest transform-test

  (testing "receiving a plain value behaviours")
  (testing "receiving a chunk behaviours")
  (testing "receiving a nil-value behaviours")
  (testing "receiving a stream-error behaviours")
  (testing "error in transform behaviours")

  (testing "simple stateless transducer"
    (testing "transforms a stream of plain values"
      (let [s (stream-of [0 1 2])
            t (sut/transform (map inc) s)]
        (pr/let [vs (safe-consume t)]
          (is (= [[::ok 1]
                  [::ok 2]
                  [::ok 3]
                  [::ok ::closed]]
                 vs)))))
    (testing "transforms a stream of chunks"
      (let [s (stream-of [(types/stream-chunk [0 1 2])])
            t (sut/transform (map inc) s)]
        (pr/let [vs (safe-consume t)]
          (is (= [[::ok 1]
                  [::ok 2]
                  [::ok 3]
                  [::ok ::closed]]
                 vs)))))
    (testing "transforms a stream of mixed plain values and chunks"
      (let [s (stream-of [(types/stream-chunk [0])
                          (types/stream-chunk [1 2])
                          3])
            t (sut/transform (map inc) s)]
        (pr/let [vs (safe-consume t)]
          (is (= [[::ok 1]
                  [::ok 2]
                  [::ok 3]
                  [::ok 4]
                  [::ok ::closed]]
                 vs))))
      (let [s (stream-of [0
                          (types/stream-chunk [1 2])
                          3])
            t (sut/transform (map inc) s)]
        (pr/let [vs (safe-consume t)]
          (is (= [[::ok 1]
                  [::ok 2]
                  [::ok 3]
                  [::ok 4]
                  [::ok ::closed]]
                 vs))))))

  (testing "stateful transducer"
    (testing "transforms a stream of plain values"
      (let [s (stream-of [0 0 1 1 2 2])
            t (sut/transform (partition-by identity) s)]
        (pr/let [vs (safe-consume t)]
          (is (= [[::ok [0 0]]
                  [::ok [1 1]]
                  [::ok [2 2]]
                  [::ok ::closed]]
                 vs)))))
    (testing "transforms a stream of chunks"
      (let [s (stream-of [(types/stream-chunk [0 0 1 1 2 2])])
            t (sut/transform (partition-by identity) s)]
        (pr/let [vs (safe-consume t)]
          (is (= [[::ok [0 0]]
                  [::ok [1 1]]
                  [::ok [2 2]]
                  [::ok ::closed]]
                 vs)))))
    (testing "transforms a stream of mixed plain values and chunks"
      (let [s (stream-of [(types/stream-chunk [0])
                          0
                          (types/stream-chunk [1 1 2])
                          2])
            t (sut/transform (partition-by identity) s)]
        (pr/let [vs (safe-consume t)]
          (is (= [[::ok [0 0]]
                  [::ok [1 1]]
                  [::ok [2 2]]
                  [::ok ::closed]]
                 vs))))))

  (testing "errors in the transducer"
    (with-log-level :fatal
      (testing "errors the output stream"
        (testing "stream of plain values"
          (let [mapinc (fn [rf]
                         (fn
                           ([] (rf))
                           ([a] (rf a))
                           ([a v]
                            (if (odd? v)
                              (throw (ex-info "boo" {:v v}))
                              (rf a (inc v))))))

                s (stream-of [0 1 2])
                t (sut/transform
                   mapinc
                   s)]

            ;; the order of the output is not fully defined, because
            ;; the safe-chunk-xform currently skips a step in the stream
            ;; topology to error the output stream
            (pr/let [vs (safe-consume t)
                     oks (filter (fn [[k _v]] (= ::ok k)) vs)
                     [[_ err]] (filter (fn [[k _v]] (= ::error k)) vs)]
              (is (= [[::ok 1]
                      [::ok ::closed]]
                     oks))
              (is (= {:v 1}
                     (ex-data err))))))

        (testing "stream of plain values and chunks"
          (let [mapinc (fn [rf]
                         (fn
                           ([] (rf))
                           ([a] (rf a))
                           ([a v]
                            (if (odd? v)
                              (throw (ex-info "boo" {:v v}))
                              (rf a (inc v))))))

                s (stream-of [0 (types/stream-chunk [1]) 2])
                t (sut/transform
                   mapinc
                   s)]

            ;; the order of the output is not fully defined, because
            ;; the safe-chunk-xform currently skips a step in the stream
            ;; topology to error the output stream
            (pr/let [vs (safe-consume t)
                     oks (filter (fn [[k _v]] (= ::ok k)) vs)
                     [[_ err]] (filter (fn [[k _v]] (= ::error k)) vs)]
              (is (= [[::ok 1]
                      [::ok ::closed]]
                     oks))
              (is (= {:v 1}
                     (ex-data err)))))))))

  (testing "when receiving an error propagates it downstream"
    ;; TODO
    )
  )

(deftest map-test

    (testing "receiving a plain value behaviours")
  (testing "receiving a chunk behaviours")
  (testing "receiving a nil-value behaviours")
  (testing "receiving a stream-error behaviours")
  (testing "error in transform behaviours")


  (testing "maps a stream"
    (let [s (stream-of [0 1 2 3])
          t (sut/map inc s)]

      (pr/let [vs (->> (range 0 5)
                       (map (fn [_](sut/take! t ::closed)))
                       (pr/all))]
        (is (= [1 2 3 4 ::closed] vs)))))

  (testing "maps multiple streams"
    (testing "maps multiple streams of the same size"
      (let [a (stream-of [0 1 2 3])
            b (stream-of [0 1 2 3])
            t (sut/map #(+ %1 %2) a b)]

        (pr/let [vs (->> (range 0 5)
                         (map (fn [_](sut/take! t ::closed)))
                         (pr/all))]
          (is (= [0 2 4 6 ::closed] vs))))))

  (testing "terminates the output when any of the inputs terminates"
    (let [a (stream-of [0 1 2 3])
          b (sut/stream)
          _ (impl/put-all! b [0 1 2 3 4 5])
          t (sut/map #(+ %1 %2) a b)]

      (pr/let [vs (->> (range 0 5)
                       (map (fn [_](sut/take! t ::closed)))
                       (pr/all))]
        (is (= [0 2 4 6 ::closed] vs)))))

  (testing "when receiving an error propagates it downstream"
    (testing "error propagation when mapping a single stream"
      (let [a (stream-of
               [0 1 2 (types/stream-error (ex-info "boo" {:boo 100}))])
            t (sut/map #(inc %1) a )]

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
      (let [a (stream-of [0 1 2])
            b (sut/stream)
            _ (impl/put-all! b [0 1 2
                                (types/stream-error (ex-info "boo" {:boo 100}))
                                4])

            t (sut/map #(+ %1 %2) a b)]

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
    (let [a (stream-of [0 (types/stream-nil)])
          t (sut/map #(some-> %1 inc) a )]

      (pr/let [[a b] (->> (range 0 2)
                          (map (fn [_] (sut/take! t ::closed)))
                          (pr/all))]
        (is (= [1 nil] [a b])))))
  #?(:cljs
     (testing "when mapping-fn returns a nil value, wraps it for the output"
       ;; nil-wrapping only happens on cljs
       (let [a (stream-of [0 (types/stream-nil)])
             t (sut/map #(some-> %1 inc) a )]

         (pr/let [[a b] (->> (range 0 2)
                             (map (fn [_] (pt/-take! t ::closed)))
                             (pr/all))]
           (is (= [1 (types/stream-nil)] [a b]))))))

  (testing "catches mapping fn errors, errors the output and cleans up"
    (let [a (stream-of [0 1])
          t (sut/map #(if (odd? %)
                        (throw (ex-info "boo" {:val %}))
                        (inc %)) a )]

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
    (testing "receiving a plain value behaviours")
  (testing "receiving a chunk behaviours")
  (testing "receiving a nil-value behaviours")
  (testing "receiving a stream-error behaviours")
  (testing "error in transform behaviours")


  )

(deftest zip-test
    (testing "receiving a plain value behaviours")
  (testing "receiving a chunk behaviours")
  (testing "receiving a nil-value behaviours")
  (testing "receiving a stream-error behaviours")
  (testing "error in transform behaviours")


  (testing "zips some streams"
    (testing "equal length streams"
      (let [a (stream-of [0 1 2 3])
            b (stream-of [:a :b :c :d])

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
      (let [a (stream-of [0 1 2 3])
            b (stream-of [:a :b])

            s (sut/zip a b)]

        (pr/let [[r0 r1 r2] (->> (range 0 3)
                                 (map (fn [_] (sut/take! s ::closed)))
                                 (pr/all))]

          (is (= [0 :a] r0))
          (is (= [1 :b] r1))
          (is (= ::closed r2))))

      (testing "including a zero length stream"
        (let [a (stream-of [])
              b (stream-of [:a :b])

              s (sut/zip a b)]

          (pr/let [[r0] (->> (range 0 1)
                             (map (fn [_] (sut/take! s ::closed)))
                             (pr/all))]

            (is (= ::closed r0))))))))

(deftest mapcat-test
    (testing "receiving a plain value behaviours")
  (testing "receiving a chunk behaviours")
  (testing "receiving a nil-value behaviours")
  (testing "receiving a stream-error behaviours")
  (testing "error in transform behaviours")


  (testing "mapcats a stream"
    (let [s (stream-of [0 1 2 3])
          t (sut/mapcat
             (fn [v] (repeat v v))
             s)]
      (pr/let [vs (safe-consume t)]
        (is (= [[::ok (types/stream-chunk [1])]
                [::ok (types/stream-chunk [2 2])]
                [::ok (types/stream-chunk [3 3 3])]
                [::ok ::closed]]
               vs)))))
  (testing "mapcats multiple streams"
    (testing "maps multiple streams of the same size"
      (let [s (stream-of [0 1 2 3])
            t (stream-of [:a :b :c :d])
            u (sut/mapcat (fn [a b] (repeat a [a b])) s t)]
        (pr/let [vs (safe-consume u)]
          (is (= [[::ok (types/stream-chunk [[1 :b]])]
                  [::ok (types/stream-chunk [[2 :c][2 :c]])]
                  [::ok (types/stream-chunk [[3 :d][3 :d][3 :d]])]
                  [::ok ::closed]]
                 vs)))))
    (testing "terminates the output when any of the inputs terminates"
      (let [s (stream-of [0 1 2])
            t (stream-of [:a :b :c :d])
            u (sut/mapcat (fn [a b] (repeat a [a b])) s t)]
        (pr/let [vs (safe-consume u)]
          (is (= [[::ok (types/stream-chunk [[1 :b]])]
                  [::ok (types/stream-chunk [[2 :c][2 :c]])]
                  [::ok ::closed]]
                 vs))))))
  (testing "when receiving an error propagates it to the downstream"
    (let [s (stream-of [2 4 (types/stream-error
                             (ex-info "boo" {:id 100}))])
          t (sut/mapcat
             (fn [v] (if (odd? v)
                      (throw (ex-info "boo" {:v v}))
                      (repeat v v)))
             s)]
      (pr/let [[r0 r1 r2 r3] (safe-consume t)]
        (is (= [[::ok (types/stream-chunk [2 2])]
                [::ok (types/stream-chunk [4 4 4 4])]
                [::ok ::closed]]
               [r0 r1 r3]))
        (is (= ::error (first r2)))
        (is (= {:id 100} (ex-data (second r2)))))))
  (testing "when receiving a nil wrapper sends nil to the mapping fn"
    (let [s (stream-of [0 1 nil 3])
          t (sut/mapcat
             (fn [v] (if (nil? v)
                      [::nil]
                      (repeat v v)))
             s)]
      (pr/let [vs (safe-consume t)]
        (is (= [[::ok (types/stream-chunk [1])]
                [::ok (types/stream-chunk [::nil])]
                [::ok (types/stream-chunk [3 3 3])]
                [::ok ::closed]]
               vs)))))
  (testing "when mapping-fn returns a nil value, sends nothing to the output"
    (let [s (stream-of [0 1 nil 3])
          t (sut/mapcat
             (fn [v] (if (nil? v)
                      nil
                      (repeat v v)))
             s)]
      (pr/let [vs (safe-consume t)]
        (is (= [[::ok (types/stream-chunk [1])]
                [::ok (types/stream-chunk [3 3 3])]
                [::ok ::closed]]
               vs)))))
  (testing "catches mapping fn errors, errors the output and cleans up"
    (let [s (stream-of [2 4 5 6])
          t (sut/mapcat
             (fn [v] (if (odd? v)
                      (throw (ex-info "boo" {:v v}))
                      (repeat v v)))
             s)]
      (pr/let [[r0 r1 r2 r3] (safe-consume t)]
        (is (= [[::ok (types/stream-chunk [2 2])]
                [::ok (types/stream-chunk [4 4 4 4])]
                [::ok ::closed]]
               [r0 r1 r3]))
        (is (= ::error (first r2)))
        (is (= {:v 5} (ex-data (second r2))))))))

(deftest filter-test
  (testing "receiving a plain value behaviours")
  (testing "receiving a chunk behaviours")
  (testing "receiving a nil-value behaviours")
  (testing "receiving a stream-error behaviours")
  (testing "error in transform behaviours")


  (testing "filters a stream"
    (testing "of plain values"
      (let [s (stream-of [0 1 2 3 4 5])
            t (sut/filter odd? s)]
        (pr/let [vs (safe-consume t)]
          (is (= [[::ok 1] [::ok 3] [::ok 5] [::ok ::closed]] vs)))))
    (testing "of chunks"
      (let [s (stream-of [(types/stream-chunk [0 1 2])
                          (types/stream-chunk [3 4 5])])
            t (sut/filter odd? s)]
        (pr/let [vs (safe-consume t)]
          (is (= [[::ok (types/stream-chunk [1])]
                  [::ok (types/stream-chunk [3 5])]
                  [::ok ::closed]]
                 vs))))
      (let [s (stream-of [(types/stream-chunk [0 2])
                          (types/stream-chunk [3 4 5])])
            t (sut/filter odd? s)]
        (pr/let [vs (safe-consume t)]
          (is (= [[::ok (types/stream-chunk [3 5])]
                  [::ok ::closed]]
                 vs)))))
    (testing "of mixed plain values and chunks"
      (let [s (stream-of [0 (types/stream-chunk [1 2])
                          3 (types/stream-chunk [4 5])])
            t (sut/filter odd? s)]
        (pr/let [vs (safe-consume t)]
          (is (= [[::ok (types/stream-chunk [1])]
                  [::ok 3]
                  [::ok (types/stream-chunk [5])]
                  [::ok ::closed]]
                 vs))))))

  (testing "catches filter fn errors, errors the output and cleans up"
    (testing "with plain values"
      (let [s (stream-of [0 2 3 4 5])
            t (sut/filter (fn [v]
                            (if (odd? v)
                              (throw (ex-info "boo" {:v v}))
                              true))
                          s)]
        (pr/let [[a b c d :as _vs] (safe-consume t)]
          (is (= [::ok 0] a))
          (is (= [::ok 2] b))
          (is (= ::error (first c)))
          (is (= {:v 3} (ex-data (second c))))
          (is (= [::ok ::closed] d)))))
    (testing "with chunks"
      (let [s (stream-of [(types/stream-chunk [0 2])
                          (types/stream-chunk [3 4 5])])
            t (sut/filter (fn [v]
                            (if (odd? v)
                              (throw (ex-info "boo" {:v v}))
                              true))
                          s)]
        (pr/let [[a b c _d :as _vs] (safe-consume t)]
          (is (= [::ok (types/stream-chunk [0 2])] a))
          (is (= ::error (first b)))
          (is (= {:v 3} (ex-data (second b))))
          (is (= [::ok ::closed] c))))))

  (testing "when receiving a nil wrapper sends nil to the filter fn"
    (let [s (stream-of [0 1 (types/stream-nil) 2 3 (types/stream-nil) 4 5])
          t (sut/filter
             (fn [v] (or (nil? v) (odd? v) ))
             s)]
      (pr/let [vs (safe-consume t)]
        (is (= [[::ok 1]
                [::ok nil]
                [::ok 3]
                [::ok nil]
                [::ok 5]
                [::ok ::closed]]
               vs))))))

(deftest reductions-test
  (testing "receiving a plain value behaviours")
  (testing "receiving a chunk behaviours")
  (testing "receiving a nil-value behaviours")
  (testing "receiving a stream-error behaviours")
  (testing "error in transform behaviours")


  (testing "returns reductions on the output stream"
    (testing "reductions of a stream of plain values"
      (testing "reductions of an empty stream"
        (let [s (stream-of [])
              t (sut/reductions ::reductions-empty-stream + s)]
          (pr/let [v (sut/take! t ::closed)]
            (is (= ::closed v )))))
      (testing "reductions of a single-element stream"
        (let [s (stream-of [5])
              t (sut/reductions ::reductions-empty-stream + s)]
          (pr/let [v1 (sut/take! t ::closed)
                   v2 (sut/take! t ::closed)]
            (is (= 5 v1))
            (is (= ::closed v2)))))
      (testing "reductions of a multi-element stream"
        (let [s (stream-of [1 2 3])
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
        (let [s (stream-of [(types/stream-chunk [5])])
              t (sut/reductions ::reductions-empty-stream + s)]
          (pr/let [v1 (sut/take! t ::closed)
                   v2 (sut/take! t ::closed)]
            (is (= (types/stream-chunk [5]) v1))
            (is (= ::closed v2)))))
      (testing "reductions of a multi-element stream"
        (let [s (stream-of [(types/stream-chunk [1 2 3])])
              t (sut/reductions ::reductions-empty-stream + s)]
          (pr/let [v1 (sut/take! t ::closed)
                   v2 (sut/take! t ::closed)]
            (is (= (types/stream-chunk [1 3 6]) v1))
            (is (= ::closed v2))))))

    (testing "reductions of a mix of chunks and plain values"
      (let [s (stream-of [1 (types/stream-chunk [2 3]) 4])
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
      (let [s (stream-of [0 2 3])
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
      (let [s (stream-of [(types/stream-chunk [0 2 3])])
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

  (testing "when receiving a nil wrapper sends nil to the reducing fn"
    (let [s (stream-of [1 (types/stream-nil) 2])]
      (pr/let [t (sut/reductions
                  ::reduce-nil
                  (fn [a v]
                    (conj a v))
                  []
                  s)

               vs (safe-consume t)]
        (is (= [[::ok []]
                [::ok [1]]
                [::ok [1 nil]]
                [::ok [1 nil 2]]
                [::ok ::closed]]
               vs)))))

  (testing "deals with reduced"
    (testing "with no initial value"
      (testing "with reduced in second place"
        (let [s (stream-of [1 3 4 6])
              t (sut/reductions
                 ::reductions-empty-stream
                 (fn [a v] (if (odd? v) (reduced (+ a v)) (+ a v)))
                 s)]
          (pr/let [v1 (sut/take! t ::closed)
                   v2 (sut/take! t ::closed)
                   v3 (sut/take! t ::closed)]
            (is (= 1 v1))
            (is (= 4 v2))
            (is (= ::closed v3)))))
      (testing "with reduced further down the stream"
        (let [s (stream-of [2 4 3 6])
              t (sut/reductions
                 ::reductions-empty-stream
                 (fn [a v] (if (odd? v) (reduced (+ a v)) (+ a v)))
                 s)]
          (pr/let [v1 (sut/take! t ::closed)
                   v2 (sut/take! t ::closed)
                   v3 (sut/take! t ::closed)
                   v4 (sut/take! t ::closed)]
            (is (= 2 v1))
            (is (= 6 v2))
            (is (= 9 v3))
            (is (= ::closed v4))))))
    (testing "with an initial value"
      (testing "with reduced in first place"
        (let [s (stream-of [1 2 4 6])
              t (sut/reductions
                 ::reductions-empty-stream
                 (fn [a v] (if (odd? v) (reduced (+ a v)) (+ a v)))
                 1
                 s)]
          (pr/let [v1 (sut/take! t ::closed)
                   v2 (sut/take! t ::closed)
                   v3 (sut/take! t ::closed)]
            (is (= 1 v1))
            (is (= 2 v2))
            (is (= ::closed v3)))))
      (testing "with reduced further down the stream"
        (let [s (stream-of [2 4 3 6])
              t (sut/reductions
                 ::reductions-empty-stream
                 (fn [a v] (if (odd? v) (reduced (+ a v)) (+ a v)))
                 1
                 s)]
          (pr/let [v1 (sut/take! t ::closed)
                   v2 (sut/take! t ::closed)
                   v3 (sut/take! t ::closed)
                   v4 (sut/take! t ::closed)
                   v5 (sut/take! t ::closed)]
            (is (= 1 v1))
            (is (= 3 v2))
            (is (= 7 v3))
            (is (= 10 v4))
            (is (= ::closed v5))))))))

(deftest reduce-test
    (testing "receiving a plain value behaviours")
  (testing "receiving a chunk behaviours")
  (testing "receiving a nil-value behaviours")
  (testing "receiving a stream-error behaviours")
  (testing "error in transform behaviours")



  (testing "reduces a stream"
    (testing "reduces an empty stream"
      (testing "with no initial value"
        (let [s (stream-of [])]
          (pr/let [r (sut/reduce ::reduces-a-stream + s)]
            (is (= 0 r)))))
      (testing "with an initial value"
        (let [s (stream-of [])]
          (pr/let [r (sut/reduce ::reduces-a-stream + 5 s)]
            (is (= 5 r))))))
    (testing "reduces a non-empty stream with plain values"
      (testing "with no initial value"
        (let [s (stream-of [1])]
          (pr/let [r (sut/reduce ::reduces-a-stream + s)]
            (is (= 1 r))))
        (let [s (stream-of [1 2])]
          (pr/let [r (sut/reduce ::reduces-a-stream + s)]
            (is (= 3 r))))
        (let [s (stream-of [1 2 3 4 5 6])]
          (pr/let [r (sut/reduce ::reduces-a-stream + s)]
            (is (= 21 r)))))

      (testing "with an initial value"
        (let [s (stream-of [1])]
          (pr/let [r (sut/reduce ::reduces-a-stream + 5 s)]
            (is (= 6 r))))
        (let [s (stream-of [1 2])]
          (pr/let [r (sut/reduce ::reduces-a-stream + 5 s)]
            (is (= 8 r))))
        (let [s (stream-of [1 2 3 4 5 6])]
          (pr/let [r (sut/reduce ::reduces-a-stream + 5 s)]
            (is (= 26 r))))))
    (testing "reduces a stream with chunks"
      (testing "with no initial value"
        (let [s (stream-of [(types/stream-chunk [1 2 3])
                            (types/stream-chunk [4 5 6])])]
          (pr/let [r (sut/reduce ::reduces-a-stream + s)]
            (is (= 21 r)))))
      (testing "with an initial value"
        (let [s (stream-of [(types/stream-chunk [1 2 3])
                            (types/stream-chunk [4 5 6])])]
          (pr/let [r (sut/reduce ::reduces-a-stream + 5 s)]
            (is (= 26 r))))))
    (testing "reduces a stream with mixed chunks and plain values"
      (testing "with no initial value"
        (let [s (stream-of [1
                            (types/stream-chunk [2 3])
                            4 5
                            (types/stream-chunk [6])])]
          (pr/let [r (sut/reduce ::reduces-a-stream + s)]
            (is (= 21 r)))))
      (testing "with an initial value"
        (let [s (stream-of [1
                            (types/stream-chunk [2 3])
                            4 5
                            (types/stream-chunk [6])])]
          (pr/let [r (sut/reduce ::reduces-a-stream + 5 s)]
            (is (= 26 r)))))))
  (testing "returns reducing function errors"
    (testing "returns errors on stream with plain values"
      (testing "with no initial value"
        (let [s (stream-of [0 1])]
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
        (let [s (stream-of [0 2 3])]
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
        (let [s (stream-of [1])]
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
        (let [s (stream-of [0 2 3])]
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
        (let [s (stream-of [(types/stream-chunk [0 3])])]
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
        (let [s (stream-of [(types/stream-chunk [0 2 3])])]
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
        (let [s (stream-of [(types/stream-chunk [3])])]
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
        (let [s (stream-of [(types/stream-chunk [0 2 3])])]
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
        (let [s (stream-of [0 (types/stream-chunk [3])])]
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
        (let [s (stream-of [0 2 (types/stream-chunk [3])])]
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
        (let [s (stream-of [(types/stream-chunk [3])])]
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
        (let [s (stream-of [0 2 (types/stream-chunk [3])])]
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
    (let [s (stream-of [(types/stream-nil)])]
      (pr/let [r (sut/reduce
                  ::reduce-nil
                  (fn [a v]
                    (conj a v))
                  [] s)]
        (is (= [nil] r)))))

  (testing "deals with reduced"
    (testing "with no initial value"
      (testing "reduced at second position"
        (let [s (stream-of [2 3 4 6])]
          (pr/let [[k v] (->
                          (sut/reduce
                           ::deals-with-reduced
                           (fn [a v] (if (odd? v)
                                      (reduced (+ a v))
                                      (+ a v)))
                           s)
                          (pr/chain (fn [v] [::ok v]))
                          (pr/catch (fn [e] [::error e])))]
            (is (= ::ok k))
            (is (= 5 v)))))
      (testing "reduced further down stream"
        (let [s (stream-of [0 2 3 4 6])]
          (pr/let [[k v] (->
                          (sut/reduce
                           ::deals-with-reduced
                           (fn [a v] (if (odd? v)
                                      (reduced (+ a v))
                                      (+ a v)))
                           s)
                          (pr/chain (fn [v] [::ok v]))
                          (pr/catch (fn [e] [::error e])))]
            (is (= ::ok k))
            (is (= 5 v))))))
    (testing "with an initial value"
      (testing "reduced at head of stream"
        (let [s (stream-of [1 2 3 4 6])]
          (pr/let [[k v] (->
                          (sut/reduce
                           ::deals-with-reduced
                           (fn [a v] (if (odd? v)
                                      (reduced (+ a v))
                                      (+ a v)))
                           2
                           s)
                          (pr/chain (fn [v] [::ok v]))
                          (pr/catch (fn [e] [::error e])))]
            (is (= ::ok k))
            (is (= 3 v)))))
      (testing "reduced at second position"
        (let [s (stream-of [2 3 4 6])]
          (pr/let [[k v] (->
                          (sut/reduce
                           ::deals-with-reduced
                           (fn [a v] (if (odd? v)
                                      (reduced (+ a v))
                                      (+ a v)))
                           2
                           s)
                          (pr/chain (fn [v] [::ok v]))
                          (pr/catch (fn [e] [::error e])))]
            (is (= ::ok k))
            (is (= 7 v)))))
      (testing "reduced further down stream"
        (let [s (stream-of [0 2 3 4 6])]
          (pr/let [[k v] (->
                          (sut/reduce
                           ::deals-with-reduced
                           (fn [a v] (if (odd? v)
                                      (reduced (+ a v))
                                      (+ a v)))
                           2
                           s)
                          (pr/chain (fn [v] [::ok v]))
                          (pr/catch (fn [e] [::error e])))]
            (is (= ::ok k))
            (is (= 7 v))))))
    ))

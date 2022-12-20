(ns prpr.stream.cross-test
  (:require
   [prpr.test :refer [deftest testing is with-log-level]]
   [clojure.math.combinatorics :as combo]
   [linked.core :as linked]
   [taoensso.timbre :as log :refer [info warn error]]

   [promesa.core :as pr]

   [prpr.stream.operations :as stream.ops]
   [prpr.stream.transport :as stream.transport]
   [prpr.stream.types :as stream.types]
   [prpr.stream :as stream]

   [prpr.stream.cross :as sut]
   [prpr.stream.cross :as-alias stream.cross]
   [prpr.stream.cross.op :as-alias stream.cross.op]
   )
  (:import
   [linked.map LinkedMap]))

(defn stream-of
  "returns a stream of the individual values
   (*not* chunked)"
  [vs]
  (let [s (stream.transport/stream)]
    (stream.transport/put-all-and-close! s vs)
    s))

(defn consume
  [s]
  (pr/loop [rs []]
    (pr/chain
     (stream.transport/take! s ::drained)
     (fn [v]
       (if (= ::drained v)
         rs
         (pr/recur (conj rs v)))))))

(defn merge-pr
  "merge the branches of a promise"
  [p f]
  (-> p
      (pr/handle
       (fn [v err]
         (if (some? err)
           [::error err]
           [::ok v])))
      (pr/chain f)))

(deftest stream-finished?-test
  (testing "not finished when partition-buffer is empty"
    (is (not (sut/stream-finished? []))))
  (testing "not finished when partition-buffer has some content"
    (is (not (sut/stream-finished? [["blah" '({:id "blah"})]]))))
  (testing "finished when partition-buffer is drained"
    (is (sut/stream-finished? [[::stream.cross/drained]]))
    (is (sut/stream-finished? [["blah" '({:id "blah"})]
                               [::stream.cross/drained]])))
  (testing "finished when partition-buffer is errored"
    (is (sut/stream-finished? [[::stream.cross/errored]]))
    (is (sut/stream-finished? [["blah" '({:id "blah"})]
                               [::stream.cross/errored]]))))

(deftest buffer-chunk!-test
  (testing "initialises, appends and terminates a partition buffer"
    (let [cfg (sut/configure-cross-op
               {::stream.cross/op ::stream.cross.op/sorted-merge
                ::stream.cross/keys [[:a identity]]
                ::stream.cross/key-comparator-fn compare})
          init-pb []
          s (stream-of [(stream.types/stream-chunk [0 0 0 1 1 1])
                        (stream.types/stream-chunk [2 2 2])])]

      (pr/let [pb1 (sut/buffer-chunk! init-pb cfg :a s)
               pb2 (sut/buffer-chunk! pb1 cfg :a s)
               pb3 (sut/buffer-chunk! pb2 cfg :a s)]

        (is (= [[0 '(0 0 0)] [1 '(1 1 1)]] pb1))

        (is (= [[0 '(0 0 0)] [1 '(1 1 1)] [2 '(2 2 2)]] pb2))

        (is (= [[0 '(0 0 0)] [1 '(1 1 1)] [2 '(2 2 2)]
                [::stream.cross/drained]
                ] pb3)))))

  (testing "deals with stream error"
    (let [cfg (sut/configure-cross-op
               {::stream.cross/op ::stream.cross.op/sorted-merge
                ::stream.cross/keys [[:a identity]]
                ::stream.cross/key-comparator-fn compare})
          init-pb []
          s (->> (stream-of [(stream.types/stream-chunk [0 0 0 2 2 2])
                             (stream.types/stream-chunk [4 4 4 5 5 5])])
                 (stream/map (fn [v]
                               (if (odd? v)
                                 (throw (ex-info "boo" {:v v}))
                                 v))))]

      (pr/let [pb1 (sut/buffer-chunk! init-pb cfg :a s)
               [_ _ [k v]:as pb2] (sut/buffer-chunk! pb1 cfg :a s)]

        (is (= [[0 '(0 0 0)] [2 '(2 2 2)]] pb1))
        (is (= [[0 '(0 0 0)] [2 '(2 2 2)]] (take 2 pb2)))
        (is (= ::stream.cross/errored k))
        (is (= {:v 5} (ex-data v))))))

  (testing "deals with empty stream"
    (let [cfg (sut/configure-cross-op
               {::stream.cross/op ::stream.cross.op/sorted-merge
                ::stream.cross/keys [[:a identity]]
                ::stream.cross/key-comparator-fn compare})
          init-pb []
          s (stream-of [])]

      (pr/let [pb1 (sut/buffer-chunk! init-pb cfg :a s)]
        (is (= [[::stream.cross/drained]] pb1)))))

  (testing "throws an error if stream not sorted"
    (testing "throws if chunk is not sorted"
      (let [cfg (sut/configure-cross-op
                 {::stream.cross/op ::stream.cross.op/sorted-merge
                  ::stream.cross/keys [[:a identity]]
                  ::stream.cross/key-comparator-fn compare})
            init-pb []
            s (stream-of [(stream.types/stream-chunk [1 1 1 0 0 0])])]

        (-> (sut/buffer-chunk! init-pb cfg :a s)
            (merge-pr
             (fn [[k v]]
               (is (= ::error k)))))))

    (testing "throws if between chunks is not sorted"
      (let [cfg (sut/configure-cross-op
                 {::stream.cross/op ::stream.cross.op/sorted-merge
                  ::stream.cross/keys [[:a identity]]
                  ::stream.cross/key-comparator-fn compare})
            init-pb []
            s (stream-of [(stream.types/stream-chunk [0 0 0])])]

        (-> (sut/buffer-chunk! [[1 '(1 1 1)]] cfg :a s)
            (merge-pr
             (fn [[k v]]
               (is (= ::error k))
               (let [{error-type :error/type

                      chunk-starts-after-previous-end?
                      ::stream.cross/chunk-starts-after-previous-end?
                      :as exd} (ex-data v)]
                 (is (= ::sut/stream-not-sorted error-type))
                 (is (false? chunk-starts-after-previous-end?))))))))))

(deftest init-partition-buffers!-test)

(deftest partition-buffer-needs-filling?-test)

(deftest fill-partition-buffers!-test)

(deftest min-key-val-test)

(deftest next-selections-test)

(deftest generate-output-test)

(deftest chunk-full?-test)

(deftest chunk-not-empty?-test)

(deftest finished?-test)

(deftest op-completed?-test)

(deftest input-errored?-test)

(deftest cross*-test)

(deftest select-first-test)

(deftest select-all-test)

(deftest set-select-all-test)

(deftest ->select-fn-test)

(deftest ->merge-fn-test)

(deftest ->product-sort-fn-test)

(deftest ->finaliser-fn-test)

(deftest ->key-comparator-fn-test)

(deftest ->key-extractor-fn-test)

(deftest ->key-extractor-fns)

(deftest partition-streams-test
  (let [s (stream-of [0 1 2 3])
        t (stream-of [100 101 102 103 104])
        {r-s :s
         r-t :t
         :as psts} (sut/partition-streams
                    (sut/configure-cross-op
                     {::stream.cross/target-chunk-size 2
                      ::stream.cross/keys [[:s identity] [:t identity]]
                      ::stream.cross/op ::stream.cross.op/sorted-merge})
                    {:t t :s s})]

    ;; check correct order of linked/map
    (is (instance? LinkedMap psts ))
    (is (= [:s :t] (keys psts)))

    (pr/let [s-vs (consume r-s)
             t-vs (consume r-t)]

      (is (= [(stream.types/stream-chunk [[0] [1]])
              (stream.types/stream-chunk [[2] [3]])]
             s-vs))
      (is (= [(stream.types/stream-chunk [[100] [101]])
              (stream.types/stream-chunk [[102] [103]])
              (stream.types/stream-chunk [[104]])]
             t-vs)))))

(deftest configure-cross-op-test
  (let [cfg {::stream.cross/target-chunk-size 2
             ::stream.cross/keys [[:s identity] [:t :other_id]]
             ::stream.cross/op ::stream.cross.op/sorted-merge}

        opcfg (sut/configure-cross-op cfg)]

    (is (= (merge
            cfg
            {::stream.cross/key-extractor-fns {:s identity :t :other_id}
             ::stream.cross/key-comparator-fn compare
             ::stream.cross/select-fn sut/select-first
             ::stream.cross/merge-fn sut/merge-sorted-merge
             ::stream.cross/product-sort-fn identity
             ::stream.cross/finalizer-fn identity})
           opcfg))))

(deftest cross-test
  (testing "inner-join"
    (let [a (stream-of
             [{:id 0 :a "a00"} {:id 0 :a "a01"} {:id 2 :a "a20"}])
          b (stream-of
             [{:id 0 :b "b00"} {:id 1 :b "b10"} {:id 3 :b "b30"}])

          o-s (sut/cross
               {::stream.cross/keys [[:a :id] [:b :id]]
                ::stream.cross/op ::stream.cross.op/inner-join}
               {:a a :b b})]

      (pr/let [ovs (stream.ops/reduce
                    ::cross-test-inner-join
                    conj
                    []
                    o-s)]

        (is (= [{:a {:id 0, :a "a00"}, :b {:id 0, :b "b00"}}
                {:a {:id 0, :a "a01"}, :b {:id 0, :b "b00"}}]
               ovs)))))

  (testing "difference"
    (let [a (stream-of [0 1 2 3 4 5 6])
          b (stream-of [1 3 5])

          o-s (sut/cross
               {::stream.cross/keys [[:a identity] [:b identity]]
                ::stream.cross/op ::stream.cross.op/difference}
               {:a a :b b})]

      (pr/let [ovs (stream.ops/reduce
                    ::cross-test-inner-join
                    conj
                    []
                    o-s)]

        (is (= [{:a 0} {:a 2} {:a 4} {:a 6}]
               ovs)))))
  )

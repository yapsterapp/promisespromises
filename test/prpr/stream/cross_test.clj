(ns prpr.stream.cross-test
  (:require
   [prpr.test :refer [deftest testing is with-log-level]]
   [clojure.math.combinatorics :as combo]
   [linked.core :as linked]
   [taoensso.timbre :as log :refer [info warn error]]

   [promesa.core :as pr]
   [prpr.promise :as prpr]

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

(defmacro merge-pr
  "merge the branches of a promise"
  [p]
  `(prpr/handle-always
    ~p
    (fn [v# err#]
      (if (some? err#)
        [::error err#]
        [::ok v#]))))

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
                ;; given the data, puts 2 partitions in each chunk
                ::stream.cross/target-chunk-size 6})

          a (->> (stream-of [0 0 0 1 1 1 2 2 2])
                 (sut/partition-stream cfg :a))]

      (pr/let [pb1 (sut/buffer-chunk! [] cfg :a a)
               pb2 (sut/buffer-chunk! pb1 cfg :a a)
               pb3 (sut/buffer-chunk! pb2 cfg :a a)]

        (is (= [[0 '(0 0 0)] [1 '(1 1 1)]] pb1))

        (is (= [[0 '(0 0 0)] [1 '(1 1 1)] [2 '(2 2 2)]] pb2))

        (is (= [[0 '(0 0 0)] [1 '(1 1 1)] [2 '(2 2 2)]
                [::stream.cross/drained]] pb3)))))

  (testing "deals with stream error"
    (let [cfg (sut/configure-cross-op
               {::stream.cross/op ::stream.cross.op/sorted-merge
                ::stream.cross/keys [[:a identity]]
                ::stream.cross/target-chunk-size 6})

          a (->> (stream-of [0 0 0 2 2 2 4 4 4 5 5 5])
                 (stream/map (fn [v] (if (odd? v)
                                      (throw (ex-info "boo" {:v v}))
                                      v)))
                 (sut/partition-stream cfg :a))]

      (pr/let [pb1 (sut/buffer-chunk! [] cfg :a a)
               [_ _ [k v]:as pb2] (sut/buffer-chunk! pb1 cfg :a a)]

        (is (= [[0 '(0 0 0)] [2 '(2 2 2)]] pb1))
        (is (= [[0 '(0 0 0)] [2 '(2 2 2)]] (take 2 pb2)))
        (is (= ::stream.cross/errored k))
        (is (= {:v 5} (ex-data v))))))

  (testing "deals with empty stream"
    (let [cfg (sut/configure-cross-op
               {::stream.cross/op ::stream.cross.op/sorted-merge
                ::stream.cross/keys [[:a identity]]
                ::stream.cross/target-chunk-size 6})
          a (->> (stream-of [])
                 (sut/partition-stream cfg :a))]

      (pr/let [pb1 (sut/buffer-chunk! [] cfg :a a)]
        (is (= [[::stream.cross/drained]] pb1)))))

  (testing "throws an error if stream not sorted"
    (testing "throws if chunk is not sorted"
      (let [cfg (sut/configure-cross-op
                 {::stream.cross/op ::stream.cross.op/sorted-merge
                  ::stream.cross/keys [[:a identity]]
                  ::stream.cross/target-chunk-size 6})
            a (->> (stream-of [1 1 1 0 0 0])
                   (sut/partition-stream cfg :a))]

        (-> (merge-pr (sut/buffer-chunk! [] cfg :a a))
            (pr/chain
             (fn [[k v]] (is (= ::error k)))))))

    (testing "throws if between chunks is not sorted"
      (let [cfg (sut/configure-cross-op
                 {::stream.cross/op ::stream.cross.op/sorted-merge
                  ::stream.cross/keys [[:a identity]]
                  ::stream.cross/target-chunk-size 6})
            a (->> (stream-of [0 0 0])
                   (sut/partition-stream cfg :a))]

        (-> (merge-pr (sut/buffer-chunk! [[1 '(1 1 1)]] cfg :a a))
            (pr/chain
             (fn [[k v]]
               (is (= ::error k))
               (let [{error-type :error/type

                      chunk-starts-after-previous-end?
                      ::stream.cross/chunk-starts-after-previous-end?
                      :as exd} (ex-data v)]
                 (is (= ::sut/stream-not-sorted error-type))
                 (is (false? chunk-starts-after-previous-end?)))))))))

  (testing "throws an error if key extraction returns nil"
    (let [cfg (sut/configure-cross-op
               {::stream.cross/op ::stream.cross.op/sorted-merge
                ::stream.cross/keys [[:a identity]]
                ::stream.cross/target-chunk-size 6})
          a (->> (stream-of [0 0 0])
                 (sut/partition-stream cfg :a))]

      (-> (merge-pr (sut/buffer-chunk! [[1 '(1 1 1)]] cfg :a a))
          (pr/chain
           (fn [[k v]]
             (is (= ::error k))
             (let [{error-type :error/type

                    chunk-starts-after-previous-end?
                    ::stream.cross/chunk-starts-after-previous-end?
                    :as exd} (ex-data v)]
               (is (= ::sut/stream-not-sorted error-type))
               (is (false? chunk-starts-after-previous-end?)))))))))

(deftest init-partition-buffers!-test
  (testing "initially fills partition-buffers and correctly orders the map"
    (let [cfg (sut/configure-cross-op
               {::stream.cross/op ::stream.cross.op/sorted-merge
                ::stream.cross/keys [[:a identity] [:b identity]]
                ::stream.cross/target-chunk-size 6})
          id-streams (->> (linked/map
                           :b (stream-of [1 1 1])
                           :a (stream-of [0 0 0]))
                          (sut/partition-streams cfg))]

      (pr/let [pbs (sut/init-partition-buffers! cfg id-streams)]
        (is (= [:a :b] (keys pbs)))
        (is (= {:a [[0 [0 0 0]]]
                :b [[1 [1 1 1]]]}
               pbs))))))

(deftest partition-buffer-needs-filling?-test
  (testing "true if there is a single partition remaining and the stream is not finished"
    (is (true? (sut/partition-buffer-needs-filling? :foo [[0 '(0)]]))))
  (testing "false if there is a single partition remaining and the stream is finished"
    (is (false? (sut/partition-buffer-needs-filling? :foo [[::sut/drained]])))
    (is (false? (sut/partition-buffer-needs-filling? :foo [[::sut/errored]]))))
  (testing "false if there is more than 1 partition remaining"
    (is (false? (sut/partition-buffer-needs-filling? :foo [[0 '(0)] [1 '(1)]]))))
  (testing "errors if there is fewer than 1 partition remaining"
    (pr/let [[k v] (merge-pr (sut/partition-buffer-needs-filling? :foo []))
             {error-type :error/type
              stream-id ::sut/stream-id} (ex-data v)]
      (is (= ::error k))
      (is (= ::sut/partition-buffer-emptied error-type))
      (is (= :foo stream-id)))))

(deftest fill-partition-buffers!-test
  (testing "fills partition-buffers which need filling"
    (let [cfg (sut/configure-cross-op
               {::stream.cross/op ::stream.cross.op/sorted-merge
                ::stream.cross/keys [[:a identity] [:b identity] [:c identity]]
                ::stream.cross/target-chunk-size 6})
          id-streams (->> (linked/map
                           :d (stream-of [])
                           :c (stream-of [])
                           :b (stream-of [12 12 12])
                           :a (stream-of [1 1 1]))
                          (sut/partition-streams cfg))]

      (pr/let [pbs (sut/fill-partition-buffers!
                    (linked/map
                     :a [[0 '(0)]]
                     :b [[10 '(10)] [11 '(11)]]
                     :c [[20 '(20)] [::sut/drained]]
                     :d [[20 '(30)] [::sut/errored]])
                    cfg
                    id-streams)]

        (is (= {:a [[0 '(0)] [1 '(1 1 1)]]
                :b [[10 '(10)] [11 '(11)]]
                :c [[20 '(20)] [::sut/drained]]
                :d [[20 '(30)] [::sut/errored]]}
               pbs))))))

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
  (testing "partitions and chunks streams and correctly orders the id-streams map"
    (let [s (stream-of [0 1 2 3])
          t (stream-of [100 101 102 103 104])
          {r-s :s
           r-t :t
           :as psts} (sut/partition-streams
                      (sut/configure-cross-op
                       {::stream.cross/target-chunk-size 2
                        ::stream.cross/keys [[:s identity] [:t identity]]
                        ::stream.cross/op ::stream.cross.op/sorted-merge})
                      ;; input map is ordered wrongly
                      (linked/map :t t :s s))]

      ;; check output map is ordered correctly
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
               t-vs))))))

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

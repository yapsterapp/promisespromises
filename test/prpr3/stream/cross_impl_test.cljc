(ns prpr3.stream.cross-impl-test
  (:require
   [prpr3.test :refer [deftest testing is with-log-level]]
   [clojure.math.combinatorics :as combo]
   [linked.core :as linked]
   #?(:cljs [linked.map :refer [LinkedMap]])
   [taoensso.timbre :as log :refer [info warn error]]

   [promesa.core :as pr]
   [prpr3.promise :as prpr]

   [prpr3.stream.test :as stream.test]
   [prpr3.stream.operations :as stream.ops]
   [prpr3.stream.transport :as stream.transport]
   [prpr3.stream.types :as stream.types]
   [prpr3.stream.chunk :as stream.chunk]
   [prpr3.stream.protocols :as stream.pt]
   [prpr3.stream :as stream]

   [prpr3.stream.cross-impl :as sut]

   [prpr3.stream.cross :as-alias stream.cross]
   [prpr3.stream.cross.op :as-alias stream.cross.op]

   )
  #?(:clj (:import
           [linked.map LinkedMap])))

(deftest stream-finished?-test
  (testing "not finished when partition-buffer is empty"
    (is (not (sut/stream-finished? []))))
  (testing "not finished when partition-buffer has some content"
    (is (not (sut/stream-finished? [["blah" '({:id "blah"})]]))))
  (testing "finished when partition-buffer is drained"
    (is (sut/stream-finished? [[::sut/drained]]))
    (is (sut/stream-finished? [["blah" '({:id "blah"})]
                               [::sut/drained]])))
  (testing "finished when partition-buffer is errored"
    (is (sut/stream-finished? [[::sut/errored]]))
    (is (sut/stream-finished? [["blah" '({:id "blah"})]
                               [::sut/errored]]))))

(deftest buffer-chunk!-test

  (testing "initialises, appends and terminates a partition buffer"
    (let [cfg (sut/configure-cross-op
               {::stream.cross/op ::stream.cross.op/sorted-merge
                ::stream.cross/keys [[:a identity]]
                ;; given the data, puts 2 partitions in each chunk
                ::stream.cross/target-chunk-size 6})

          a (->> (stream.test/stream-of [0 0 0 1 1 1 2 2 2])
                 (sut/partition-stream cfg :a))]

      (pr/let [pb1 (sut/buffer-chunk! [] cfg :a a)
               pb2 (sut/buffer-chunk! pb1 cfg :a a)
               pb3 (sut/buffer-chunk! pb2 cfg :a a)]

        (is (= [[0 '(0 0 0)] [1 '(1 1 1)]] pb1))

        (is (= [[0 '(0 0 0)] [1 '(1 1 1)] [2 '(2 2 2)]] pb2))

        (is (= [[0 '(0 0 0)] [1 '(1 1 1)] [2 '(2 2 2)]
                [::sut/drained]] pb3)))))

  (testing "deals with stream error"

    (let [cfg (sut/configure-cross-op
               {::stream.cross/op ::stream.cross.op/sorted-merge
                ::stream.cross/keys [[:a identity]]
                ::stream.cross/target-chunk-size 6})

          a (->> (stream.test/stream-of [1 1 1])
                 (stream/map (fn [v] (if (odd? v)
                                      (throw (ex-info "boo" {:v v}))
                                      v)))
                 (sut/partition-stream cfg :a))]

      (pr/let [[[k1 v1]] (sut/buffer-chunk! [] cfg :a a)
               exd (ex-data v1)]

        (is (= ::sut/errored k1))
        (is (= {:v 1} exd))))

    (let [cfg (sut/configure-cross-op
               {::stream.cross/op ::stream.cross.op/sorted-merge
                ::stream.cross/keys [[:a identity]]
                ::stream.cross/target-chunk-size 6})

          a (->> (stream.test/stream-of [0 0 0 2 2 2 4 4 4 5 5 5])
                 (stream/map (fn [v] (if (odd? v)
                                      (throw (ex-info "boo" {:v v}))
                                      v)))
                 (sut/partition-stream cfg :a))]

      (pr/let [pb1 (sut/buffer-chunk! [] cfg :a a)
               [_ _ [k v]:as pb2] (sut/buffer-chunk! pb1 cfg :a a)
               [fk fv] (->> pb2
                            (filter
                             (fn [[k _v]] (sut/stream-finished-markers k)))
                            first)
               exd (ex-data fv)]

        (is (= ::sut/errored fk))
        (is (= {:v 5} (ex-data fv))))))

  (testing "deals with empty stream"
    (let [cfg (sut/configure-cross-op
               {::stream.cross/op ::stream.cross.op/sorted-merge
                ::stream.cross/keys [[:a identity]]
                ::stream.cross/target-chunk-size 6})
          a (->> (stream.test/stream-of [])
                 (sut/partition-stream cfg :a))]

      (pr/let [pb1 (sut/buffer-chunk! [] cfg :a a)]
        (is (= [[::sut/drained]] pb1)))))

  (testing "throws an error if stream not sorted"
    (testing "throws if chunk is not sorted"
      (let [cfg (sut/configure-cross-op
                 {::stream.cross/op ::stream.cross.op/sorted-merge
                  ::stream.cross/keys [[:a identity]]
                  ::stream.cross/target-chunk-size 6})
            a (->> (stream.test/stream-of [1 1 1 0 0 0])
                   (sut/partition-stream cfg :a))]

        (-> (prpr/merge-always (sut/buffer-chunk! [] cfg :a a))
            (pr/chain
             (fn [[k v]] (is (= ::prpr/error k)))))))

    (testing "throws if between chunks is not sorted"
      (let [cfg (sut/configure-cross-op
                 {::stream.cross/op ::stream.cross.op/sorted-merge
                  ::stream.cross/keys [[:a identity]]
                  ::stream.cross/target-chunk-size 6})
            a (->> (stream.test/stream-of [0 0 0])
                   (sut/partition-stream cfg :a))]

        (-> (prpr/merge-always (sut/buffer-chunk! [[1 '(1 1 1)]] cfg :a a))
            (pr/chain
             (fn [[k v]]
               (is (= ::prpr/error k))
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
          a (->> (stream.test/stream-of [0 0 0])
                 (sut/partition-stream cfg :a))]

      (-> (prpr/merge-always (sut/buffer-chunk! [[1 '(1 1 1)]] cfg :a a))
          (pr/chain
           (fn [[k v]]
             (is (= ::prpr/error k))
             (let [{error-type :error/type

                    chunk-starts-after-previous-end?
                    ::stream.cross/chunk-starts-after-previous-end?
                    :as exd} (ex-data v)]
               (is (= ::sut/stream-not-sorted error-type))
               (is (false? chunk-starts-after-previous-end?))))))))
  )

(deftest init-partition-buffers!-test
  (testing "initially fills partition-buffers and correctly orders the map"
    (let [cfg (sut/configure-cross-op
               {::stream.cross/op ::stream.cross.op/sorted-merge
                ::stream.cross/keys [[:a identity] [:b identity]]
                ::stream.cross/target-chunk-size 6})
          id-streams (->> (linked/map
                           :b (stream.test/stream-of [1 1 1])
                           :a (stream.test/stream-of [0 0 0]))
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
    (pr/let [[k v] (prpr/merge-always (sut/partition-buffer-needs-filling? :foo []))
             {error-type :error/type
              stream-id ::sut/stream-id} (ex-data v)]
      (is (= ::prpr/error k))
      (is (= ::sut/partition-buffer-emptied error-type))
      (is (= :foo stream-id)))))

(deftest fill-partition-buffers!-test
  (testing "fills partition-buffers which need filling"
    (let [cfg (sut/configure-cross-op
               {::stream.cross/op ::stream.cross.op/sorted-merge
                ::stream.cross/keys [[:a identity] [:b identity] [:c identity]]
                ::stream.cross/target-chunk-size 6})
          id-streams (->> (linked/map
                           :d (stream.test/stream-of [])
                           :c (stream.test/stream-of [])
                           :b (stream.test/stream-of [12 12 12])
                           :a (stream.test/stream-of [1 1 1]))
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

(deftest min-key-val-test
  (is (= 0 (sut/min-key-val compare [1 3 7 0 9])))
  (is (= 9 (sut/min-key-val (comp - compare) [1 3 7 9 5]))))

(deftest partition-buffer-content-drained?-test
  (is (true? (sut/partition-buffer-content-drained? [[::sut/drained]])))
  (is (false? (sut/partition-buffer-content-drained? [[0 '(0)][::sut/drained]]))))

(deftest partition-buffer-content-errored?-test
  (is (true? (sut/partition-buffer-content-drained? [[::sut/errored]])))
  (is (false? (sut/partition-buffer-content-drained? [[0 '(0)][::sut/errored]]))))

(deftest next-selections-test

  (testing "selects just the first min-key partition for sorted-merge"
    (let [cfg (sut/configure-cross-op
               {::stream.cross/op ::stream.cross.op/sorted-merge
                ::stream.cross/keys [[:a identity] [:b identity] [:c identity]]
                ::stream.cross/target-chunk-size 6})]
      (is (= [[[:a '(0 0 0)]]

              {:a [[1 '(1 1 1)]]
               :b [[1 '(1 1 1)] [2 '(2 2 2)]]
               :c [[0 '(0 0)] [3 '(3 3)]]}]
             (sut/next-selections
              cfg
              (linked/map
               :a [[0 '(0 0 0)] [1 '(1 1 1)]]
               :b [[1 '(1 1 1)] [2 '(2 2 2)]]
               :c [[0 '(0 0)] [3 '(3 3)]]))))))

  (testing "selects all min-key partitions for inner-join"
    (let [cfg (sut/configure-cross-op
               {::stream.cross/op ::stream.cross.op/inner-join
                ::stream.cross/keys [[:a identity] [:b identity] [:c identity]]
                ::stream.cross/target-chunk-size 6})]
      (is (= [[[:a '(0 0 0)] [:c '(0 0)]]

              {:a [[1 '(1 1 1)]]
               :b [[1 '(1 1 1)] [2 '(2 2 2)]]
               :c [[3 '(3 3)]]}]
             (sut/next-selections
              cfg
              (linked/map
               :a [[0 '(0 0 0)] [1 '(1 1 1)]]
               :b [[1 '(1 1 1)] [2 '(2 2 2)]]
               :c [[0 '(0 0)] [3 '(3 3)]])))))))

(deftest generate-output-test
  (testing "cartesian products the selected partitions"
    (let [cfg (sut/configure-cross-op
               {::stream.cross/op ::stream.cross.op/inner-join
                ::stream.cross/keys [[:a :id] [:b :org_id]]
                ::stream.cross/target-chunk-size 6})

          selected-id-partitions (linked/map
                                  :a [{:id 0 :a "a0_0"} {:id 0 :a "a0_1"}]
                                  :b [{:org_id 0 :b "b0_0"} {:org_id 0 :b "b0_1"}])]

      (is (= [{:a {:id 0, :a "a0_0"}, :b {:org_id 0, :b "b0_0"}}
              {:a {:id 0, :a "a0_0"}, :b {:org_id 0, :b "b0_1"}}
              {:a {:id 0, :a "a0_1"}, :b {:org_id 0, :b "b0_0"}}
              {:a {:id 0, :a "a0_1"}, :b {:org_id 0, :b "b0_1"}}]
             (sut/generate-output
              cfg
              selected-id-partitions)))))

  (testing "optionally finalizes and sorts the output"
    (let [cfg (sut/configure-cross-op
               {::stream.cross/op ::stream.cross.op/inner-join
                ::stream.cross/keys [[:a :id] [:b :org_id]]
                ::stream.cross/target-chunk-size 6
                ::stream.cross/finalizer (fn [m] (->> m (vals) (apply merge)))
                ::stream.cross/product-sort (fn [ms]
                                              (sort-by
                                               (fn [{n :n m :m}] (+ n m))
                                               ms))})

          selected-id-partitions (linked/map
                                  :a [{:id 0 :a "a0_0" :n 20} {:id 0 :a "a0_1" :n 10}]
                                  :b [{:org_id 0 :b "b0_0" :m 0} {:org_id 0 :b "b0_1" :m 100}])]

      (is (= [{:id 0, :a "a0_1", :n 10, :org_id 0, :b "b0_0", :m 0}
              {:id 0, :a "a0_0", :n 20, :org_id 0, :b "b0_0", :m 0}
              {:id 0, :a "a0_1", :n 10, :org_id 0, :b "b0_1", :m 100}
              {:id 0, :a "a0_0", :n 20, :org_id 0, :b "b0_1", :m 100}]
             (sut/generate-output
              cfg
              selected-id-partitions))))))

(deftest chunk-full?-test
  (let [cb (stream.chunk/stream-chunk-builder)
        _ (stream.pt/-start-chunk cb)
        _ (stream.pt/-add-all-to-chunk cb (vec (repeat 10 :foo)))]

    (let [cfg (sut/configure-cross-op
               {::stream.cross/op ::stream.cross.op/inner-join
                ::stream.cross/keys [[:a :id] [:b :org_id]]
                ::stream.cross/target-chunk-size 9})]
      (is (true? (sut/chunk-full? cb cfg))))

    (let [cfg (sut/configure-cross-op
               {::stream.cross/op ::stream.cross.op/inner-join
                ::stream.cross/keys [[:a :id] [:b :org_id]]
                ::stream.cross/target-chunk-size 10})]
      (is (true? (sut/chunk-full? cb cfg))))

    (let [cfg (sut/configure-cross-op
               {::stream.cross/op ::stream.cross.op/inner-join
                ::stream.cross/keys [[:a :id] [:b :org_id]]
                ::stream.cross/target-chunk-size 11})]
      (is (false? (sut/chunk-full? cb cfg))))))

(deftest chunk-not-empty?-test
  (let [cb (stream.chunk/stream-chunk-builder)
        _ (stream.pt/-start-chunk cb)
        _ (stream.pt/-add-all-to-chunk cb (vec (repeat 10 :foo)))]
    (is (true? (sut/chunk-not-empty? cb))))

  (let [cb (stream.chunk/stream-chunk-builder)
        _ (stream.pt/-start-chunk cb)]
    (is (false? (sut/chunk-not-empty? cb))))

  (let [cb (stream.chunk/stream-chunk-builder)]
    (is (false? (sut/chunk-not-empty? cb)))))

(deftest cross-finished?-test
  )

(deftest cross-input-errored?-test)

(deftest first-cross-input-error-test)

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

(defn linked-map?
  [v]
  (instance? LinkedMap v))

(deftest partition-streams-test
  (testing "partitions and chunks streams and correctly orders the id-streams map"
    (let [s (stream.test/stream-of [0 1 2 3])
          t (stream.test/stream-of [100 101 102 103 104])
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
      (is (linked-map? psts))
      (is (= [:s :t] (keys psts)))

      (pr/let [s-vs (stream.test/consume r-s)
               t-vs (stream.test/consume r-t)]

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
    (let [a (stream.test/stream-of
             [{:id 0 :a "a00"} {:id 0 :a "a01"} {:id 2 :a "a20"}])
          b (stream.test/stream-of
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
    (let [a (stream.test/stream-of [0 1 2 3 4 5 6])
          b (stream.test/stream-of [1 3 5])

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

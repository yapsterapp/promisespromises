(ns prpr.stream.cross-test
  (:require
   [clojure.math.combinatorics :as combo]
   [prpr.stream.cross :as sut]
   [manifold
    [deferred :as d]
    [stream :as s]]
   [clojure.test :as t :refer [deftest testing is]]
   [linked.core :as linked]
   [prpr.util.test :refer [with-log-level]]
   [taoensso.timbre :refer [info warn error]]
   [prpr.promise :as pr])
  (:import
   [prpr.stream.cross SortedStream]))

(deftest sorted-stream-test
  ;; instance? tests are fragile in the REPL
  ;; (testing "creates SortedStreams"
  ;;   (is (instance? sut/SortedStream
  ;;                  (sut/sorted-stream (s/stream) identity merge))))
  (testing "ISortedStream ops work correctly"
    (let [s (s/stream)
          _ (s/put! s {:foo 10})
          ss (sut/sorted-stream :foo merge s)
          v @(sut/-take! ss)]
      (is (= {:foo 10} v))
      (is (= 10 (sut/-key ss v)))
      (is (= {:foo 10 :bar 100} (sut/-merge ss {:bar 100} v)))))

  (testing "doesn't nest"
    (let [ss (sut/sorted-stream  identity merge (s/stream))
          ss2 (sut/sorted-stream identity merge ss)]
      ;; (is (instance? SortedStream ss))
      (is (identical? ss ss2)))))

(defn random-sorted-vec
  "returns a sorted vector of random integers"
  [{min-vec-sz :min-vec-sz
    max-vec-sz :max-vec-sz
    min-el-val :min-el-val
    el-val-range :el-val-range
    :or {min-vec-sz 0
         el-val-range Integer/MAX_VALUE}}]
  (assert max-vec-sz)
  (let [sz (rand-int (- (inc max-vec-sz) min-vec-sz))
        sz (+ min-vec-sz sz)

        min-el-val (or min-el-val
                       (rand-int (- Integer/MAX_VALUE el-val-range)))]
    (->> (range 0 sz)
         (map (fn [_] (+ min-el-val (rand-int el-val-range))))
         (sort)
         (vec))))

(defn random-sorted-vecs
  [{vec-cnt :vec-cnt
    min-el-val :min-el-val
    el-val-range :el-val-range
    :or {el-val-range Integer/MAX_VALUE}
    :as args}]
  (assert vec-cnt)
  (let [min-el-val (or min-el-val
                       (rand-int (- Integer/MAX_VALUE el-val-range)))]
    (vec
     (repeatedly vec-cnt #(random-sorted-vec
                           (-> args
                               (dissoc :vec-cnt)
                               (assoc :min-el-val min-el-val)))))))

(defn random-keyed-vecs
  "returns a linked/map of random vectors keyed by
   keywords formed from a 0-based index"
  [{min-vec-cnt :min-vec-cnt
    max-vec-cnt :max-vec-cnt
    :or {min-vec-cnt 1}
    :as args}]
  (assert max-vec-cnt)
  (let [cnt (rand-int (- (inc max-vec-cnt) min-vec-cnt))
        cnt (+ min-vec-cnt cnt)
        vs (random-sorted-vecs (-> args
                                   (dissoc :min-vec-cnt :max-vec-cnt)
                                   (assoc :vec-cnt cnt)))]
    (->>
     vs
     (map-indexed
      (fn [i v]
        [(keyword (str i)) v]))
     (into (linked/map)))))

(defn random-keyed-vecs-seq
  [{seq-cnt :seq-cnt
    :as args}]
  (assert seq-cnt)
  (repeatedly seq-cnt #(random-keyed-vecs (-> args
                                              (dissoc :seq-cnt)))))

(defn random-keys
  "return a non-empty set containing a random selection of keys from a
   non-empty map of stream buffers with
   sequential 0-based int-keyword stream keys"
  [kvs]
  (let [cnt (count kvs)]
    (assert (> cnt 0))
    (->> (rand-int cnt)
         inc
         (range 0)
         (map (fn [_] (rand-int cnt)))
         (distinct)
         (map (comp keyword str))
         (set))))

(defn vecs->streams
  [vs]
  (mapv s/->source vs))

(defn keyed-vecs->streams
  [kvs]
  (->> kvs
       (map (fn [[k v]] [k (s/->source v)]))
       (into (linked/map))))

(defn keyed-vecs->sorted-streams
  ([key-fn merge-fn kvs]
   (->> kvs
        (map (fn [[k v]] [k (sut/sorted-stream
                             key-fn
                             merge-fn
                             (s/->source v))]))
        (into (linked/map)))))

(deftest buffer-values-test
  (testing "empty stream"
    (let [[h n :as r] @(sut/buffer-values
                        (s/->source [])
                        nil)]
      (is (= h ::sut/drained))))

  (testing "initial values"
    (let [[h n :as r] @(sut/buffer-values
                        (s/->source [0 0 0 1])
                        nil)]
      (is (= h [0 [0 0 0]]))))

  (testing "end of stream"
    (let [[h n :as r] @(sut/buffer-values
                        (s/->source [])
                        [0 [0 0 0]])]
      (is (= r [[0 [0 0 0]]
                ::sut/drained]))))

  (testing "another value with the key then end-of-stream"
    (let [[h n :as r] @(sut/buffer-values
                        (s/->source [0])
                        [0 [0 0]])]
      (is (= r [[0 [0 0 0]]
                ::sut/drained]))))

  (testing "another value with the key then a different key"
    (let [[h n :as r] @(sut/buffer-values
                        (s/->source [0 1])
                        [0 [0 0]])]
      (is (= r [[0 [0 0 0]]
                [1 [1]]])))))

(deftest init-stream-buffers-test
  ;; max-value smaller than max-sz leads to repeated sequences
  ;; of the same int
  (doseq [kvs (random-keyed-vecs-seq {:seq-cnt 1000
                                      :max-vec-cnt 20
                                      :max-vec-sz 10
                                      :el-val-range 3})]
    (let [ss (keyed-vecs->streams kvs)
          hvs @(sut/init-stream-buffers ss)]
      (is (= (->> kvs (map
                       (fn [[k vs]]
                         (let [pvs (->> vs
                                        (partition-by identity)
                                        (map vec))]
                           (if (not-empty pvs)
                             [k [[(-> pvs first first) (first pvs)]
                                 (if (second pvs)
                                   ;; note only a *single* element of the
                                   ;; next-value items is retrieved
                                   [(-> pvs second first)
                                    [(-> pvs second first)]]
                                   ::sut/drained)]]
                             [k [::sut/drained]]))))
                  (into (linked/map)))
             hvs)))))

(deftest advance-stream-buffers-test
  (doseq [kvs (random-keyed-vecs-seq {:seq-cnt 1000
                                      :max-vec-cnt 20
                                      :max-vec-sz 10
                                      :el-val-range 3})]
    (let [nks (random-keys kvs)

          kss (keyed-vecs->streams kvs)
          hvs @(sut/init-stream-buffers kss)

          nvs @(sut/advance-stream-buffers kss hvs nks)]
      (is (= (->> (keys kvs)
                  (map (fn [k]
                         (if (nks k)
                           (let [pvs (->> (get kvs k)
                                          (partition-by identity)
                                          (map vec))

                                 [n-1 n-2] (let [[a b] (next pvs)]
                                             [(if a
                                                [(first a) a]
                                                ::sut/drained)
                                              (when a
                                                (if b
                                                  [(first b) [(-> b first)]]
                                                  ::sut/drained))])]
                             [k (filterv some? [n-1 n-2])])
                           [k (get hvs k)])))
                  (into (linked/map)))
             nvs)))))

(deftest min-key-skey-values-test
  (doseq [kvs (random-keyed-vecs-seq {:seq-cnt 1000
                                      :max-vec-cnt 20
                                      :max-vec-sz 10
                                      :el-val-range 3})]
    (let [kss (keyed-vecs->streams kvs)
          hvs @(sut/init-stream-buffers kss)

          mkskvs (sut/min-key-skey-values
                  compare
                  kss
                  hvs)]
      (is (= (->> kvs
                  (map (fn [[k vs]]
                         (let [pvs (partition-by identity vs)]
                           (when (not-empty pvs)
                             [k (-> pvs first first) (first pvs)]))))
                  (filter some?)
                  (sort-by second)
                  (partition-by second)
                  first
                  (map (fn [[sk vk vs]]
                         [sk vs]))
                  (into (linked/map)))
             mkskvs)))))

(deftest select-skey-values-test
  (doseq [kvs (random-keyed-vecs-seq {:seq-cnt 1000
                                      :max-vec-cnt 20
                                      :max-vec-sz 10
                                      :el-val-range 3})]
    (let [kss (keyed-vecs->streams kvs)
          hvs @(sut/init-stream-buffers kss)

          mkskvs (sut/min-key-skey-values
                  compare
                  kss
                  hvs)]

      ;; (info "mkskvs" mkskvs)

      (testing "select-first"
        (let [sel-skvs (sut/select-skey-values
                        sut/select-first
                        mkskvs)]
          (is (= (->> mkskvs
                      (take 1)
                      (into (linked/map)))
                 sel-skvs))))

      (testing "select-all"
        (let [sel-skvs (sut/select-skey-values
                        sut/select-all
                        mkskvs)]
          (is (= mkskvs
                 sel-skvs)))))))

(deftest merge-stream-objects-test
  ;; limit the max-vec-sz to 1 so there are no repeated
  ;; skeys in skey-vals
  (doseq [kvs (random-keyed-vecs-seq {:seq-cnt 1000
                                      :max-vec-cnt 20
                                      :max-vec-sz 1
                                      :el-val-range 3})]
    (let [kss (keyed-vecs->sorted-streams
               identity
               (fn [o [sk v]] (+ o v))
               kvs)
          hvs @(sut/init-stream-buffers kss)

          mkskvs (sut/min-key-skey-values
                  compare
                  kss
                  hvs)

          sel-skvs (sut/select-skey-values
                    sut/select-all
                    mkskvs)

          skey-vals (->> sel-skvs
                         (map (fn [[sk vs]]
                                (for [v vs]
                                  [sk v])))
                         (apply concat))

          mos (sut/merge-stream-objects
               0
               kss
               skey-vals)]
      (is (=  (->> skey-vals
                   (map second)
                   (reduce + 0))
             mos)))))

(deftest head-values-cartesian-product-merge-test
  (doseq [;; ensure no empty streams with :min-vec-sz
          ;; also be careful - it will generate
          ;; ~ :max-vec-sz ^ max-vec-cnt
          ;; records
          kvs (random-keyed-vecs-seq {:seq-cnt 1000
                                      :max-vec-cnt 5
                                      :min-vec-sz 1
                                      :max-vec-sz 10
                                      :el-val-range 3})]
    (let [kss (keyed-vecs->streams kvs)
          hvs @(sut/init-stream-buffers kss)

          mkskvs (sut/min-key-skey-values
                  compare
                  kss
                  hvs)

          kvcp (sut/head-values-cartesian-product-merge
                []
                kss
                mkskvs)]
      ;; (info "mkskvs" mkskvs)
      ;; (info "kvcp" (vec kvcp))

      (is (= (->> mkskvs
                  (map (fn [[sk hvals]]
                         (for [v hvals]
                           [sk v])))
                  (apply combo/cartesian-product)
                  (map #(sut/merge-stream-objects [] kss %)))
             kvcp)))))

(deftest min-key-val-test
  (is (= 1 (sut/min-key-val compare [3 2 1])))
  (is (= 3 (sut/min-key-val (comp - compare) [3 2 1]))))

(deftest next-output-values-test
  (testing "next-output-values"
    (doseq [kvs (concat
                 ;; always try degenerate all-empty-streams cases
                 [{:0 []}]
                 [{:0 [] :1 []}]
                 (random-keyed-vecs-seq {:seq-cnt 1000
                                         :max-vec-cnt 20
                                         :max-vec-sz 100
                                         :el-val-range 5})
                 )]
      (let [kss (keyed-vecs->sorted-streams
                 identity
                 (fn [o [sk v]] (conj o [sk v]))
                 kvs)

            skey-streambufs @(sut/init-stream-buffers kss)

            all-drained? (->> skey-streambufs
                              (filter (fn [[sk [hv]]]
                                        (not= ::sut/drained hv)))
                              (count)
                              (= 0))

            ;; _ (warn "skey-streambufs" skey-streambufs)

            [output-values
             next-skey-head-values] @(sut/next-output-values
                                      compare
                                      sut/select-first
                                      {}
                                      kss
                                      skey-streambufs)]
        ;; (warn "kvs" kvs)
        (if all-drained?
          (do
            (is (= ::sut/drained output-values))
            (is (= nil next-skey-head-values)))
          (do
            (let [expected-result (->> kvs
                                       (filter #(not-empty (second %)))
                                       (sort-by (comp first second))
                                       (take 1)
                                       (map (fn [[k vs]] (->> vs
                                                              (partition-by identity)
                                                              first
                                                              (map (fn [v] {k v})))))
                                       (apply concat))]
              (when (not= expected-result output-values)
                (warn "bad-kvs" kvs)
                (warn "bad-skey-streambufs" skey-streambufs)
                (warn "bad-expected-result" expected-result)
                (warn "bad-output-values" (vec output-values)))
              (is (= expected-result
                     output-values))))))))

  (testing "throws if selector-fn doesn't select at least one value"
    (let [s0 (s/->source [1 2 3])
          s1 (s/->source [1 2 3])
          kss {:0 s0 :1 s1}]
      (is (thrown-with-msg?
           Exception
           #"selector-fn failed to choose"
           (sut/next-output-values
            compare
            (fn [& args] nil)
            {}
            kss
            {:0 [[1 [1]] ::sut/drained] :1 [[1 [1]] ::sut/drained]})))))

  (testing "throws if selector-fn chooses a wrong value"
    (let [s0 (s/->source [1 2 3])
          s1 (s/->source [1 2 3])
          kss {:0 s0 :1 s1}]
      (is (thrown-with-msg?
           Exception
           #"selector-fn failed to choose"
           (sut/next-output-values
            compare
            (fn [& args] [::blah])
            {}
            kss
            {:0 [[1 [1]] ::sut/drained] :1 [[1 [1]] ::sut/drained]})))))
  )

(deftest cross-streams-test
  (testing "streams get crossed"
    (doseq [kvs (concat
                 ;; always try degenerate all-empty-streams cases
                 [{:0 []}]
                 [{:0 [] :1 []}]
                 ;; combinatorics can generate massive data
                 ;; :max-vec-size ^ :max-vec-cnt
                 (random-keyed-vecs-seq {:seq-cnt 1000
                                         :max-vec-cnt 3
                                         :max-vec-sz 100
                                         :el-val-range 10}))]
      ;; (warn "vs" kvs)
      (let [kss (keyed-vecs->streams kvs)
            os @(sut/cross-streams
                 compare
                 sut/select-first
                 {}
                 kss)
            ovs @(s/reduce conj [] os)]
        (is (= (->> kvs
                    (mapcat (fn [[k vs]] (for [v vs] [k v])))
                    (sort-by second)
                    (map (fn [[k v]] {k v})))
               ovs))))
    )

  (with-log-level :error
    (testing "errors during setup close the sources"
      (let [s0 (s/stream)
            s1 (s/stream)
            _ (doseq [v [1 2 3]] (s/put! s0 v))
            _ (doseq [v [1 2 3]] (s/put! s1 v))
            kss {:0 s0 :1 s1}
            os-d (sut/cross-streams
                  (fn [& args] (throw (ex-info "boo" {})))
                  sut/select-first
                  nil
                  kss)]
        (is (s/drained? @os-d))
        (is (s/closed? s0))
        (is (s/closed? s0)))))

  (with-log-level :error
    (testing "errors after setup close the sources and the output"
      (let [s0 (s/stream)
            s1 (s/stream)
            _ (doseq [v [1 2 3]] (s/put! s0 v))
            _ (doseq [v [1 2 3]] (s/put! s1 v))
            kss {:0 s0 :1 s1}
            os @(sut/cross-streams
                 (let [v (atom 0)]
                   (fn [& args]
                     (if (> (swap! v inc) 1)
                       (throw (ex-info "hoo" {}))
                       (apply compare args))))
                 sut/select-first
                 {}
                 kss)
            ovs @(s/reduce conj [] os)]

        (is (s/closed? s0))
        (is (s/closed? s1))
        (is (s/drained? os))
        (is (= [{:0 1}]
               ovs)))))

  (with-log-level :fatal
    (testing "errors on derived streams don't cause hangs"
      ;; like they used to during participant-stream processing
      (let [s0 (s/stream)
            s1 (s/stream)
            kss {:0 s0 :1 s1}
            _ (doseq [[k s] kss]
                (doseq [v [1 2 3]]
                  (s/put! s v))
                (s/close! s))

            cs @(sut/cross-streams
                 clojure.core/compare
                 sut/select-first
                 {}
                 kss)
            ms (s/map (fn [k-v]
                        (let [[k v] (first k-v)]
                          (if (< v 2)
                            (inc v)
                            (throw (ex-info "woo" {})))))
                      cs)
            ovs @(s/reduce conj [] ms)]

        (is (s/closed? s0))
        (is (s/closed? s1))
        (is (s/drained? ms))
        ;; (is (s/drained? cs)) ;; this is reasonable

        (is (= [2 2] ovs))))))

(deftest sort-merge-streams-test
  (doseq [kvs (random-keyed-vecs-seq {:seq-cnt 1000
                                      :max-vec-cnt 20
                                      :max-vec-sz 100
                                      :el-val-range 5})]
    (let [kss (keyed-vecs->streams kvs)
          os @(sut/sort-merge-streams kss)
          ovs @(s/reduce conj [] os)]
      (is (= (->> kvs
                  vals
                  (apply concat)
                  sort)
             ovs)))))

(deftest full-outer-join-records-test
  (testing "with a single key-fn"
    (let [s0 (s/->source [{:foo 1 :bar 10} {:foo 3 :bar 30} {:foo 4 :bar 40}])
          s1 (s/->source [{:foo 1 :baz 100} {:foo 2 :baz 200} {:foo 3 :baz 300}])
          kss {:0 s0 :1 s1}

          os @(sut/full-outer-join-streams :foo kss)
          ovs @(s/reduce conj [] os)]

      (is (= [{:0 {:foo 1 :bar 10} :1 {:foo 1 :baz 100}}
              {:1 {:foo 2 :baz 200}}
              {:0 {:foo 3 :bar 30} :1 {:foo 3 :baz 300}}
              {:0 {:foo 4 :bar 40}}]
             ovs))))

  (testing "1-many joins"
    (let [s0 (s/->source [{:foo 1 :bar 10}
                          {:foo 3 :bar 30}
                          {:foo 3 :bar 40}
                          {:foo 4 :bar 40}])
          s1 (s/->source [{:foo 1 :baz 100}
                          {:foo 1 :baz 200}
                          {:foo 2 :baz 200}
                          {:foo 3 :baz 300}])
          kss {:0 s0 :1 s1}

          os @(sut/full-outer-join-streams :foo kss)
          ovs @(s/reduce conj [] os)]

      (is (= [{:0 {:foo 1 :bar 10} :1 {:foo 1 :baz 100}}
              {:0 {:foo 1 :bar 10} :1 {:foo 1 :baz 200}}
              {:1 {:foo 2 :baz 200}}
              {:0 {:foo 3 :bar 30} :1 {:foo 3 :baz 300}}
              {:0 {:foo 3 :bar 40} :1 {:foo 3 :baz 300}}
              {:0 {:foo 4 :bar 40}}]
             ovs))))

  (testing "many-many joins"
    (with-log-level :error
      (let [s0 (s/->source [{:foo 1 :bar 10}
                            {:foo 3 :bar 30}
                            {:foo 3 :bar 40}
                            {:foo 4 :bar 40}])
            s1 (s/->source [{:foo 1 :baz 100}
                            {:foo 3 :baz 300}
                            {:foo 3 :baz 400}])
            kss {:0 s0 :1 s1}

            os @(sut/full-outer-join-streams :foo kss)
            ovs @(s/reduce conj [] os)]

        (is (= [{:0 {:foo 1 :bar 10} :1 {:foo 1 :baz 100}}
                {:0 {:foo 3 :bar 30} :1 {:foo 3 :baz 300}}
                {:0 {:foo 3 :bar 30} :1 {:foo 3 :baz 400}}
                {:0 {:foo 3 :bar 40} :1 {:foo 3 :baz 300}}
                {:0 {:foo 3 :bar 40} :1 {:foo 3 :baz 400}}
                {:0 {:foo 4 :bar 40}}]
               ovs)))))

  (testing "with passed ISortedStreams with custom key-fns"
    (let [s0 (s/->source [{:foo 1 :bar 10} {:foo 3 :bar 30} {:foo 4 :bar 40}])
          s1 (s/->source [{:foofoo 1 :baz 100}
                          {:foofoo 2 :baz 200}
                          {:foofoo 3 :baz 300}])
          ss1 (sut/sorted-stream :foofoo conj s1)
          kss {:0 s0 :1 ss1}

          os @(sut/full-outer-join-streams :foo kss)
          ovs @(s/reduce conj [] os)]

      (is (= [{:0 {:foo 1 :bar 10} :1 {:foofoo 1 :baz 100}}
              {:1 {:foofoo 2 :baz 200}}
              {:0 {:foo 3 :bar 30} :1 {:foofoo 3 :baz 300}}
              {:0 {:foo 4 :bar 40}}]
             ovs)))))

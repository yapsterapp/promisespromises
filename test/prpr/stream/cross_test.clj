(ns prpr.stream.cross-test
  (:require
   [prpr.stream.cross :as sut]
   [manifold
    [deferred :as d]
    [stream :as s]]
   [clojure.test :as t :refer [deftest testing is]]
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


(defn random-vecs
  "creates a random-length (<sz) vector,
   with random postitive integers,
   partitions it randomly and sorts each partition"
  [max-vec-sz]
  (let [vec-sz (inc (rand-int max-vec-sz))
        part-sz (inc (rand-int vec-sz))
        base (quot Integer/MAX_VALUE 2)]
    (->> (range 0 vec-sz)
         (map (fn [_] (rand-int Integer/MAX_VALUE)))
         (partition part-sz part-sz [])
         (map sort)
         (mapv vec))))

(defn random-keyed-vecs
  "returns a map of random vectors keyed by
   keywords formed from a 0-based index"
  [max-vec-sz]
  (let [vs (random-vecs max-vec-sz)]
    (->>
     vs
     (reduce-kv
      (fn [m i v]
        (assoc m (keyword (str i)) v))
      {}))))

(defn vecs->streams
  [vs]
  (mapv s/->source vs))

(defn keyed-vecs->streams
  [kvs]
  (->> kvs
       (map (fn [[k v]] [k (s/->source v)]))
       (into {})))

(defn random-vecs-seq
  [cnt max-vec-sz]
  (repeatedly cnt #(random-vecs max-vec-sz)))

(defn random-keyed-vecs-seq
  [cnt max-vec-sz]
  (repeatedly cnt #(random-keyed-vecs max-vec-sz)))



(deftest head-values-test
  (doseq [kvs (random-keyed-vecs-seq 100 100)]
    (let [ss (keyed-vecs->streams kvs)
          hvs @(sut/head-values ss)]
      (is (= (->> kvs (map (fn [[k vs]] [k (first vs)])) (into {}))
             hvs)))))

(deftest next-values-test
  (doseq [kvs (random-keyed-vecs-seq 100 100)]
    (let [cnt (count kvs)
          nks (->> (rand-int cnt)
                   (range 0)
                   (map (fn [_] (rand-int cnt)))
                   (distinct)
                   (sort)
                   (map (comp keyword str)))

          kss (keyed-vecs->streams kvs)
          nvs @(sut/next-values kss nks)]
      (is (= (->> nks
                  (map (fn [k] [k (first (get kvs k))]))
                  (into {}))
             nvs)))))

(deftest min-key-val-test
  (is (= 1 (sut/min-key-val compare [3 2 1])))
  (is (= 3 (sut/min-key-val (comp - compare) [3 2 1]))))

(deftest next-output-value-test
  (testing "next-output-value"
    (doseq [kvs (random-keyed-vecs-seq 100 100)]
      (let [kss (keyed-vecs->streams kvs)
            skey-head-values @(sut/head-values kss)
            [output-value
             next-skey-head-values] @(sut/next-output-value
                                      compare
                                      sut/select-first
                                      {} ;; to conj with [sk hv]
                                      kss
                                      skey-head-values)]
        (is (= (->> kvs
                    (sort-by (comp first second))
                    (take 1)
                    (map (fn [[k vs]] [k (first vs)]))
                    (into {}))
               output-value)))))

  (testing "throws if selector-fn doesn't select at least one value"
    (let [s0 (s/->source [1 2 3])
          s1 (s/->source [1 2 3])
          kss {:0 s0 :1 s1}]
      (is (thrown-with-msg?
           Exception
           #"selector-fn failed to choose"
           (sut/next-output-value
            compare
            (fn [& args] nil)
            {}
            kss
            {:0 1 :1 1})))))

  (testing "throws if selector-fn chooses a wrong value"
    (let [s0 (s/->source [1 2 3])
          s1 (s/->source [1 2 3])
          kss {:0 s0 :1 s1}]
      (is (thrown-with-msg?
           Exception
           #"selector-fn failed to choose"
           (sut/next-output-value
            compare
            (fn [& args] [::blah])
            {}
            kss
            {:0 1 :1 1}))))))

(deftest cross-streams-test
  (testing "streams get crossed"
    (doseq [kvs (random-keyed-vecs-seq 100 1000)]
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
               ovs)))))

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

        (is (= [2 2] ovs)))))
  )

(deftest sort-merge-streams-test
  (doseq [kvs (random-keyed-vecs-seq 100 1000)]
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

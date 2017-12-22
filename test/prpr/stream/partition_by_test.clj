(ns prpr.stream.partition-by-test
  (:require
   [clojure.test :as t
    :refer [deftest testing is]]
   [prpr.stream.partition-by :as sut]
   [manifold.stream :as s]
   [manifold.deferred :as d]))

(deftest partition-by-test
  (testing "partitions empty stream"
    (let [s (sut/partition-by :foo (s/->source []))]
      (is (empty? (s/stream->seq s)))))

  (testing "partitions single-element stream"
    (let [s (sut/partition-by :foo (s/->source [{:foo :a}]))]
      (is (= [[{:foo :a}]]
             (s/stream->seq s)))))

  (testing "partitions several single-element partitions"
    (let [s (sut/partition-by :foo (s/->source [{:foo :a}
                                                {:foo :b}
                                                {:foo :c}]))]
      (is (= [[{:foo :a}]
              [{:foo :b}]
              [{:foo :c}]]
             (s/stream->seq s)))))

  (testing "partitions some longer partitions"
    (let [s (sut/partition-by :foo (s/->source [{:foo :a :bar 1}
                                                {:foo :a :bar 2}
                                                {:foo :b :bar 3}
                                                {:foo :b :bar 4}
                                                {:foo :b :bar 5}
                                                {:foo :c :bar 6}
                                                {:foo :c :bar 7}]))]
      (is (= [[{:foo :a :bar 1}{:foo :a :bar 2}]
              [{:foo :b :bar 3}{:foo :b :bar 4}{:foo :b :bar 5}]
              [{:foo :c :bar 6}{:foo :c :bar 7}]]
             (s/stream->seq s))))))

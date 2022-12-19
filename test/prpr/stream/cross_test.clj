(ns prpr.stream.cross-test
  (:require
   [prpr.test :refer [deftest testing is with-log-level]]
   [clojure.math.combinatorics :as combo]
   [linked.core :as linked]
   [taoensso.timbre :as log :refer [info warn error]]

   [promesa.core :as pr]

   [prpr.stream.operations :as stream.ops]
   [prpr.stream.transport :as stream.transport]

   [prpr.stream.cross :as sut]
   [prpr.stream.cross :as-alias stream.cross]
   [prpr.stream.cross.op :as-alias stream.cross.op]
   ))

(defn stream-of
  "returns a stream of the individual values
   (*not* chunked)"
  [vs]
  (let [s (stream.transport/stream)]
    (stream.transport/put-all-and-close! s vs)
    s))

(deftest stream-finished?-test
  (is (not (sut/stream-finished? [])))
  (is (not (sut/stream-finished? [["blah" '({:id "blah"})]])))
  (is (sut/stream-finished? [::stream.cross/drained]))
  (is (sut/stream-finished? [::stream.cross/errored])))


(deftest buffer-chunk!-test
  (testing "initialises a partition buffer")
  (testing "appends to a partition buffer")
  (testing "deals with stream-end")
  (testing "deals with stream error")
  (testing "deals with empty stream")
  (testing "errors if stream not sorted"))

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

(deftest partition-streams-test)

(deftest configure-cross-op-test)

(deftest cross-test
  (testing "inner-join"
    (let [a (stream-of
             [{:id 0 :a "a00"} {:id 0 :a "a01"} {:id 2 :a "a20"}])
          b (stream-of
             [{:id 0 :b "b00"} {:id 1 :b "b10"} {:id 3 :b "b30"}])

          o-s (sut/cross
               {::stream.cross/keys {:a :id :b :id}
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
               {::stream.cross/keys {:a identity :b identity}
                ::stream.cross/op ::stream.cross.op/difference}
               {:a a :b b})]

      (pr/let [ovs (stream.ops/reduce
                    ::cross-test-inner-join
                    conj
                    []
                    o-s)]

        (is (= [{:a 0} {:a 2} {:a 4} {:a 6}]
               ovs))))))

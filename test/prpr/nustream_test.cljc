(ns prpr.nustream-test
  (:require
   #?(:clj [prpr.test :refer [deftest testing is]]
      :cljs [prpr.test :refer-macros [deftest testing is]])
   [promesa.core :as pr]
   [prpr.nustream :as sut]))

(deftest realize-each-test
  (testing "does nothing to non-promise values"
    (let [s (sut/stream)
          t (sut/realize-each s)
          _ (pr/chain
             (sut/put-all! s [0 1 2 3])
             (fn [_] (sut/close! s)))]

      (pr/let [vs (->> (range 0 5)
                       (map (fn [_](sut/take! t ::closed)))
                       (pr/all))]
        (is (= [0 1 2 3 ::closed] vs)))))

  (testing "realizes promise values"
    (let [s (sut/stream)
          t (sut/realize-each s)
          _ (pr/chain
             (sut/put-all! s (map pr/resolved [0 1 2 3]))
             (fn [_] (sut/close! s)))]

      (pr/let [vs (->> (range 0 5)
                       (map (fn [_](sut/take! t ::closed)))
                       (pr/all))]
        (is (= [0 1 2 3 ::closed] vs)))))

  (testing "correctly propagates nil values"
    (let [s (sut/stream)
          t (sut/realize-each s)
          _ (pr/chain
             (sut/put-all!
              s
              [(pr/resolved 0)
               (pr/resolved nil)
               2
               nil])
             (fn [_] (sut/close! s)))]

      (pr/let [vs (->> (range 0 5)
                       (map (fn [_](sut/take! t ::closed)))
                       (pr/all))]
        (is (= [0 nil 2 nil ::closed] vs))))))

(deftest stream-error-capturing-stream-xform-test)

(deftest transform-test)

(deftest map-test
  (testing "maps a stream"
    (let [s (sut/stream)
          t (sut/map inc s)
          _ (pr/chain
             (sut/put-all! s [0 1 2 3])
             (fn [_] (sut/close! s)))]

      (pr/let [vs (->> (range 0 5)
                       (map (fn [_](sut/take! t ::closed)))
                       (pr/all))]
        (is (= [1 2 3 4 ::closed] vs)))))

  (testing "maps multiple streams"
    (testing "maps multiple streams of the same size"
      (let [a (sut/stream)
            b (sut/stream)
            t (sut/map #(+ %1 %2) a b)
            _ (pr/chain
               (sut/put-all! a [0 1 2 3])
               (fn [_] (sut/close! a)))
            _ (pr/chain
               (sut/put-all! b [0 1 2 3])
               (fn [_] (sut/close! b)))]

      (pr/let [vs (->> (range 0 5)
                       (map (fn [_](sut/take! t ::closed)))
                       (pr/all))]
        (is (= [0 2 4 6 ::closed] vs))))))

  (testing "terminates the output when any of the inputs terminates"
    (let [a (sut/stream)
          b (sut/stream)
          t (sut/map #(+ %1 %2) a b)
          _ (pr/chain
             (sut/put-all! a [0 1 2 3])
             (fn [_] (sut/close! a)))
          _ (sut/put-all! b [0 1 2 3 4 5])]

      (pr/let [vs (->> (range 0 5)
                       (map (fn [_](sut/take! t ::closed)))
                       (pr/all))]
        (is (= [0 2 4 6 ::closed] vs)))))

  (testing "when receiving an error propagates it to the downstream")
  (testing "when receiving a nil wrapper sends nil to the mapping fn")
  (testing "when mapping-fn returns a nil value, wraps it for the output")
  (testing "catches mapping fn errors, errors the output and cleans up"))

(deftest zip-test
  (testing "zips some streams")
  (testing "cleanly terminates the output when any one of the inputs terminates"))

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
  (testing "returns reductinos on the output stream")
  (testing "returns reducing function errors")
  (testing "when receiving a nil wrapper sends nil to the reducing fn"))

(deftest reduce-test
  (testing "reduces a stream")
  (testing "returns reducing function errors")
  (testing "when receiving a nil wrapper sends nil to the reducing fn"))

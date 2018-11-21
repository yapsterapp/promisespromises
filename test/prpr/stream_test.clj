(ns prpr.stream-test
  (:require
   [clojure.test :as t :refer [deftest testing is are use-fixtures]]
   [prpr.util.test :refer [with-log-level]]
   [cats.core :refer [return]]
   [manifold.deferred :as d]
   [manifold.stream :as s]
   [prpr.stream :as sut]
   [taoensso.timbre :refer [debug info warn]])
  (:import
   [prpr.stream StreamError]))

(deftest s-first-test
  (testing "empty stream"
    (let [s (s/stream)
          _ (s/close! s)]
      (is (= ::sut/none @(sut/s-first ::sut/none s)))))
  (testing "empty stream default no-val"
    (let [s (s/stream)
          _ (s/close! s)]
      (is (nil? @(sut/s-first s)))))
  (testing "first value"
    (let [s (s/stream 3)
          _ @(s/put-all! s [:foo :bar :baz])
          _ (s/close! s)]
      (is (= :foo @(sut/s-first s))))))

(deftest divert-stream-errors-test
  (testing "empty source"
    (let [s (s/stream)
          _ (s/close! s)
          [err o] (sut/divert-stream-errors s)]
      (is (= [] (s/stream->seq o)))
      (is (= [] (s/stream->seq err)))))

  (testing "no errors source"
    (let [s (s/stream 3)
          _ @(s/put-all! s [:foo :bar :baz])
          _ (s/close! s)
          [err o] (sut/divert-stream-errors s)]
      (is (= [:foo :bar :baz] (s/stream->seq o)))
      (is (= [] (s/stream->seq err)))))

  (testing "all errors source"
    (let [s (s/stream 3)
          _ @(s/put-all! s [(StreamError.
                             (ex-info "foo" {:foo :foo}))
                            (StreamError.
                             (ex-info "bar" {:bar :bar}))
                            (StreamError.
                             (ex-info "baz" {:baz :baz}))])
          _ (s/close! s)
          [err o] (sut/divert-stream-errors s)]
      ;; have to convert err first or it will block
      (is (= [{:foo :foo}
              {:bar :bar}
              {:baz :baz}]
             (map (comp ex-data :error) (s/stream->seq err))))
      (is (= [] (s/stream->seq o)))))
  (testing "mixed source"
    (let [s (s/stream 3)
          _ @(s/put-all! s [:foo
                            (StreamError.
                             (ex-info "bar" {:bar :bar}))
                            :baz])
          _ (s/close! s)
          [err o] (sut/divert-stream-errors s)]
      ;; have to take in the right order or will block
      (is (= :foo @(s/take! o)))
      (is (= {:bar :bar}
             (-> @(s/take! err)
                 :error
                 ex-data)))
      (is (= :baz @(s/take! o)))
      (is (= ::closed @(s/take! o ::closed)))
      (is (= ::closed @(s/take! err ::closed))))))

(deftest reduce-all-throw-test
  (with-log-level :error
    (testing "empty source"
      (let [s (s/stream)
            _ (s/close! s)
            r @(sut/reduce-all-throw "empty-source" conj [] s)]
        (is (= [] r))))

    (testing "no errors source"
      (let [s (s/stream 3)
            _ @(s/put-all! s [:foo :bar :baz])
            _ (s/close! s)
            r @(sut/reduce-all-throw "no-errors-source" conj [] s)]
        (is (= [:foo :bar :baz] r))))

    (testing "all errors source"
      (let [s (s/stream 3)
            _ @(s/put-all! s [(StreamError.
                               (ex-info "foo" {:foo :foo}))
                              (StreamError.
                               (ex-info "bar" {:bar :bar}))
                              (StreamError.
                               (ex-info "baz" {:baz :baz}))])
            _ (s/close! s)
            r (sut/reduce-all-throw
               "all-errors-source"
               conj
               []
               s)
            ed (ex-data (d/error-value r ::oops))]
        (is (= {:foo :foo} ed))))

    (testing "mixed source"
      (let [s (s/stream 3)
            _ @(s/put-all! s [:foo
                              (StreamError.
                               (ex-info "bar" {:bar :bar}))
                              :baz])
            _ (s/close! s)
            r (sut/reduce-all-throw
               "mixed-source"
               conj
               []
               s)
            ed (ex-data (d/error-value r ::oops))]
        (is (= {:bar :bar} ed))))

    (testing "no init"
      (let [s (s/stream 3)
            _ @(s/put-all! s [1 2 3])
            _ (s/close! s)
            r @(sut/reduce-all-throw
                "no-init"
                +
                s)]
        (is (= 6 r))))

    (testing "mixed no init"
      (let [s (s/stream 3)
            _ @(s/put-all! s [1
                              (StreamError.
                               (ex-info "bar" {:bar :bar}))
                              3])
            _ (s/close! s)
            r (sut/reduce-all-throw
               "mixed-no-init"
               +
               s)
            ed (ex-data (d/error-value r ::oops))]
        (is (= {:bar :bar} ed))))

    (testing "throw gets caught"
      (let [s (s/stream 1)
            ev (d/chain (d/success-deferred :foo)
                        (fn [v]
                          (throw (ex-info "boo" {:foo ::foo}))))
            _ @(s/put! s ev)
            _ (s/close! s)
            r (sut/reduce-all-throw
               "throw"
               conj
               []
               s)
            ed (ex-data (d/error-value r ::oops))]
        (is (= {:foo ::foo} ed))))))

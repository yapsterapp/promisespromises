(ns prpr.stream.impl-test
  #?(:cljs
     (:require-macros
      [prpr.test
       :refer [defprtest testing is]
       :rename {defprtest deftest}]))
  (:require
   [promesa.core :as pr]
   #?(:clj [prpr.test
            :refer [defprtest testing is]
            :rename {defprtest deftest}]
      :cljs [prpr.test])
   [prpr.stream.protocols :as pt]
   [prpr.stream.types :as types]
   [prpr.stream.impl :as sut]))

(deftest stream-test
  (testing "returns an object which tests stream?"
    (let [s (sut/stream)]
      (is (sut/stream? s)))))

(deftest close!-test
  (testing "close! returns nil, and after close! -put! returns false"
    (pr/let [s (sut/stream 1)
             r (sut/close! s)
             pr (pt/-put! s ::foo)]
      (is (nil? r))
      (is (not pr)))))

(deftest put!-test
  (testing "put! returns true and is take!able"
    (let [s (sut/stream)
          prp (pt/-put! s ::foo)]
      (pr/let [r (pt/-take! s)
               pr prp]
              (is (= ::foo r))
              (is (identical? true pr)))))
  (testing "returns false when the stream is closed"
    (pr/let [s (sut/stream)
             _ (sut/close! s)
             r (pt/-put! s ::foo)]
      (is (identical? false r))))
  (testing "implements a timeout with timeout-val"
    (pr/let [s (sut/stream 1)
             r (sut/put! s ::foo 1 ::timeout)
             tr (sut/take! s)]
      (is (identical? true r))
      (is (= ::foo tr)))
    (pr/let [s (sut/stream)
             r (sut/put! s ::foo 1 ::timeout)]
      (is (= ::timeout r)))
    (pr/let [s (sut/stream)
             r (sut/put! s ::foo 1 nil)]
      (is (nil? r)))))

(deftest error!-test
  (testing "put!s a StreamError and close!s the stream"
    (pr/let [s (sut/stream 1)
             er (sut/error! s ::foo)
             r (pt/-take! s)
             cv (pt/-take! s ::closed)]
      (is (identical? false er))
      (is (types/stream-error? r))
      (is (= ::foo (pt/-unwrap-error r)))
      (is (= ::closed cv)))))

(deftest put-all!-test
  (testing "puts all the values in a collection and returns true"
    (let [s (sut/stream)
          par (sut/put-all! s [::foo ::bar ::baz])]
      (pr/let [r1 (sut/take! s)
               r2 (sut/take! s)]
        (is (= ::foo r1))
        (is (= ::bar r2))
        (is (identical? false (pr/resolved? par)))
        (pr/let [r3 (sut/take! s)
                 _ (is (= ::baz r3))
                 par par]
          (is (identical? true par)))))))

(deftest take!-test
  (testing "retuns values from a stream"
    (pr/let [s (sut/stream 1)
             _ (sut/put! s ::foo)
             r (sut/take! s)]
      (is (= ::foo r))))
  (testing "returns nil when a stream is closed"
    (pr/let [s (sut/stream 1)
             _ (sut/close! s)
             _ (sut/put! s ::foo)
             r (sut/take! s)]
      (is (nil? r))))
  (testing "returns the default-val when a stream is closed"
    (pr/let [s (sut/stream 1)
             _ (sut/close! s)
             _ (sut/put! s ::foo)
             r (sut/take! s ::closed)]
      (is (= ::closed r))))
  (testing "implements a timeout with timeout-val"
    (pr/let [s (sut/stream)
             r (sut/take! s ::closed 1 ::timeout)]
      (is (= ::timeout r))))
  (testing "throws an exception on a timeout with no timeout-val"
    (pr/let [s (sut/stream)
             r (sut/take! s ::closed 1 nil)]
      (is (nil? r)))))

(deftest connect-via-error-fn-test
  (testing "applies f, puts the result on the sink, returs true"
    (let [t (sut/stream)
          f (sut/connect-via-error-fn inc t)
          frp1 (f 0)
          frp2 (f 1)]
      (pr/let [_ (sut/close! t)
               t1 (sut/take! t ::closed)
               t2 (sut/take! t ::closed)
               t3 (sut/take! t ::closed)
               fr1 frp1
               fr2 frp2]
        (is (identical? true fr1))
        (is (identical? true fr2))
        (is (= 1 t1))
        (is (= 2 t2))
        (is (= ::closed t3)))))
  (testing "unwraps wrapped values before sending to f"
    (let [t (sut/stream)
          f (sut/connect-via-error-fn
             inc
             t)
          frp0 (f (reify pt/IStreamValue
                    (-unwrap-value [_] 0)))]
      (pr/let [_ (sut/close! t)
               t0 (sut/take! t ::closed)
               t1 (sut/take! t ::closed)
               fr0 frp0]
        (is (identical? true fr0))
        (is (= 1 t0 ))
        (is (= ::closed t1)))))
  (testing "wraps wrappable results of f"
    ;; this doesn't test anything on clj/manifold, but it
    ;; does on cljs/core.async
    (let [t (sut/stream)
          f (sut/connect-via-error-fn
             (constantly nil)
             t)
          frp0 (f 0)]
      (pr/let [_ (sut/close! t)
               t0 (sut/take! t ::closed)
               t1 (sut/take! t ::closed)
               fr0 frp0]
        (is (identical? true fr0))
        (is (= nil t0 ))
        (is (= ::closed t1)))))
  (testing "catches errors in f, error!s the sink, returns false"
    (let [t (sut/stream)
          f (sut/connect-via-error-fn
             (fn [_] (throw (ex-info "boo" {})))
             t)
          frp0 (f 0)]
      (pr/let [[k0 v0] (-> (sut/take! t ::closed)
                         (pr/chain (fn [v] [::ok v]))
                         (pr/catch (fn [err] [::error err])))
               t1 (sut/take! t ::closed)
               fr0 frp0]
        (is (identical? false fr0))
        (is (= ::error k0))
        (is (= "boo" (ex-message v0)))
        (is (= ::closed t1))))))

(deftest connect-via-test
  (testing "connects source to sink via f"
    (let [s (sut/stream)
          t (sut/stream)
          psrp (pr/chain
                (sut/put-all! s [1 2 3])
                (fn [_] (sut/close! s)))
          cvrp (sut/connect-via s inc t)]
      (pr/let [t0 (sut/take! t)
               t1 (sut/take! t)
               t2 (sut/take! t)
               t3 (sut/take! t ::closed)
               psr psrp
               cvr cvrp]
        (is (= 2 t0))
        (is (= 3 t1))
        (is (= 4 t2))
        (is (= ::closed t3))
        (is (true? psr))
        (is (true? cvr)))))
  (testing "error!s the sink when f throws")
  (testing "deals with wrappable values"))

(ns prpr.stream.repeat-source-test
  (:require
   [clojure.test :refer [deftest is are testing use-fixtures]]
   [manifold.stream :as m.stream]
   [prpr.promise :as prpr]
   [prpr.stream :as stream]
   [prpr.stream.repeat-source :as sut]
   [schema.test :refer [validate-schemas]])
  (:import
   (clojure.lang ExceptionInfo)))

(use-fixtures :once validate-schemas)

(deftest create-repeat-source-custom-halt-test
  (let [custom-halt ::halt
        count (atom 0)
        src (sut/create-repeat-source
             (fn []
               (prpr/ddo
                [:let [n (swap! count inc)
                       false-halt? (= 5 n)
                       finished? (< 10 n)]]
                (prpr/return
                 (cond
                   false-halt? sut/halt!
                   finished? custom-halt
                   :else n))))
             {:halt-val custom-halt})
        r @(m.stream/reduce conj [] src)]
    (is (= [1 2 3 4 sut/halt! 6 7 8 9 10] r))))

(deftest create-repeat-source-timeout-test
  (testing "non-pass thru"
    (let [timeout-ms 10
          timeout-val ::timeout
          count (atom 0)
          src (sut/create-repeat-source
               (fn []
                 (prpr/delay-pr
                  (* 2 timeout-ms)
                  (prpr/ddo
                   [:let [n (swap! count inc)
                          finished? (< 10 n)]]
                   (prpr/return
                    (if finished? sut/halt! n))))))
          r @(m.stream/try-take! src nil 10 timeout-val)]
      (is (= timeout-val r))))
  (testing "pass thru"
    (let [timeout-ms 10
          timeout-val ::timeout
          src (sut/create-repeat-source
               (fn
                 ([tms tval]
                  [tms tval])
                 ([]
                  (throw (ex-info "💥" {:error :no-timeout})))))
          r @(m.stream/try-take! src nil 10 timeout-val)]
      (is (= [timeout-ms timeout-val] r))))
  (testing "misbehaving pass thru"
    (let [timeout-ms 10
          timeout-val ::timeout
          count (atom 0)
          src (sut/create-repeat-source
               (fn repeat-fn
                 ([tms _]
                  (prpr/delay-pr
                   (* 10 tms)
                   (repeat-fn)))
                 ([]
                  (prpr/ddo
                   [:let [n (swap! count inc)
                          finished? (< 10 n)]]
                   (prpr/return
                    (if finished? sut/halt! n))))))
          r @(m.stream/try-take! src nil 10 timeout-val)]
      (is (= timeout-val r)))))

(deftest repeatedly-source-test
  (let [count (atom 0)
        src (sut/repeatedly-source
             10
             (fn []
               (prpr/ddo
                [:let [n (swap! count inc)
                       too-far? (< 10 n)]]
                (when too-far?
                  (throw (ex-info "💥" {:n n})))
                (prpr/return n))))
        r @(m.stream/reduce conj [] src)]
    (is (= [1 2 3 4 5 6 7 8 9 10] r))))

(deftest iterate-source-test
  (let [src (sut/iterate-source
             (fn [n]
               (prpr/ddo
                [:let [finished? (<= 10 n)]]
                (prpr/return
                 (if finished?
                   sut/halt!
                   (inc n)))))
             0)
        r @(m.stream/reduce conj [] src)]
    (is (= [1 2 3 4 5 6 7 8 9 10] r))))

;;
;; error handling tests
;;

(defn replace-errors
  ([xs]
   (replace-errors ::stream-error xs))
  ([replace-val xs]
   (map
    (fn [x]
      (if (stream/stream-error? x)
        replace-val
        x))
    xs)))

(deftest repeat-source-initial-error-test
  (let [src (sut/iterate-source
             (fn [n]
               (throw (ex-info "💥" {:n n})))
             0)
        r @(m.stream/reduce conj [] src)]
    (is (= [::stream-error]
           (replace-errors r)))))

(deftest repeat-source-midstream-error-test
  (let [src (sut/iterate-source
             (fn [n]
               (when (<= 5 n)
                 (throw (ex-info "💥" {:n n})))
               (if (< 10 n)
                 sut/halt!
                 (inc n)))
             0)
        r @(m.stream/reduce conj [] src)]
    (is (= [1 2 3 4 5 ::stream-error]
           (replace-errors r)))))

(deftest repeat-source-midstream-promise-error-test
  (let [src (sut/iterate-source
             (fn [n]
               (prpr/ddo
                [:let [too-far? (< 10 n)
                       r (if too-far? sut/halt! (inc n))]]
                (when (<= 5 n)
                  (throw (ex-info "💥" {:n n})))
                (prpr/return r)))
             0)
        r @(m.stream/reduce conj [] src)]
    (is (= [1 2 3 4 5 ::stream-error]
           (replace-errors r)))))

(deftest create-repeat-source-dont-halt-on-error-test
  (let [count (atom 0)
        src (sut/create-repeat-source
             (fn []
               (prpr/ddo
                [:let [n (swap! count inc)
                       throw? (= 5 n)
                       finished? (< 10 n)]]
                (when throw?
                  (throw (ex-info "💥" {:n n})))
                (prpr/return
                 (if finished? sut/halt! n))))
             {:halt-on-error? false})
        r @(m.stream/reduce conj [] src)]
    (is (= [1 2 3 4 ::stream-error 6 7 8 9 10] (replace-errors r)))))


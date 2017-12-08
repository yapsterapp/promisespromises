(ns prpr.promise-test
  (:require
   [clojure.test :as t :refer [deftest testing is are use-fixtures]]
   [prpr.util.test :refer [with-log-level]]
   [cats.core :refer [return]]
   [manifold.deferred :as d]
   [prpr.promise :as sut]))

(deftest promise?-test
  (is (sut/promise? (d/success-deferred 10)))
  (is (not (sut/promise? :foo))))

(deftest ddo-test
  (let [pr (sut/ddo [a (d/success-deferred 10)
                     b (d/success-deferred 20)]
             (return (+ a b)))]
    (is (d/deferred? pr))
    (is (= 30 @pr))))

(deftest exception?-test
  (is (sut/exception? (RuntimeException. "foo")))
  (is (not (sut/exception? :foo))))

(deftest error-ex-test
  (let [ex (sut/error-ex [:foo :bar])
        ex2 (sut/error-ex :foo :bar)]
    (is (= {:tag :foo :value :bar} (ex-data ex)))
    (is (= {:tag :foo :value :bar} (ex-data ex2)))))

(deftest error-pr-test
  (let [pr (sut/error-pr [:foo :bar])
        pr2 (sut/error-pr :foo :bar)]
    (is (= {:tag :foo :value :bar}
           (ex-data (d/error-value pr nil))))
    (is (= {:tag :foo :value :bar}
           (ex-data (d/error-value pr2 nil))))))

(deftest decode-error-value-test
  (is (= [:foo :bar] (sut/decode-error-value [:foo :bar])))
  (is (= [:foo :bar] (sut/decode-error-value {:tag :foo :value :bar})))
  (is (= [:foo :bar] (sut/decode-error-value (sut/error-ex :foo :bar))))
  (is (= ::sut/unknown-error
         (first (sut/decode-error-value (ex-info "foo" {})))))
  (is (instance? Throwable
                 (second (sut/decode-error-value (ex-info "foo" {}))))))

(deftest catch-error-test
  ;; test exceptions in the promise init are caught
  (is (= [:foo :bar]
         @(sut/catch-error (throw (sut/error-ex :foo :bar)))))
  (let [[k v] @(sut/catch-error (throw (ex-info "boo" {:boo true})))]
    (is (= ::sut/unknown-error k))
    (is (= {:boo true} (ex-data v))))

  ;; test errored promises are caught
  (is (= [:ok] @(sut/catch-error (d/success-deferred [:ok]))))
  (is (= [:foo :bar] @(sut/catch-error (sut/error-pr :foo :bar))))
  (is (= [:foo :bar] @(sut/catch-error
                       (d/chain
                        (d/success-deferred true)
                        (fn [_] (throw (sut/error-ex :foo :bar))))))))

(deftest catch-error-log-test
  ;; test exceptions in the promise init are caught
  (with-log-level :error
    (is (= [:foo :bar]
           @(sut/catch-error-log "test" (throw (sut/error-ex :foo :bar)))))
    (let [[k v] @(sut/catch-error-log "test" (throw (ex-info "boo" {:boo true})))]
      (is (= ::sut/unknown-error k))
      (is (= {:boo true} (ex-data v))))

    ;; test errored promises are caught
    (is (= [:ok] @(sut/catch-error-log "test" (d/success-deferred [:ok]))))
    (is (= [:foo :bar] @(sut/catch-error-log "test" (sut/error-pr :foo :bar))))
    (is (= [:foo :bar] @(sut/catch-error-log
                         "test"
                         (d/chain
                          (d/success-deferred true)
                          (fn [_] (throw (sut/error-ex :foo :bar)))))))))

(deftest catch-xform-error-test
  ;; test exceptions in the promise init are caught
  (is (= [:foo :bar :baz]
         @(sut/catch-xform-error #(into % [:baz])
                                 (throw (sut/error-ex :foo :bar)))))
  (let [[k v z] @(sut/catch-xform-error
                  #(into % [:baz])
                  (throw (ex-info "boo" {:boo true})))]
    (is (= ::sut/unknown-error k))
    (is (= {:boo true} (ex-data v)))
    (is (= :baz z)))

  ;; test errored promises are caught
  (is (= [:ok]
         @(sut/catch-xform-error #(into % [:baz])
                                 (d/success-deferred [:ok]))))
  (is (= [:foo :bar :baz]
         @(sut/catch-xform-error #(into % [:baz])
                                 (sut/error-pr :foo :bar)))))

(deftest handle-test
  (is (= 3 (sut/handle {:foo inc} [:foo 2])))
  (let [x (try (sut/handle {:foo inc} [:bar 2])
               (catch Exception e e))]
    (is (instance? Exception x))
    (is (= {:tag ::sut/unhandled-tag
            :value [:bar 2]}
           (ex-data x)))))

(deftest handle-safe
  (is (= 3 (sut/handle-safe {:foo inc} :default [:foo 2])))
  (is (= [:foobar 100]
         (sut/handle-safe {:foo inc} [:foobar 100] [:bar 2])))
  (is (= [:bar 3]
         (sut/handle-safe {:foo inc} (fn [[t v]]
                                       [t (inc v)]) [:bar 2]))))

(deftest catch-handle
  (is (= 3 @(sut/catch-handle
             {:foo inc}
             (d/success-deferred [:foo 2]))))
  (let [x (try @(sut/catch-handle
                 {:foo inc}
                 (d/success-deferred [:bar 2]))
               (catch Exception e e))]
    (is (instance? Exception x))
    (is (= {:tag ::sut/unhandled-tag
            :value [:bar 2]}
           (ex-data x)))))

(deftest catch-handle-safe
  (is (= 3 @(sut/catch-handle-safe
             {:foo inc}
             :default
             (d/success-deferred [:foo 2]))))
  (is (= [:foobar 100]
         @(sut/catch-handle-safe
           {:foo inc}
           [:foobar 100]
           (d/success-deferred [:bar 2]))))
  (is (= [:bar 3]
         @(sut/catch-handle-safe
           {:foo inc}
           (fn [[t v]] [t (inc v)])
           (d/success-deferred [:bar 2])))))

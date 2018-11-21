(ns prpr.promise-cljs-test
  (:require
   [cljs.test :as t :refer-macros [deftest is testing run-tests async]]
   [prpr.promise :as sut :include-macros true]
   [cats.core :refer [return]]
   [promesa.core :as p]))

(deftest promise?-test
  (is (sut/promise? (p/promise 10)))
  (is (not (sut/promise? :foo))))

(deftest ddo-test
  (let [pr (sut/ddo [a (p/promise 10)
                     b (p/promise 20)]
             (return (+ a b)))]
    (is (p/promise? pr))
    (async done
           (p/then pr (fn [v] (is (= 30 v)) (done))))))

(deftest exception?-test
  (is (sut/exception? (js/Error. "foo")))
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
           (ex-data (p/extract pr))))
    (is (= {:tag :foo :value :bar}
           (ex-data (p/extract pr2))))))

(deftest decode-error-value-test
  (is (= [:foo :bar] (sut/decode-error-value [:foo :bar])))
  (is (= [:foo :bar] (sut/decode-error-value {:tag :foo :value :bar})))
  (is (= [:foo :bar] (sut/decode-error-value (sut/error-ex :foo :bar))))
  (let [[tag value] (sut/decode-error-value (ex-info "foo" {}))]
    (is (= ::sut/unknown-error tag))
    (is (string? (:error value)))))

(deftest catch-error-test-init-variant
  (async done
         (p/then (sut/catch-error (throw (sut/error-ex :foo :bar)))
                 (fn [v] (is (= [:foo :bar] v)) (done)))))

(deftest catch-error-test-init-unkown
  (async done
         (p/then (sut/catch-error (throw (ex-info "boo" {:boo true})))
                 (fn [[k v]]
                   (is (= k ::sut/unknown-error))
                   (is (string? (:error v)))
                   (done)))))

(deftest catch-error-test-normal
  (async done
         (p/then (sut/catch-error (p/promise [:ok]))
                 (fn [v] (is (= [:ok] v)) (done)))))

(deftest catch-error-test-variant-error
  (async done
         (p/then (sut/catch-error (sut/error-pr :foo :bar))
                 (fn [v] (is (= [:foo :bar] v)) (done)))))

(deftest catch-error-test-thrown-variant
  (async done
         (p/then (sut/catch-error
                  (p/then
                   (p/promise true)
                   (fn [_] (throw (sut/error-ex :foo :bar)))))
                 (fn [v] (is (= [:foo :bar] v)) (done)))))

(deftest catch-xform-error-test-init-variant
  (async done
         (p/then (sut/catch-xform-error #(into % [:baz])
                                        (throw (sut/error-ex :foo :bar)))
                 (fn [v] (is (= [:foo :bar :baz] v)) (done)))))

(deftest catch-xform-error-test-init-unknown
  (async done
         (p/then (sut/catch-xform-error #(into % [:baz])
                                        (throw (ex-info "boo" {:boo true})))
                 (fn [[k v]]
                   (is (= k ::sut/unknown-error))
                   (is (string? (:error v)))
                   (done)))))

(deftest catch-xform-error-test-normal
  (async done
         (p/then (sut/catch-xform-error #(into % [:baz])
                                        (p/promise [:ok]))
                 (fn [v] (is (= [:ok] v)) (done)))))

(deftest catch-xform-error-test-variant
  (async done
         (p/then (sut/catch-xform-error #(into % [:baz])
                                        (sut/error-pr :foo :bar))
                 (fn [v] (is (= [:foo :bar :baz] v)) (done)))))

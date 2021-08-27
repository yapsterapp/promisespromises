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
  (testing "vector variant"
    (let [ex (sut/error-ex [:foo :bar])]
      (is (= ":foo" (.getMessage ex)))
      (is (= {:tag :foo :value :bar} (ex-data ex)))))
  (testing "map variant"
    (let [ex (sut/error-ex {:tag :foo :value :bar})]
      (is (= ":foo" (.getMessage ex)))
      (is (= {:tag :foo :value :bar} (ex-data ex)))))
  (testing "tag and value"
    (let [ex (sut/error-ex :foo :bar)]
      (is (= ":foo" (.getMessage ex)))
      (is (= {:tag :foo :value :bar} (ex-data ex)))))
  (testing "just a tag"
    (let [ex (sut/error-ex :blargh)]
      (is (= ":blargh" (.getMessage ex)))
      (is (= {:tag :blargh
              :value nil} (ex-data ex)))))
  (testing "anything else"
    (let [ex (sut/error-ex 100)]
      (is (= ":prpr.promise/unknown-error" (.getMessage ex)))
      (is (= {:tag ::sut/unknown-error
              :value 100} (ex-data ex))))
    (let [ex (sut/error-ex nil)]
      (is (= ":prpr.promise/unknown-error" (.getMessage ex)))
      (is (= {:tag ::sut/unknown-error
              :value nil} (ex-data ex))))))

(deftest error-pr-test
  (let [pr (sut/error-pr [:foo :bar])
        pr2 (sut/error-pr :foo :bar)
        pr3 (sut/error-pr (sut/error-ex :foo :bar))]
    (is (= {:tag :foo :value :bar}
           (ex-data (d/error-value pr nil))))
    (is (= {:tag :foo :value :bar}
           (ex-data (d/error-value pr2 nil))))
    (is (= {:tag :foo :value :bar}
           (ex-data (d/error-value pr3 nil))))))

(deftest decode-error-value-test
  (is (= [:foo :bar] (sut/decode-error-value [:foo :bar])))
  (is (= [:foo :bar] (sut/decode-error-value {:tag :foo :value :bar})))
  (is (= [:foo :bar] (sut/decode-error-value (sut/error-ex :foo :bar))))
  (let [[tag value :as variant] (sut/decode-error-value (ex-info "foo" {}))]
    (is (= ::sut/unknown-error tag))
    (is (string? (:error value)))
    (is (re-matches #"(?s)^#error .*" (:error value)))
    (is (instance? Throwable (sut/error-variant-exception variant)))))

(deftest finally-test
  (testing "calls the callback when successful"
    (let [a (atom 0)
          r @(sut/finally
               (sut/success-pr :foo)
               (fn [] (swap! a inc)))]
      (is (= 1 @a ))
      (is (= :foo r))))
  (testing "calls the callback when successful in a thread"
    (let [a (atom 0)
          r @(-> (sut/success-pr 1)
                 (sut/chain-pr inc)
                 (sut/chain-pr inc)
                 (sut/finally (fn [] (swap! a inc))))]
      (is (= 1 @a))
      (is (= 3 r))))
  (testing "calls the callback when errored"
    (let [a (atom 0)
          r (try
              @(sut/finally
                 (sut/error-pr :foo :bar)
                 (fn [] (swap! a inc)))
              :nope
              (catch Exception x
                x))]
      (is (= 1 @a))
      (is (= [:foo :bar]
             (sut/decode-error-value r)))))
  (testing "calls the callback when errored in a thread"
    (let [a (atom 0)
          r (try
              @(-> (sut/success-pr 1)
                   (sut/chain-pr inc)
                   (sut/chain-pr inc)
                   (sut/chain-pr (fn [n] (throw (sut/error-ex :foo n))))
                   (sut/finally (fn [] (swap! a inc))))
              :nope
              (catch Exception x
                x))]
      (is (= 1 @a))
      (is (= [:foo 3]
             (sut/decode-error-value r)))))
  (testing "raison d'être - calls the callback when initial promise construction errors"
    (let [a (atom 0)
          r (try
              @(-> (throw (sut/error-ex :foo :bar))
                   (sut/finally (fn [] (swap! a inc))))
              :nope
              (catch Exception x
                x))]
      (is (= 1 @a))
      (is (= [:foo :bar]
             (sut/decode-error-value r))))))

(deftest catchall-test
  (testing "returns successful promises"
    (is (= :foo
           @(sut/catchall
             (sut/success-pr :foo)
             (constantly :nope)))))
  (testing "catches errored promises"
    (is (= [:foo :bar]
           @(sut/catchall
             (sut/error-pr :foo :bar)
             sut/decode-error-value))))
  (testing "catches errored promises in a thread"
    (is (= [:foo :bar]
           @(-> (sut/error-pr :foo :bar)
                (sut/catchall sut/decode-error-value)))))
  (testing "raison d'être - catches errors during initial promise construction"
    (is (= [:foo :bar]
           @(sut/catchall
             (throw (sut/error-ex :foo :bar))
             sut/decode-error-value))))
  (testing "raison d'être - catches errors during initial promise construction in a thread"
    (is (= [:foo :bar]
           @(-> (throw (sut/error-ex :foo :bar))
                (sut/catchall sut/decode-error-value)))))
  (testing "catches errors in threaded chains"
    (is (= [:foo 3]
           @(-> (sut/success-pr 1)
                (sut/chain-pr inc)
                (sut/chain-pr inc)
                (sut/chain-pr (fn [n] (throw (sut/error-ex :foo n))))
                (sut/chain-pr inc)
                (sut/catchall sut/decode-error-value))))))

(deftest handle-tag-test
  (testing "rethrows an unhandled exception"
    (is (thrown-with-msg?
         Exception
         #"blah"
         (sut/handle-tag nil (ex-info "blah" {})))))
  (testing "handles an exception"
    (is (= [::caught {:foo 100}]
           (sut/handle-tag
            {::bloop (fn [err]
                       (let [[_tag val] (sut/decode-error-value err)]
                         [::caught val]))}
            (sut/error-ex ::bloop {:foo 100}))))))

(deftest catch-tag-test
  (testing "does not catch an unhandled exception"
    (is (thrown-with-msg?
         Exception
         #"boo!"
         @(sut/catch-tag
           (d/error-deferred
            (ex-info "boo!" {}))
           {}))))
  (testing "handles an exception"
    (is (= {:foo 100}
           @(sut/catch-tag
             (d/error-deferred (sut/error-ex ::bloop {:foo 100}))
             {::bloop (comp second sut/decode-error-value)})))))

(deftest catchall-variant-test
  (testing "returns successes wrapped in :ok"
    (is (= [:ok :foo]
           @(sut/catchall-variant
             (sut/success-pr :foo)))))
  (testing "returns errors decoded"
    (is (= [:foo 2]
           @(-> (sut/success-pr 1)
                (sut/chain-pr inc)
                (sut/chain-pr (fn [n] (throw (sut/error-ex :foo n))))
                (sut/chain-pr inc)
                (sut/catchall-variant))))))

(deftest catchall-rethrowable-test
  (testing "returns successes wrapped in :ok"
    (is (= [:ok :foo]
           @(sut/catchall-variant
             (sut/success-pr :foo)))))
  (testing "returns error values unchanged"
    (testing "when the error value is an exception"
      (let [x (sut/error-ex :foo :bar)]
        (identical?
         [:error x]
         @(-> (sut/success-pr 1)
              (sut/chain-pr inc)
              (sut/chain-pr (fn [n] (throw x)))
              (sut/catchall-rethrowable)))))
    (testing "when the error value is not an exception"
      (let [x ::foo]
        (identical?
         [:error x]
         @(-> (sut/success-pr 1)
              (sut/chain-pr inc)
              (sut/chain-pr (fn [_] (prpr.promise.platform/pr-error x)))
              (sut/catchall-rethrowable)))))))

(deftest return-or-rethrow-test
  (testing "returns successes plain"
    (is (= :foo
           @(sut/return-or-rethrow [:ok :foo]))))
  (testing "passes exceptions unchanged"
    (let [x (sut/error-ex :foo :bar)
          caught-x (try
                     @(sut/return-or-rethrow [:error x])
                     (catch Exception x*
                       x*))]
      (is (identical? x caught-x)))))

(deftest catch-error-test
  ;; test exceptions in the promise init are caught
  (is (= [:foo :bar]
         @(sut/catch-error (throw (sut/error-ex :foo :bar)))))
  (let [[k v] @(sut/catch-error (throw (ex-info "boo" {:boo true})))]
    (is (= ::sut/unknown-error k))
    (is (string? (:error v)))
    (is (re-matches #"(?s)^#error .*" (:error v))))

  ;; test errored promises are caught
  (is (= [:ok] @(sut/catch-error (d/success-deferred [:ok]))))
  (is (= [:foo :bar] @(sut/catch-error (sut/error-pr :foo :bar))))
  (is (= [:foo :bar] @(sut/catch-error
                       (d/chain
                        (d/success-deferred true)
                        (fn [_] (throw (sut/error-ex :foo :bar))))))))

(deftest wrap-catch-error-test
  ;; test exceptions in the promise init are caught
  (is (= [:foo :bar]
         @(sut/wrap-catch-error (throw (sut/error-ex :foo :bar)))))
  (let [[k v] @(sut/wrap-catch-error (throw (ex-info "boo" {:boo true})))]
    (is (= ::sut/unknown-error k))
    (is (string? (:error v)))
    (is (re-matches #"(?s)^#error .*" (:error v))))

  ;; test errored promises are caught
  (is (= [:ok :val] @(sut/wrap-catch-error (d/success-deferred :val))))
  (is (= [:success-tag :val] @(sut/wrap-catch-error :success-tag (d/success-deferred :val))))
  (is (= [:foo :bar] @(sut/wrap-catch-error (sut/error-pr :foo :bar))))
  (is (= [:foo :bar] @(sut/wrap-catch-error
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
      (is (string? (:error v)))
      (is (re-matches #"(?s)^#error .*" (:error v))))

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
    (is (string? (:error v)))
    (is (re-matches #"(?s)^#error .*" (:error v)))
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
  (with-log-level :error
    (is (= 3 (sut/handle-safe {:foo inc} :default [:foo 2])))
    (is (= [:foobar 100]
           (sut/handle-safe {:foo inc} [:foobar 100] [:bar 2])))
    (is (= [:bar 3]
           (sut/handle-safe {:foo inc} (fn [[t v]]
                                         [t (inc v)]) [:bar 2])))))

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
  (with-log-level :error
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
             (d/success-deferred [:bar 2]))))))

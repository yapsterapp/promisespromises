(ns prpr.nustream-test
  (:require-macros
   [prpr.test
    :refer [defprtest testing is]
    :rename {defprtest deftest}])
  (:require
   #?(:clj [prpr.test
            :refer [defprtest testing is]
            :rename {defprtest deftest}]
      :cljs [prpr.test])
   [prpr.nustream :as sut]))

(deftest close!-test
  (testing "closes a stream"
    (is (= true true)))
  (testing "no-op on an already closed stream"))

(deftest put!-test
  (testing "returns true and puts to an open stream")
  (testing "resolves to true when backpressure finally allows put!")
  (testing "returns false on a closed stream")
  (testing "supports putting nils on a stream")
  (testing "supports a timeout"))

(deftest error!-test
  (testing "returns false and puts an error marker on to an open stream")
  (testing "throws an exception and logs on a closed stream"))

(deftest put-all!-test
  (testing "returns true when all values are accepted")
  (testing "supports backpressure")
  (testing "returns false when some values are not accepted"))

(deftest throw-if-error-test
  (testing "does not throw for non stream-error values")
  (testing "throws with stream-error values"))

(deftest take!-test
  (testing "1-arity"
    (testing "returns a value from an open stream")
    (testing "parks until a value is available")
    (testing "returns nil from a closed stream")
    (testing "returns an error from an errored stream"))
  (testing "2-arity"
    (testing "returns a value from an open stream")
    (testing "parks until a value is available")
    (testing "supports taking nils from a stream")
    (testing "returns the default-value from a closed stream")
    (testing "returns an error from an errored stream"))
  (testing "4-arity"
    (testing "returns a value from an open stream")
    (testing "parks until a value is available")
    (testing "supports taking nils from a stream")
    (testing "returns the default-value from a closed stream")
    (testing "returns an error from an errored stream")))

(deftest connect-via-test
  (testing "sends all messages through the connect fn and cleans up")
  (testing "catches errors in the connect fn, errors the sink stream and cleans up")
  (testing "correctly propagates nil values"))

(deftest realize-each-test
  (testing "does nothing to non-promise values")
  (testing "realizes promise values")
  (testing "correctly propagates nil values"))

(deftest stream-error-capturing-stream-xform-test)

(deftest transform-test)

(deftest map-test
  (testing "maps a stream")
  (testing "maps multiple streams"
    (testing "maps multiple streams of the same size")
    (testing "terminates the output when any of the inputs terminates"))
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

(deftest reduce
  (testing "reduces a stream")
  (testing "returns reducing function errors")
  (testing "when receiving a nil wrapper sends nil to the reducing fn"))

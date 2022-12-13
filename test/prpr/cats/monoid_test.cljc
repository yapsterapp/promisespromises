(ns prpr.cats.monoid-test
  (:require
   [prpr.test :refer [deftest testing is]]
   [prpr.cats.monoid :as sut]))

(deftest test-monoid-<>
  (testing "add to empty"
    (is (= #{:foo} (sut/<> #{} [:foo])))
    (is (= '(:foo) (sut/<> '() [:foo])))
    (is (= [:foo] (sut/<> [] [:foo])))
    (is (= '(:foo) (sut/<> (repeatedly 0 (constantly :foo)) [:foo])))
    (is (= [:foo] (sut/<> nil [:foo])))
    (is (= #{:foo} (sut/<> nil #{:foo})))
    (is (= {:foo #{:foob} :bar [:barb] :baz '(:bazb)}
           (sut/<> {:foo #{} :bar [] :baz '()}
                   {:foo [:foob] :bar [:barb] :baz [:bazb]}))))
  (testing "add to non-empty"
    (is (= #{:foo :bar} (sut/<> #{:foo} [:bar])))
    (is (= '(:foo :bar) (sut/<> '(:foo) [:bar])))
    (is (= [:foo :bar] (sut/<> [:foo] [:bar])))
    (is (= '(:foo :bar) (sut/<> (repeatedly 1 (constantly :foo)) [:bar])))
    (is (= {:foo #{:fooa :foob} :bar [:bara :barb] :baz '(:baza :bazb)}
           (sut/<> {:foo #{:fooa} :bar [:bara] :baz '(:baza)}
                   {:foo [:foob] :bar [:barb] :baz [:bazb]})))))

(ns prpr.a-frame.cofx.resolve-arg-test
  (:require
   [clojure.test :as t :refer [deftest testing is]]
   [prpr.a-frame.schema :as af.schema]
   [prpr.a-frame.cofx.resolve-arg.protocols :refer [->CofxPath]]
   [prpr.a-frame.cofx.resolve-arg :as sut]))

(deftest coeffect-path-reader-test
  (is (= (->CofxPath [:foo])
         #cofx/path [:foo]))
  (is (= (->CofxPath [:foo])
         #cofx/p [:foo])))

(deftest event-path-reader-test
  (is (= (->CofxPath [af.schema/a-frame-coeffect-event :foo])
         #cofx/event-path [:foo]))
  (is (= (->CofxPath [af.schema/a-frame-coeffect-event :foo])
         #cofx/evp [:foo])))

(deftest resolve-cofx-arg-test
  (testing "literal values"
    (is (= {:foo 10 :bar [10 20]}
           (sut/resolve-cofx-arg
            {:foo 10 :bar [10 20]}
            {})))
    (is (= [:a 2 :foo]
           (sut/resolve-cofx-arg
            [:a 2 :foo]
            {}))))
  (testing "coeffects path"
    (is (= 100
           (sut/resolve-cofx-arg
            #cofx/path [:foo :bar]
            {:foo {:bar 100}}))))
  (testing "event path"
    (is (= 100
           (sut/resolve-cofx-arg
            #cofx/event-path [1 ::foofoo]
            {af.schema/a-frame-coeffect-event [::foo {::foofoo 100}]}))))
  (testing "mixed literals and paths"
    (is (= {:some-key ["foo" 200]
            :other-key {:foo 100
                        :bar "bar"}}

           (sut/resolve-cofx-arg
            {:some-key [#cofx/path [:a]
                        #cofx/path [:b]]
             :other-key {:foo #cofx/event-path [1]
                         :bar #cofx/event-path [2 ::evdata]}}

            {:a "foo"
             :b 200

             af.schema/a-frame-coeffect-event
             [::ev 100 {::evdata "bar"}]})))))

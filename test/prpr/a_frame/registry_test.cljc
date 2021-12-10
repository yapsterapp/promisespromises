(ns prpr.a-frame.registry-test
  (:require
   #?(:clj [clojure.test :as t :refer [deftest testing is use-fixtures]]
      :cljs [cljs.test :as t :include-macros true :refer-macros [deftest testing is use-fixtures]])
   [prpr.a-frame.schema :as schema]
   [prpr.a-frame.registry :as sut]
   [prpr.a-frame.registry.test :as registry.test]))

(use-fixtures :each registry.test/reset-registry)

(deftest register-handler-test
  (sut/register-handler schema/a-frame-kind-event ::foo ::foo-handler)
  (is (= ::foo-handler
         (get-in
          @sut/registry
          [schema/a-frame-kind-event ::foo]))))

(deftest get-handler-test
  (sut/register-handler schema/a-frame-kind-event ::foo ::foo-handler)

  (is (= ::foo-handler
         (sut/get-handler schema/a-frame-kind-event ::foo))))

(deftest unregister-handler-test
  (testing "unregisters a single handler"
    (sut/register-handler schema/a-frame-kind-event ::foo ::foo-handler)
    (sut/register-handler schema/a-frame-kind-event ::bar ::bar-handler)
    (is (= ::foo-handler (sut/get-handler schema/a-frame-kind-event ::foo)))
    (is (= ::bar-handler (sut/get-handler schema/a-frame-kind-event ::bar)))
    (sut/unregister-handler schema/a-frame-kind-event ::foo)
    (is (= nil (sut/get-handler schema/a-frame-kind-event ::foo)))
    (is (= ::bar-handler (sut/get-handler schema/a-frame-kind-event ::bar))))
  (testing "unregisters all of a kind of handler"
    (sut/register-handler schema/a-frame-kind-event ::foo ::foo-handler)
    (sut/register-handler schema/a-frame-kind-event ::bar ::bar-handler)
    (is (= ::foo-handler (sut/get-handler schema/a-frame-kind-event ::foo)))
    (is (= ::bar-handler (sut/get-handler schema/a-frame-kind-event ::bar)))
    (sut/unregister-handler schema/a-frame-kind-event)
    (is (= nil (sut/get-handler schema/a-frame-kind-event ::foo)))
    (is (= nil (sut/get-handler schema/a-frame-kind-event ::bar)))))

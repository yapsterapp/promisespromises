(ns prpr.a-frame.events-test
  #?(:cljs (:require-macros [prpr.test :refer [deftest test-async is testing use-fixtures]]))
  (:require
   #?(:clj [prpr.test :refer [deftest test-async is testing use-fixtures]])
   [prpr.promise :as prpr :refer [ddo return]]
   [prpr.a-frame.schema :as schema]
   [prpr.a-frame.registry :as registry]
   [prpr.a-frame.registry.test :as registry.test]
   [prpr.a-frame.fx :as fx]
   [prpr.a-frame.cofx :as cofx]
   [prpr.a-frame.events :as sut]
   [prpr.a-frame.std-interceptors :as std-interceptors]))

(use-fixtures :each registry.test/reset-registry)

(deftest flatten-and-remove-nils-test
  (test-async
   (is (= [:foo :bar :baz :blah]
          (sut/flatten-and-remove-nils
           ::id
           [:foo
            nil
            [:bar nil :baz]
            nil
            [:blah]])))))

(deftest register-test
  (test-async
   (testing "registers an event handler"
     (sut/register ::foo [::foo-interceptor nil [::bar-interceptor]])

     (is (= [::foo-interceptor ::bar-interceptor]
            (registry/get-handler schema/a-frame-kind-event ::foo))))))

(deftest handle-test
  (test-async
   (testing "runs interceptor chain with co-fx, event-handler and fx"
     (ddo [:let [fx-a (atom {})

                 _ (fx/reg-fx ::fx-foo (fn [app data]
                                         (is (= ::app app))
                                         (swap! fx-a assoc ::fx-foo data)
                                         (prpr/return-pr data)))

                 _ (cofx/reg-cofx ::cofx-bar (fn [app
                                                 {[_ev-key ev-data] schema/a-frame-coeffect-event
                                                  :as coeffects}
                                                 cofx-data]
                                               (is (= ::app app))

                                               (prpr/return-pr
                                                (assoc
                                                 coeffects
                                                 ::cofx-bar
                                                 (+ ev-data cofx-data)))))

                 _ (sut/register
                    ::event-blah
                    [fx/do-fx
                     (cofx/inject-cofx ::cofx-bar 100)
                     (std-interceptors/fx-handler->interceptor
                      (fn [app coeffects event]
                        (is (= ::app app))
                        (is (= {schema/a-frame-coeffect-event event
                                ::cofx-bar 200}
                               coeffects))
                        (is (= [::event-blah 100] event))
                        {::fx-foo {::coeffects coeffects
                                   ::event event}}))])]

           {h-r-app-ctx schema/a-frame-app-ctx
            h-r-queue :interceptor/queue
            h-r-stack :interceptor/stack
            h-r-coeffects schema/a-frame-coeffects
            h-r-effects schema/a-frame-effects
            :as _h-r} (sut/handle ::app [::event-blah 100])]

       (is (= {::fx-foo {::coeffects
                         {schema/a-frame-coeffect-event [::event-blah 100]
                          ::cofx-bar 200}

                         ::event [::event-blah 100]}} @fx-a))


       (is (= ::app h-r-app-ctx))
       (is (= [] h-r-queue))
       (is (= '() h-r-stack))
       (is (= {schema/a-frame-coeffect-event [::event-blah 100]
               ::cofx-bar 200} h-r-coeffects))
       (is (= {::fx-foo {::coeffects
                         {schema/a-frame-coeffect-event [::event-blah 100]
                          ::cofx-bar 200}

                         ::event [::event-blah 100]}}
              @fx-a
              h-r-effects))))

   (testing "runs interceptor chain with context and coeffects from init-ctx"
     (ddo [:let [fx-a (atom {})

                 _ (fx/reg-fx ::fx-bar (fn [app data]
                                         (is (= ::app app))
                                         (swap! fx-a assoc ::fx-bar data)
                                         (prpr/return-pr data)))

                 _ (sut/register
                    ::handle-test-init-ctx
                    [fx/do-fx
                     (std-interceptors/fx-handler->interceptor
                      (fn [app coeffects event]
                        (is (= ::app app))
                        (is (= {schema/a-frame-coeffect-event event
                                ::cofx-init 550}
                               coeffects))
                        (is (= [::handle-test-init-ctx 100] event))
                        {::fx-bar {::coeffects coeffects
                                   ::event event}}))])]

           {h-r-ctx-val ::ctx-val
            h-r-app-ctx schema/a-frame-app-ctx
            h-r-queue :interceptor/queue
            h-r-stack :interceptor/stack
            h-r-coeffects schema/a-frame-coeffects
            h-r-effects schema/a-frame-effects
            :as _h-r} (sut/handle
                       {::ctx-val ::blahblah
                        schema/a-frame-coeffects {::cofx-init 550}}
                       ::app
                       [::handle-test-init-ctx 100])]

       (is (= ::blahblah h-r-ctx-val))
       (is (= ::app h-r-app-ctx))
       (is (= [] h-r-queue))
       (is (= '() h-r-stack))
       (is (= {schema/a-frame-coeffect-event [::handle-test-init-ctx 100]
               ::cofx-init 550}
              h-r-coeffects))
       (is (= {::fx-bar {::event [::handle-test-init-ctx 100]
                         ::coeffects {schema/a-frame-coeffect-event
                                      [::handle-test-init-ctx 100]

                                      ::cofx-init 550}}}
              @fx-a
              h-r-effects))))))

(ns prpr.a-frame.cofx-test
  #?(:cljs (:require-macros [prpr.test :refer [deftest test-async is testing use-fixtures]]))
  (:require
   #?(:clj [prpr.test :refer [deftest test-async is testing use-fixtures]])
   [prpr.promise :as prpr :refer [ddo]]
   [prpr.a-frame.schema :as schema]
   [prpr.a-frame.registry :as registry]
   [prpr.a-frame.registry.test :as registry.test]
   [prpr.interceptor-chain :as interceptor-chain]
   [prpr.a-frame.cofx :as sut]))

(use-fixtures :each registry.test/reset-registry)

(deftest reg-cofx-test
  (test-async
   (testing "registers an cofx handler"
     (let [cofx-key ::reg-cofx-test]
       (sut/reg-cofx cofx-key ::reg-cofx-test-handler)

       (is (= ::reg-cofx-test-handler
              (registry/get-handler schema/a-frame-kind-cofx cofx-key)))))))

(deftest inject-cofx-test
  (test-async
   (testing "0-arg cofx"
     (ddo [:let [cofx-key ::inject-cofx-test-0-arg
                 _ (sut/reg-cofx cofx-key (fn [app cofx]
                                            (is (= ::app app))
                                            (assoc cofx cofx-key 100)))

                 init-int-ctx {schema/a-frame-app-ctx ::app}]

           int-r (interceptor-chain/execute
                  [(sut/inject-cofx cofx-key)]
                  init-int-ctx)]

       (is (= (assoc
               init-int-ctx
               :interceptor/queue []
               :interceptor/stack '()
               schema/a-frame-coeffects {cofx-key 100})
              int-r))))

   (testing "1-arg cofx"
     (ddo [:let [cofx-key ::inject-cofx-test-1-arg
                 _ (sut/reg-cofx cofx-key (fn [app cofx arg]
                                            (is (= ::app app))
                                            (assoc cofx cofx-key arg)))

                 init-int-ctx {schema/a-frame-app-ctx ::app}]

           int-r (interceptor-chain/execute
                  [(sut/inject-cofx cofx-key 100)]
                  init-int-ctx)]

       (is (= (assoc
               init-int-ctx
               :interceptor/queue []
               :interceptor/stack '()
               schema/a-frame-coeffects {cofx-key 100})
              int-r))))

   (testing "1-arg cofx with resolver"
     (ddo [:let [static-cofx-key ::inject-cofx-1-arg-resolver-static
                 resolved-cofx-key ::inject-cofx-1-arg-resolver-resolved

                 _ (sut/reg-cofx
                    static-cofx-key
                    (fn [app cofx]
                      (is (= ::app app))
                      (assoc cofx static-cofx-key ::static-val)))

                 _ (sut/reg-cofx
                    resolved-cofx-key
                    (fn [app cofx {_a :a
                                  _b :b
                                  :as arg}]
                      (is (= ::app app))
                      (assoc cofx resolved-cofx-key arg)))

                 init-coeffects {schema/a-frame-coeffect-event
                                 [::foo {:event-data :val}]}

                 init-int-ctx {schema/a-frame-app-ctx ::app

                               schema/a-frame-coeffects init-coeffects}]

           int-r (interceptor-chain/execute
                  [(sut/inject-cofx static-cofx-key)
                   (sut/inject-cofx resolved-cofx-key {:a #cofx/path [::inject-cofx-1-arg-resolver-static]
                                                       :b #cofx/event-path [1]})]
                  init-int-ctx)]

       (is (= (assoc
               init-int-ctx
               :interceptor/queue []
               :interceptor/stack '()

               schema/a-frame-coeffects
               (merge
                init-coeffects
                {static-cofx-key ::static-val
                 resolved-cofx-key {:a ::static-val
                                    :b {:event-data :val}}}))

              int-r))))))

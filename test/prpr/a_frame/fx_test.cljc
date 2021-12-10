(ns prpr.a-frame.fx-test
  #?(:cljs (:require-macros [prpr.test :refer [deftest test-async is testing use-fixtures]]))
  (:require
   #?(:clj [prpr.test :refer [deftest test-async is testing use-fixtures]])
   [prpr.promise :as prpr :refer [ddo return]]
   #?(:clj [prpr.stream :as stream])
   [prpr.a-frame.schema :as schema]
   [prpr.a-frame.registry :as registry]
   [prpr.a-frame.registry.test :as registry.test]
   [prpr.interceptor-chain :as interceptor-chain]
   [prpr.a-frame.fx :as sut]
   [prpr.a-frame.router :as router]
   [prpr.a-frame.std-interceptors :as std-interceptors]))

(use-fixtures :each registry.test/reset-registry)

(deftest reg-fx-test
  (test-async
   (testing "registers an fx handler"
     (let [a (atom nil)
           _ (sut/reg-fx ::reg-fx-test (fn [app data]
                                         (reset! a {:app app
                                                    :data data})))
           h (registry/get-handler schema/a-frame-kind-fx ::reg-fx-test)]

       (h {schema/a-frame-app-ctx ::app} ::data)

       (is (= {:app ::app
               :data ::data}
              @a))))))

(deftest reg-fx-ctx-test
  (test-async
   (testing "registers a fx context handler"
     (sut/reg-fx-ctx ::reg-fx-ctx-test ::foo-handler)

     (is (= ::foo-handler
            (registry/get-handler schema/a-frame-kind-fx ::reg-fx-ctx-test))))))

(deftest do-single-effect-test
  (test-async
   (testing "calls a sync fx handler"
     (let [fx-key ::do-single-effect-test-sync
           r-a (atom nil)
           _ (sut/reg-fx fx-key (fn [app data]
                                  (is (= ::app app))
                                  (swap! r-a (constantly data))))
           _ (sut/do-single-effect
              {schema/a-frame-app-ctx ::app}
              fx-key
              ::foo-data)]

       (is (= ::foo-data @r-a))))

   (testing "calls an async fx handler"
     (ddo [:let [fx-key ::do-single-effect-test-async
                 r-a (atom nil)
                 trigger-pr (prpr/deferred-pr)
                 _ (sut/reg-fx fx-key (fn [app data]
                                        (is (= ::app app))
                                        (prpr/chain-pr
                                         trigger-pr
                                         (fn [_]
                                           (swap! r-a (constantly data))))))

                 ;; don't wait for the promise to be resolved
                 fx-r-pr (sut/do-single-effect
                          {schema/a-frame-app-ctx ::app}
                          fx-key
                          ::foo-data)

                 ;; trigger the fx chain
                 _ (prpr/resolve!-pr trigger-pr ::trigger)]

           ;; and wait for completion
           _ fx-r-pr]


       (is (= ::foo-data @r-a))))))

(deftest do-map-of-effects-test
  (test-async
   (testing "calls multiple fx handlers and returns a map of results"
     (ddo [:let [foo-a (atom 1)
                 bar-a (atom 9)
                 foo-fx-key ::do-map-of-effects-test-foo
                 _ (sut/reg-fx foo-fx-key (fn [app data]
                                            (is (= ::app app))
                                            (prpr/return-pr
                                             (swap! foo-a + data))))
                 bar-fx-key ::do-map-of-effects-test-bar
                 _ (sut/reg-fx bar-fx-key (fn [app data]
                                            (is (= ::app app))
                                            (swap! bar-a + data)))]

           fx-r (sut/do-map-of-effects
                 {schema/a-frame-app-ctx ::app}
                 {foo-fx-key 3
                  bar-fx-key 2})]

       (is (= 4 @foo-a))
       (is (= 11 @bar-a))
       (is (= {foo-fx-key 4
               bar-fx-key 11} fx-r))))))

(deftest do-seq-of-effects-test
  (test-async
   (testing "calls multiple fx handlers serially and returns a seq of results"
     (ddo [:let [r-a (atom [])

                 foo-fx-key ::do-seq-of-effects-test-foo
                 _ (sut/reg-fx foo-fx-key (fn [app data]
                                            (is (= ::app app))
                                            (prpr/return-pr
                                             (swap! r-a conj data))))
                 bar-fx-key ::do-seq-of-effects-test-bar
                 _ (sut/reg-fx bar-fx-key (fn [app data]
                                            (is (= ::app app))
                                            (swap! r-a conj data)))]

           fx-r (sut/do-seq-of-effects
                 {schema/a-frame-app-ctx ::app}
                 [{foo-fx-key ::foo-val}
                  {bar-fx-key ::bar-val}])]

       (is (= [::foo-val ::bar-val]
              @r-a))

       (is (= [{foo-fx-key [::foo-val]}
               {bar-fx-key [::foo-val ::bar-val]}]
              fx-r))))

   (testing "calls multiple maps of fx handlers with multiple fx"
     (ddo [:let [r-a (atom {})

                 foo-fx-key ::do-seq-of-effects-test-multiple-maps-foo
                 _ (sut/reg-fx foo-fx-key (fn [app data]
                                            (is (= ::app app))
                                            (swap! r-a assoc ::foo data)
                                            (prpr/return-pr data)))
                 bar-fx-key ::do-seq-of-effects-test-multiple-maps-bar
                 _ (sut/reg-fx bar-fx-key (fn [app data]
                                            (is (= ::app app))
                                            (swap! r-a assoc ::bar data)
                                            data))
                 blah-fx-key ::do-seq-of-effects-test-multiple-maps-blah
                 _ (sut/reg-fx blah-fx-key (fn [app data]
                                             (is (= ::app app))
                                             (swap! r-a assoc ::blah data)
                                             (prpr/return-pr data)))]

           ;; check that the fn works with a seq as well as a vector
           fx-r (sut/do-seq-of-effects
                 {schema/a-frame-app-ctx ::app}
                 (map
                  identity
                  [{foo-fx-key ::foo-val
                    bar-fx-key ::bar-val}
                   {blah-fx-key ::blah-val}]))]

       (is (= {::foo ::foo-val
               ::bar ::bar-val
               ::blah ::blah-val}
              @r-a))

       (is (= [{foo-fx-key ::foo-val
                bar-fx-key ::bar-val}
               {blah-fx-key ::blah-val}]
              fx-r))))))

(deftest do-fx-test
  (test-async
   (testing "calls a seq of maps of multiple fx handlers"
     (ddo [:let [r-a (atom {})

                 foo-fx-key ::do-fx-test-foo
                 _ (sut/reg-fx foo-fx-key (fn [app data]
                                            (is (= ::app app))
                                            (swap! r-a assoc ::foo data)
                                            (prpr/return-pr data)))
                 bar-fx-key ::do-fx-test-bar
                 _ (sut/reg-fx bar-fx-key (fn [app data]
                                            (is (= ::app app))
                                            (swap! r-a assoc ::bar data)
                                            data))
                 blah-fx-key ::do-fx-test-blah
                 _ (sut/reg-fx blah-fx-key (fn [app data]
                                             (is (= ::app app))
                                             (swap! r-a assoc ::blah data)
                                             (prpr/return-pr data)))

                 init-int-ctx {schema/a-frame-app-ctx ::app
                               schema/a-frame-effects [{foo-fx-key ::foo-val
                                                        bar-fx-key ::bar-val}
                                                       {blah-fx-key ::blah-val}]}]

           int-r (interceptor-chain/execute
                  [sut/do-fx]
                  init-int-ctx)]

       ;; performs the side-effects
       (is (= {::foo ::foo-val
               ::bar ::bar-val
               ::blah ::blah-val}
              @r-a))

       ;; leaves the context untouched
       (is (= (assoc
               init-int-ctx
               :interceptor/queue []
               :interceptor/stack '())
              int-r))))))

(def test-app-ctx {::FOO "foo"})

;; these fx tests are currently clj only because they use router/dispatch-sync
;; which has no cljs implementation yet

#?(:clj (deftest dispatch-fx-test
          (test-async
           (testing "dispatches without transitive coeffects"
             (let [{event-s schema/a-frame-router-event-stream
                    :as router} (router/create-router test-app-ctx {})
                   out-a (atom [])

                   _ (registry/register-handler
                      schema/a-frame-kind-event
                      ::dispatch-fx-test-without-transitive-coeffects
                      [sut/do-fx
                       (std-interceptors/fx-handler->interceptor
                        (fn [app cofx [_ n :as event-v]]
                          (is (= test-app-ctx app))
                          ;; only the first event should have the ::BAR coeffect
                          (if (= 0 n)
                            (is (= {schema/a-frame-coeffect-event event-v
                                    ::BAR "bar"} cofx))
                            (is (= {schema/a-frame-coeffect-event event-v} cofx)))

                          (swap! out-a conj n)

                          (when (<= n 3)

                            {:a-frame/dispatch
                             [::dispatch-fx-test-without-transitive-coeffects (+ n 2)]})))])]

               @(router/dispatch-sync
                 router
                 {schema/a-frame-event [::dispatch-fx-test-without-transitive-coeffects 0]
                  schema/a-frame-coeffects {::BAR "bar"}})

               (is (= [0 2 4] @out-a))

               ;; main stream should not be closed
               (is (not (stream/closed? event-s)))))

           (testing "dispatches with transitive coeffects"
             (let [{event-s schema/a-frame-router-event-stream
                    :as router} (router/create-router test-app-ctx {})
                   out-a (atom [])

                   _ (registry/register-handler
                      schema/a-frame-kind-event
                      ::dispatch-fx-test-with-transitive-coeffects
                      [sut/do-fx
                       (std-interceptors/fx-handler->interceptor
                        (fn [app cofx [_ n :as event-v]]
                          (is (= test-app-ctx app))

                          ;; all events should have the ::BAR coeffect
                          (is (= {schema/a-frame-coeffect-event event-v
                                  ::BAR "bar"} cofx))

                          (swap! out-a conj n)

                          (when (<= n 3)

                            {:a-frame/dispatch
                             {schema/a-frame-event [::dispatch-fx-test-with-transitive-coeffects (+ n 2)]
                              schema/a-frame-event-transitive-coeffects? true}})))])]

               @(router/dispatch-sync
                 router
                 {schema/a-frame-event [::dispatch-fx-test-with-transitive-coeffects 0]
                  schema/a-frame-coeffects {::BAR "bar"}})

               (is (= [0 2 4] @out-a))

               ;; main stream should not be closed
               (is (not (stream/closed? event-s))))))))

#?(:clj (deftest dispatch-sync-fx-test
          (test-async
           (testing "dispatches without transitive coeffects"
             (let [{event-s schema/a-frame-router-event-stream
                    :as router} (router/create-router test-app-ctx {})
                   out-a (atom [])

                   _ (registry/register-handler
                      schema/a-frame-kind-event
                      ::dispatch-sync-fx-test-without-transitive-coeffects
                      [sut/do-fx
                       (std-interceptors/fx-handler->interceptor
                        (fn [app cofx [_ n :as event-v]]
                          (is (= test-app-ctx app))
                          ;; only the first event should have the ::BAR coeffect
                          (if (= 0 n)
                            (is (= {schema/a-frame-coeffect-event event-v
                                    ::BAR "bar"} cofx))
                            (is (= {schema/a-frame-coeffect-event event-v} cofx)))

                          (swap! out-a conj n)

                          (when (<= n 3)

                            {:a-frame/dispatch-sync
                             {schema/a-frame-event [::dispatch-sync-fx-test-without-transitive-coeffects (+ n 2)]
                              schema/a-frame-event-transitive-coeffects? false}})))])]

               @(router/dispatch-sync
                 router
                 {schema/a-frame-event [::dispatch-sync-fx-test-without-transitive-coeffects 0]
                  schema/a-frame-coeffects {::BAR "bar"}})

               (is (= [0 2 4] @out-a))

               ;; main stream should not be closed
               (is (not (stream/closed? event-s)))))

           (testing "dispatches with transitive coeffects"
             (let [{event-s schema/a-frame-router-event-stream
                    :as router} (router/create-router test-app-ctx {})
                   out-a (atom [])

                   _ (registry/register-handler
                      schema/a-frame-kind-event
                      ::dispatch-sync-fx-test-with-transitive-coeffects
                      [sut/do-fx
                       (std-interceptors/fx-handler->interceptor
                        (fn [app cofx [_ n :as event-v]]
                          (is (= test-app-ctx app))

                          ;; all events should have the ::BAR coeffect
                          (is (= {schema/a-frame-coeffect-event event-v
                                  ::BAR "bar"} cofx))

                          (swap! out-a conj n)

                          (when (<= n 3)

                            {:a-frame/dispatch-sync
                             {schema/a-frame-event [::dispatch-sync-fx-test-with-transitive-coeffects (+ n 2)]}})))])]

               @(router/dispatch-sync
                 router
                 {schema/a-frame-event [::dispatch-sync-fx-test-with-transitive-coeffects 0]
                  schema/a-frame-coeffects {::BAR "bar"}})

               (is (= [0 2 4] @out-a))

               ;; main stream should not be closed
               (is (not (stream/closed? event-s))))))))

#?(:clj (deftest dispatch-n-fx-test
          (test-async
           (testing "dispatches without transitive coeffects"
             (let [{event-s schema/a-frame-router-event-stream
                    :as router} (router/create-router test-app-ctx {})
                   out-a (atom [])

                   _ (registry/register-handler
                      schema/a-frame-kind-event
                      ::dispatch-n-fx-test-without-transitive-coeffects
                      [sut/do-fx
                       (std-interceptors/fx-handler->interceptor
                        (fn [app cofx [_ n :as event-v]]
                          (is (= test-app-ctx app))
                          ;; only the first event should have the ::BAR coeffect
                          (if (= 0 n)
                            (is (= {schema/a-frame-coeffect-event event-v
                                    ::BAR "bar"} cofx))
                            (is (= {schema/a-frame-coeffect-event event-v} cofx)))

                          (swap! out-a conj n)

                          (when (<= n 3)

                            {:a-frame/dispatch-n
                             [[::dispatch-n-fx-test-without-transitive-coeffects (+ n 2)]]})))])]

               @(router/dispatch-sync
                 router
                 {schema/a-frame-event [::dispatch-n-fx-test-without-transitive-coeffects 0]
                  schema/a-frame-coeffects {::BAR "bar"}})

               (is (= [0 2 4] @out-a))

               ;; main stream should not be closed
               (is (not (stream/closed? event-s)))))

           (testing "dispatches with transitive coeffects"
             (let [{event-s schema/a-frame-router-event-stream
                    :as router} (router/create-router test-app-ctx {})
                   out-a (atom [])

                   _ (registry/register-handler
                      schema/a-frame-kind-event
                      ::dispatch-n-fx-test-with-transitive-coeffects
                      [sut/do-fx
                       (std-interceptors/fx-handler->interceptor
                        (fn [app cofx [_ n :as event-v]]
                          (is (= test-app-ctx app))

                          ;; all events should have the ::BAR coeffect
                          (is (= {schema/a-frame-coeffect-event event-v
                                  ::BAR "bar"} cofx))

                          (swap! out-a conj n)

                          (when (<= n 3)

                            {:a-frame/dispatch-n
                             [{schema/a-frame-event [::dispatch-n-fx-test-with-transitive-coeffects (+ n 2)]
                               schema/a-frame-event-transitive-coeffects? true}]})))])]

               @(router/dispatch-sync
                 router
                 {schema/a-frame-event [::dispatch-n-fx-test-with-transitive-coeffects 0]
                  schema/a-frame-coeffects {::BAR "bar"}})

               (is (= [0 2 4] @out-a))

               ;; main stream should not be closed
               (is (not (stream/closed? event-s))))))))

#?(:clj (deftest dispatch-n-sync-fx-test
          (test-async
           (testing "dispatches without transitive coeffects"
             (let [{event-s schema/a-frame-router-event-stream
                    :as router} (router/create-router test-app-ctx {})
                   out-a (atom [])

                   _ (registry/register-handler
                      schema/a-frame-kind-event
                      ::dispatch-n-sync-fx-test-without-transitive-coeffects
                      [sut/do-fx
                       (std-interceptors/fx-handler->interceptor
                        (fn [app cofx [_ n :as event-v]]
                          (is (= test-app-ctx app))
                          ;; only the first event should have the ::BAR coeffect
                          (if (= 0 n)
                            (is (= {schema/a-frame-coeffect-event event-v
                                    ::BAR "bar"} cofx))
                            (is (= {schema/a-frame-coeffect-event event-v} cofx)))

                          (swap! out-a conj n)

                          (when (<= n 3)

                            {:a-frame/dispatch-n-sync
                             [{schema/a-frame-event [::dispatch-n-sync-fx-test-without-transitive-coeffects (+ n 2)]
                               schema/a-frame-event-transitive-coeffects? false}]})))])]

               @(router/dispatch-sync
                 router
                 {schema/a-frame-event [::dispatch-n-sync-fx-test-without-transitive-coeffects 0]
                  schema/a-frame-coeffects {::BAR "bar"}})

               (is (= [0 2 4] @out-a))

               ;; main stream should not be closed
               (is (not (stream/closed? event-s)))))

           (testing "dispatches with transitive coeffects"
             (let [{event-s schema/a-frame-router-event-stream
                    :as router} (router/create-router test-app-ctx {})
                   out-a (atom [])

                   _ (registry/register-handler
                      schema/a-frame-kind-event
                      ::dispatch-n-sync-fx-test-with-transitive-coeffects
                      [sut/do-fx
                       (std-interceptors/fx-handler->interceptor
                        (fn [app cofx [_ n :as event-v]]
                          (is (= test-app-ctx app))

                          ;; all events should have the ::BAR coeffect
                          (is (= {schema/a-frame-coeffect-event event-v
                                  ::BAR "bar"} cofx))

                          (swap! out-a conj n)

                          (when (<= n 3)

                            {:a-frame/dispatch-n-sync
                             [{schema/a-frame-event [::dispatch-n-sync-fx-test-with-transitive-coeffects (+ n 2)]}]})))])]

               @(router/dispatch-sync
                 router
                 {schema/a-frame-event [::dispatch-n-sync-fx-test-with-transitive-coeffects 0]
                  schema/a-frame-coeffects {::BAR "bar"}})

               (is (= [0 2 4] @out-a))

               ;; main stream should not be closed
               (is (not (stream/closed? event-s))))))))

(ns prpr.a-frame.router-test
  (:require
   [clojure.test :refer [deftest testing is use-fixtures compose-fixtures]]
   [schema.test :refer [validate-schemas]]
   [prpr.util.test :refer [with-log-level with-log-level-fixture]]
   [prpr.promise :as prpr]
   [prpr.stream :as stream]
   [prpr.a-frame.schema :as schema]
   [prpr.a-frame.registry :as registry]
   [prpr.a-frame.registry.test :as registry.test]
   [prpr.a-frame.interceptor-chain :as interceptor-chain]
   [prpr.a-frame.std-interceptors :as std-interceptors]
   [prpr.a-frame.events :as events]
   [prpr.a-frame.fx :as fx]
   [prpr.a-frame.router :as sut]
   [taoensso.timbre :refer [error]]))

(use-fixtures :once (compose-fixtures
                     validate-schemas
                     (with-log-level-fixture :warn)))

(use-fixtures :each registry.test/reset-registry)

(def test-app-ctx {::FOO "foo"})

(deftest create-router-test
  (let [{event-s schema/a-frame-router-event-stream
         :as _router} (sut/create-router test-app-ctx {})]
    (is (stream/stream? event-s))))

(deftest reg-global-interceptor-test
  (let [{global-interceptors-a schema/a-frame-router-global-interceptors-a
         :as router} (sut/create-router
                      test-app-ctx
                      {schema/a-frame-router-global-interceptors [{:id ::foo}]})]
    (is (= [{:id ::foo}]
           @global-interceptors-a))
    (testing "registering a new global interceptor"
      (sut/reg-global-interceptor router {:id ::bar})
      (is (= [{:id ::foo}
              {:id ::bar}]
             @global-interceptors-a)))
    (testing "registering a replacement global interceptor"
      (sut/reg-global-interceptor router {:id ::bar :stuff ::things})
      (is (= [{:id ::foo}
              {:id ::bar :stuff ::things}]
             @global-interceptors-a)))))

(deftest clear-global-interceptor-test
  (testing "clearing all global interceptors"
    (let [{global-interceptors-a schema/a-frame-router-global-interceptors-a
           :as router} (sut/create-router
                        test-app-ctx
                        {schema/a-frame-router-global-interceptors
                         [{:id ::foo}]})]
      (sut/clear-global-interceptors router)
      (is (= [] @global-interceptors-a))))
  (testing "clearing a single global interceptor"
    (let [{global-interceptors-a schema/a-frame-router-global-interceptors-a
           :as router} (sut/create-router
                        test-app-ctx
                        {schema/a-frame-router-global-interceptors
                         [{:id ::foo} {:id ::bar}]})]
      (sut/clear-global-interceptors router ::bar)
      (is (= [{:id ::foo}] @global-interceptors-a)))))



(deftest dispatch-test
  (let [{event-s schema/a-frame-router-event-stream
         :as router} (sut/create-router test-app-ctx {})]

    (testing "dispatch with a plain event"
      (sut/dispatch router [::foo])

      (is (= (events/coerce-extended-event [::foo])
             @(stream/take! event-s))))

    (testing "dispatch with an extended-event"
      (let [cofxev {schema/a-frame-coeffects {::bar 100}
                    schema/a-frame-event [::foo]}]
        (sut/dispatch router cofxev)

        (is (= cofxev @(stream/take! event-s)))))))

(deftest dispatch-n-test
  (let [{event-s schema/a-frame-router-event-stream
         :as router} (sut/create-router
                      test-app-ctx
                      {schema/a-frame-router-buffer-size 0})]
    (sut/dispatch-n router [[::foo]
                            [::bar]
                            {schema/a-frame-event [::baz]
                             schema/a-frame-coeffects {::baz 300}}])

    (is (= (events/coerce-extended-event [::foo]) @(stream/take! event-s)))
    (is (= (events/coerce-extended-event [::bar]) @(stream/take! event-s)))
    (is (= {schema/a-frame-event [::baz]
            schema/a-frame-coeffects {::baz 300}} @(stream/take! event-s)))))

(deftest handle-event-test
  ;; we use a ::bar effect in a few of these tests
  (fx/reg-fx ::bar (fn [app data]
                     (is (= test-app-ctx app))
                     (is (= 100 data))))

  (testing "handles a successfully processed event"
    (let [router (sut/create-router test-app-ctx {})

          _ (events/reg-event-fx
             ::handle-event-test-success
             (fn [cofx event-v]
               (is (= {schema/a-frame-coeffect-event event-v} cofx))
               (is (= [::handle-event-test-success] event-v))
               {::bar 100}))

          {effects schema/a-frame-effects}
          @(sut/handle-event
            router
            false
            (events/coerce-extended-event [::handle-event-test-success]))]

      (is (= {::bar 100} effects))))

  (testing "applies global interceptors"
    (let [intc {::interceptor-chain/name ::applies-global-interceptors-intc
                ::interceptor-chain/leave (fn [ctx] (assoc ctx ::intc ::ok))}
          _ (interceptor-chain/register-interceptor
             ::applies-global-interceptors-intc
             intc)

          router (sut/create-router
                  test-app-ctx
                  {schema/a-frame-router-global-interceptors
                   [::applies-global-interceptors-intc]})

          _ (events/reg-event-fx
             ::applies-global-interceptors
             (fn [cofx event-v]
               (is (= {schema/a-frame-coeffect-event event-v} cofx))
               (is (= [::applies-global-interceptors] event-v))
               {::bar 100}))

          {effects schema/a-frame-effects
           interceptor-result ::intc}
          @(sut/handle-event
            router
            false
            (events/coerce-extended-event [::applies-global-interceptors]))]

      (is (= {::bar 100} effects))
      (is (= ::ok interceptor-result))))

  (testing "implements interceptor-chain modification"
    (let [router (sut/create-router test-app-ctx {})

          _ (interceptor-chain/register-interceptor
             ::foo
             {::interceptor-chain/name ::foo
              ::interceptor-chain/enter (fn [ctx]
                                          (assoc-in
                                           ctx
                                           [schema/a-frame-coeffects
                                            ::foo-enter]
                                           true))
              ::interceptor-chain/leave (fn [ctx]
                                          (assoc-in
                                           ctx
                                           [schema/a-frame-coeffects
                                            ::foo-leave]
                                           true))})

          _ (interceptor-chain/register-interceptor
             ::bar
             {::interceptor-chain/name ::bar
              ::interceptor-chain/enter (fn [ctx]
                                          (assoc-in
                                           ctx
                                           [schema/a-frame-coeffects
                                            ::bar-enter]
                                           true))
              ::interceptor-chain/leave (fn [ctx]
                                          (assoc-in
                                           ctx
                                           [schema/a-frame-coeffects
                                            ::bar-leave]
                                           true))})

          _ (events/reg-event-fx
             ::implements-interceptor-chain-mods
             [::foo
              ::bar]
             (fn [cofx event-v]
               (is (= {schema/a-frame-coeffect-event event-v} cofx))
               (is (= [::implements-interceptor-chain-mods] event-v))
               {::event-handler true}))

          {_effects schema/a-frame-effects
           foo-enter ::foo-enter
           foo-leave ::foo-leave
           bar-enter ::bar-enter
           bar-leave ::bar-leave
           :as _coeffects}
          @(sut/handle-event
            router
            false
            (assoc
             (events/coerce-extended-event [::implements-interceptor-chain-mods])

             ;; this interceptor-chain modifier removes
             ;; event-handler, and all :leave and :error fns, before
             ;; adding an initial interceptor which extracts coeffects
             ;; from the context
             schema/a-frame-event-modify-interceptor-chain
             (partial std-interceptors/modify-interceptors-for-coeffects 1)))]

      (is foo-enter)
      (is (not foo-leave))
      (is bar-enter)
      (is (not bar-leave))))

  (testing "handles an extended-event with coeffects"
    (let [router (sut/create-router test-app-ctx {})

          org-event-v [::handle-event-test-extended-event-with-coeffects]

          _ (events/reg-event-fx
             ::handle-event-test-extended-event-with-coeffects
             (fn [cofx event-v]
               (is (= org-event-v event-v))
               (is (= {schema/a-frame-coeffect-event event-v
                       ::foo 1000} cofx))
               (is (= [::handle-event-test-extended-event-with-coeffects
                       ] event-v))
               {::bar 100}))

          {effects schema/a-frame-effects}
          @(sut/handle-event
            router
            false
            {schema/a-frame-event org-event-v
             schema/a-frame-coeffects {::foo 1000}})]

      (is (= {::bar 100} effects))))

  (with-log-level :fatal
    (testing "handles an event processing failure with catch? true"
      (let [router (sut/create-router test-app-ctx {})
            _ (events/reg-event-fx
               ::foo
               (fn [cofx event-v]
                 (is (= {schema/a-frame-coeffect-event event-v} cofx))
                 (is (= [::foo] event-v))

                 (throw (prpr/error-ex ::boo {::blah 55}))))

            r @(sut/handle-event router true (events/coerce-extended-event [::foo]))

            ;; unwrap to get the original error
            org-err (some-> r ex-cause)]

        (is (= (str ::boo) (ex-message org-err)))
        (is (= {:tag ::boo
                :value {::blah 55}} (ex-data org-err)))))

    (testing "propagates an event-processing failure with catch? false"
      (let [router (sut/create-router test-app-ctx {})
            _ (events/reg-event-fx
               ::foo
               (fn [cofx event-v]
                 (is (= {schema/a-frame-coeffect-event event-v} cofx))
                 (is (= [::foo] event-v))

                 (throw (prpr/error-ex ::boo {::blah 55}))))

            [tag val] (try
                        [::ok @(sut/handle-event
                                router
                                false
                                (events/coerce-extended-event [::foo]))]
                        (catch Exception x
                          [::error x]))

            ;; unwrap to get the original error
            org-err (some-> val ex-cause)]

        (is (= ::error tag))
        (is (= (str ::boo) (ex-message org-err)))
        (is (= {:tag ::boo
                :value {::blah 55}} (ex-data org-err)))))))

(deftest handle-event-stream-test
  (testing "handles a stream of successful events"
    (let [router (sut/create-router test-app-ctx {})

          out-s (stream/stream 100)

          _ (events/reg-event-fx
             ::foo
             (fn [cofx event-v]
               (is (= {schema/a-frame-coeffect-event event-v} cofx))

               (stream/put! out-s event-v)
               {}))

          _ (events/reg-event-fx
             ::bar
             (fn [cofx event-v]
               (is (= {schema/a-frame-coeffect-event event-v} cofx))

               (stream/put! out-s event-v)
               {}))

          _ (sut/handle-event-stream router)]

      (sut/dispatch router [::foo])
      (sut/dispatch router [::bar 100])

      (is (= [::foo] @(stream/take! out-s)))
      (is (= [::bar 100] @(stream/take! out-s)))))

  (with-log-level :fatal
    (testing "handles failures"
      (let [router (sut/create-router test-app-ctx {})

            out-s (stream/stream 100)

            _ (events/reg-event-fx
               ::foo
               (fn [cofx [_ n :as event-v]]
                 (is (= {schema/a-frame-coeffect-event event-v} cofx))

                 (if (odd? n)
                   (throw (prpr/error-ex ::boo {::boo ::hoo}))
                   (do
                     (stream/put! out-s event-v)
                     {}))))

            _ (sut/handle-event-stream router)]

        (sut/dispatch router [::foo 0])
        (sut/dispatch router [::foo 1])
        (sut/dispatch router [::foo 2])

        (is (= [::foo 0] @(stream/take! out-s)))
        (is (= [::foo 2] @(stream/take! out-s)))))))

(deftest handle-sync-event-stream-test
  (testing "with no dispatch fx"
    (let [{event-s schema/a-frame-router-event-stream
           :as router} (sut/create-router test-app-ctx {})
          out-a (atom [])

          _ (events/reg-event-fx
             ::handle-sync-event-stream-test-no-dispatch
             (fn [cofx [_ n :as event-v]]
               (is (= {schema/a-frame-coeffect-event event-v} cofx))

               (swap! out-a conj n)

               {}))]

      @(stream/put!
        event-s
        (events/coerce-extended-event
         [::handle-sync-event-stream-test-no-dispatch 0]))
      @(sut/handle-sync-event-stream router)
      (is (= [0] @out-a))
      (is (stream/closed? event-s))))

  (testing "with a dispatch fx"
    (let [{event-s schema/a-frame-router-event-stream
           :as router} (sut/create-router test-app-ctx {})
          out-a (atom [])

          _ (events/reg-event-fx
             ::handle-sync-event-stream-test-with-dispatch
             (fn [cofx [_ n :as event-v]]
               (is (= {schema/a-frame-coeffect-event event-v} cofx))

               (swap! out-a conj n)

               (when (<= n 3)
                 {:a-frame/dispatch
                  [::handle-sync-event-stream-test-with-dispatch (+ n 2)]})))]

      @(stream/put!
        event-s
        (events/coerce-extended-event
         [::handle-sync-event-stream-test-with-dispatch 0]))
      @(sut/handle-sync-event-stream router)
      (is (= [0 2 4] @out-a))
      (is (stream/closed? event-s)))))

(deftest dispatch-sync-test
  (testing "with no dispatch fx"
    (let [{event-s schema/a-frame-router-event-stream
           :as router} (sut/create-router test-app-ctx {})
          out-a (atom [])

          _ (events/reg-event-fx
             ::dispatch-sync-test-no-dispatch
             (fn [cofx [_ n :as event-v]]
               (is (= {schema/a-frame-coeffect-event event-v} cofx))

               (swap! out-a conj n)

               {}))

          {r-effects :a-frame/effects
           r-coeffects :a-frame/coeffects
           :as _r} @(sut/dispatch-sync
                     router
                     [::dispatch-sync-test-no-dispatch 0])]

      (is (= [0] @out-a))
      ;; the main event-s should not be closed
      (is (not (stream/closed? event-s)))

      (is (= {} r-effects))
      (is (= {:a-frame.coeffect/event [::dispatch-sync-test-no-dispatch 0]}
             r-coeffects))))

  (testing "with a dispatch fx"
    (let [{event-s schema/a-frame-router-event-stream
           :as router} (sut/create-router test-app-ctx {})
          out-a (atom [])

          _ (events/reg-event-fx
             ::dispatch-sync-test-with-dispatch
             (fn [cofx [_ n :as event-v]]
               (is (= {schema/a-frame-coeffect-event event-v} cofx))

               (swap! out-a conj n)

               (when (<= n 3)
                 {:a-frame/dispatch
                  [::dispatch-sync-test-with-dispatch (+ n 2)]})))

          {r-effects :a-frame/effects
           r-coeffects :a-frame/coeffects
           :as _r} @(sut/dispatch-sync
                     router
                     [::dispatch-sync-test-with-dispatch 0])]

      (is (= [0 2 4] @out-a))
      (is (not (stream/closed? event-s)))

      (is (= {:a-frame/dispatch [::dispatch-sync-test-with-dispatch 2]}
             r-effects))
      (is (= {:a-frame.coeffect/event [::dispatch-sync-test-with-dispatch 0]}
             r-coeffects))))

  (testing "with a dispatch-sync fx"
    (let [{event-s schema/a-frame-router-event-stream
           :as router} (sut/create-router test-app-ctx {})
          out-a (atom [])

          _ (events/reg-event-fx
             ::dispatch-sync-test-with-dispatch-sync-cofx
             (fn [cofx [_ n :as event-v]]
               (is (= {schema/a-frame-coeffect-event event-v} cofx))

               (swap! out-a conj n)

               (when (<= n 3)
                 {:a-frame/dispatch-sync
                  [::dispatch-sync-test-with-dispatch-sync-cofx (+ n 2)]})))

          {r-effects :a-frame/effects
           r-coeffects :a-frame/coeffects
           :as _r} @(sut/dispatch-sync
                     router
                     [::dispatch-sync-test-with-dispatch-sync-cofx 0])]


      (is (= [0 2 4] @out-a))
      (is (not (stream/closed? event-s)))

      (is (= {:a-frame/dispatch-sync
              [::dispatch-sync-test-with-dispatch-sync-cofx 2]}
             r-effects))
      (is (= {:a-frame.coeffect/event
              [::dispatch-sync-test-with-dispatch-sync-cofx 0]}
             r-coeffects))))

  (with-log-level :fatal
    (testing "propagates error from dispatched event"
      (let [{event-s schema/a-frame-router-event-stream
             :as router} (sut/create-router test-app-ctx {})
            out-a (atom [])

            _ (events/reg-event-fx
               ::dispatch-sync-test-propagates-error
               (fn [cofx [_ n :as event-v]]
                 (is (= {schema/a-frame-coeffect-event event-v} cofx))

                 (swap! out-a conj n)

                 (throw (prpr/error-ex
                         ::boo
                         {::event-v event-v}))))

            [tag val] (try
                        [::ok @(sut/dispatch-sync
                                router
                                [::dispatch-sync-test-propagates-error 0])]
                        (catch Exception x
                          [::error x]))

            ;; must unwrap the original error
            {err-tag :tag
             err-val :value
             :as _err-data} (some-> val ex-cause ex-data)]

        (is (= [0] @out-a))
        ;; the main event-s should not be closed
        (is (not (stream/closed? event-s)))

        (is (= tag ::error))
        (is (= ::boo err-tag))
        (is (= {::event-v [::dispatch-sync-test-propagates-error 0]} err-val)))))

  (with-log-level :fatal
    (testing "propagates error from nested dispatches"
      (let [{event-s schema/a-frame-router-event-stream
             :as router} (sut/create-router test-app-ctx {})
            out-a (atom [])
            after-fx-calls-a (atom [])

            _ (registry/register-handler
               schema/a-frame-kind-fx
               ::dispatch-sync-propagates-error-from-nested-dispatch-after-dispatch-fx
               (fn [_app val]
                 (swap! after-fx-calls-a conj val)))

            _ (events/reg-event-fx
               ::dispatch-sync-propagates-error-from-nested-dispatch
               (fn [cofx [_ n :as event-v]]
                 (is (= {schema/a-frame-coeffect-event event-v} cofx))

                 (swap! out-a conj n)

                 (if (<= n 3)
                   [{:a-frame/dispatch-sync
                     [::dispatch-sync-propagates-error-from-nested-dispatch
                      (+ n 2)]}

                    {::dispatch-sync-propagates-error-from-nested-dispatch-after-dispatch-fx
                     n}]

                   (throw (prpr/error-ex ::boo {::event-v event-v})))))

            [tag val] (try
                        [::ok
                         @(sut/dispatch-sync
                           router
                           [::dispatch-sync-propagates-error-from-nested-dispatch
                            0])]
                        (catch Exception x
                          [::error x]))

            ;; have to unwrap the original error from the nested errors
            {err-tag :tag
             err-val :value
             :as _err-data} (some-> val ex-cause ex-cause ex-cause ex-data)]

        (is (= [0 2 4] @out-a))
        (is (not (stream/closed? event-s)))

        (is (= tag ::error))
        (is (= ::boo err-tag))
        (is (= {::event-v
                [::dispatch-sync-propagates-error-from-nested-dispatch 4]}
               err-val))

        ;; none of the fx called after the dispatch-sync should be called, since
        ;; dispatch-sync propagates the error back to caller and prevents progress
        ;; through the fx list
        (is (= [] @after-fx-calls-a))))))

(deftest dispatch-n-sync-test
  (testing "with no dispatch fx"
    (let [{event-s schema/a-frame-router-event-stream
           :as router} (sut/create-router test-app-ctx {})
          out-a (atom [])

          _ (events/reg-event-fx
             ::handle-n-sync-event-stream-test-no-dispatch
             (fn [cofx [_ n :as event-v]]
               (is (= {schema/a-frame-coeffect-event event-v} cofx))

               (swap! out-a conj n)

               {}))]

      @(sut/dispatch-n-sync
        router
        [[::handle-n-sync-event-stream-test-no-dispatch 0]
         [::handle-n-sync-event-stream-test-no-dispatch 1]])

      (is (= [0 1] @out-a))
      (is (not (stream/closed? event-s)))))

  (testing "with dispatch fx"
    (let [{event-s schema/a-frame-router-event-stream
           :as router} (sut/create-router test-app-ctx {})
          out-a (atom [])

          _ (events/reg-event-fx
             ::handle-n-sync-event-stream-test-with-dispatch
             (fn [cofx [_ n :as event-v]]
               (is (= {schema/a-frame-coeffect-event event-v} cofx))

               (swap! out-a conj n)

               (when (<= n 3)

                 {:a-frame/dispatch
                  [::handle-n-sync-event-stream-test-with-dispatch (+ n 2)]})))]

      @(sut/dispatch-n-sync
        router
        [[::handle-n-sync-event-stream-test-with-dispatch 0]
         [::handle-n-sync-event-stream-test-with-dispatch 1]])

      (is (= [0 1 2 3 4 5]
             @out-a))

      ;; main stream should not be closed
      (is (not (stream/closed? event-s)))))

  (testing "with dispatch fx"
    (let [{event-s schema/a-frame-router-event-stream
           :as router} (sut/create-router test-app-ctx {})
          out-a (atom [])

          _ (events/reg-event-fx
             ::handle-n-sync-event-stream-test-with-dispatch-sync-and-coeffects
             (fn [cofx [_ n :as event-v]]
               (is (= {schema/a-frame-coeffect-event event-v} cofx))

               (swap! out-a conj n)

               (when (<= n 3)

                 {:a-frame/dispatch-sync
                  [::handle-n-sync-event-stream-test-with-dispatch-sync-and-coeffects
                   (+ n 2)]})))]

      @(sut/dispatch-n-sync
        router
        [[::handle-n-sync-event-stream-test-with-dispatch-sync-and-coeffects 0]
         [::handle-n-sync-event-stream-test-with-dispatch-sync-and-coeffects 1]])

      ;; note the order because :dispatch-sync fx are used
      (is (= [0 2 4 1 3 5]
             @out-a))

      ;; main stream should not be closed
      (is (not (stream/closed? event-s)))))

  )

(deftest run-a-frame-router-test
  (testing "handles event loopback correctly"
    (let [router (sut/create-router test-app-ctx {})
          _ (sut/run-a-frame-router router)

          out-s (stream/stream 100)

          _ (events/reg-event-fx
             ::foo
             (fn [cofx [_ n :as event-v]]
               ;; (prn "entering" event-v)
               (is (= {schema/a-frame-coeffect-event event-v} cofx))

               (if (<= n 100)
                 (do
                   (stream/put! out-s n)
                   (sut/dispatch router [::foo (inc n)]))
                 (stream/close! out-s))

               {}))]

      (sut/dispatch router [::foo 0])

      (is (= 5050 @(stream/reduce + 0 out-s))))))

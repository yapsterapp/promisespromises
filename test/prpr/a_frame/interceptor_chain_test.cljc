(ns prpr.a-frame.interceptor-chain-test
  #?(:cljs (:require-macros
            [prpr.test
             :refer [deftest test-async is testing
                     use-fixtures]]
            [prpr.a-frame.interceptor-chain-test
             :refer [wrap-catch]]))
  (:require

   #?(:cljs [prpr.test])

   #?(:clj [prpr.test :refer [deftest test-async is testing
                              use-fixtures compose-fixtures]])

   #?(:cljs [schema.core :as s])
   #?(:clj [schema.test :refer [validate-schemas]])

   [prpr.promise :as prpr]
   [prpr.a-frame.schema :as af.schema]
   [prpr.a-frame.registry.test :as registry.test]
   [prpr.a-frame.interceptor-chain :as sut]))

#?(:clj (use-fixtures :each (compose-fixtures
                             validate-schemas
                             registry.test/reset-registry)))

#?(:cljs (use-fixtures :once {:before (fn [] (s/set-fn-validation! true))
                              :after (fn [] (s/set-fn-validation! false))}))

#?(:cljs (use-fixtures :each registry.test/reset-registry))

(defn epoch
  []
  #?(:clj (System/currentTimeMillis)
     :cljs (.now js/Date)))

(def empty-interceptor-context
  {af.schema/a-frame-app-ctx ::app
   af.schema/a-frame-router ::a-frame
   ::sut/queue []
   ::sut/stack '()
   ::sut/history []})

(deftest execute-empty-chain-test
  (test-async
   (prpr/ddo
    [:let [chain []
           input {:test (rand-int 9999)}]
     r (sut/execute ::app ::a-frame chain input)]
    (prpr/return
     (is (= (merge
             empty-interceptor-context
             input)
            r))))))

(deftest execute-single-interceptor-test
  (sut/register-interceptor
   ::execute-single-interceptor-test
   {::sut/enter (fn [x] (assoc x :entered? true))
    ::sut/leave (fn [x] (assoc x :left? true))})

  (test-async
   (prpr/ddo
    [:let [chain [::execute-single-interceptor-test]
           input {:test (rand-int 9999)}]
     r (sut/execute ::app ::a-frame chain input)]
    (prpr/return
     (is (= (merge
             empty-interceptor-context
             input
             {:entered? true
              :left? true}
             {::sut/history [[::execute-single-interceptor-test ::sut/enter]
                             [::execute-single-interceptor-test ::sut/leave]]})
            r))))))

(deftest execute-single-interceptor-with-data-test
  (sut/register-interceptor
   ::execute-single-interceptor-with-data-test
   {::sut/enter (fn [x data]
                  (is (= "foofoo" data))
                  (assoc x :entered? true))
    ::sut/leave (fn [x data]
                  (is (= "barbar" data))
                  (assoc x :left? true))})

  (test-async
   (prpr/ddo
    [:let [intc-with-data {::sut/key ::execute-single-interceptor-with-data-test
                           ::sut/data {::sut/enter-data #ctx/path [::foo]
                                       ::sut/leave-data #ctx/path [::bar]}}
           chain [intc-with-data]
           input {:test (rand-int 9999)
                  ::foo "foofoo"
                  ::bar "barbar"}]
     r (sut/execute ::app ::a-frame chain input)]
    (prpr/return
     (is (= (merge
             empty-interceptor-context
             input
             {:entered? true
              :left? true}
             {::sut/history [[intc-with-data ::sut/enter "foofoo"]
                             [intc-with-data ::sut/leave "barbar"]]})
            r))))))

(deftest execute-multiple-interceptors-test
  (doseq [[key inter] [[::execute-multiple-interceptors-test-A
                        {::sut/name ::copy-restore
                         ::sut/enter (fn [{t :test :as x}]
                                       (assoc x :test2 t))
                         ::sut/leave (fn [{t :test2 :as x}]
                                       (-> x
                                           (assoc :test t)
                                           (dissoc :test2)))}]

                       [::execute-multiple-interceptors-test-B
                        {::sut/name ::mult
                         ::sut/enter (fn [x]
                                       (update x :test * 2))}]

                       [::execute-multiple-interceptors-test-C
                        {::sut/name ::save-state
                         ::sut/enter (fn [x]
                                       (update
                                        x
                                        :states (fnil conj [])
                                        (-> (sut/dissoc-context-keys x)
                                            (dissoc :states))))}]

                       [::execute-multiple-interceptors-test-D
                        {::sut/name ::mark-leaving
                         ::sut/leave (fn [x]
                                       (assoc x :leaving-at (epoch)))}]]]
    (sut/register-interceptor key inter))

  (test-async
   (prpr/ddo
    [:let [chain [::execute-multiple-interceptors-test-A
                  ::execute-multiple-interceptors-test-B
                  ::execute-multiple-interceptors-test-C
                  ::execute-multiple-interceptors-test-D]
           {t :test :as input} {:test (rand-int 9999)}
           epoch-before (epoch)]
     r (sut/execute ::app ::a-frame chain input)
     :let [epoch-after (epoch)]]
    (prpr/return
     (do (is (= (merge
                 empty-interceptor-context
                 input
                 {:states [{:test (* t 2) :test2 t}]}
                 {::sut/history
                  [[::execute-multiple-interceptors-test-A ::sut/enter]
                   [::execute-multiple-interceptors-test-B ::sut/enter]
                   [::execute-multiple-interceptors-test-C ::sut/enter]
                   [::execute-multiple-interceptors-test-D ::sut/noop ::sut/enter]
                   [::execute-multiple-interceptors-test-D ::sut/leave]
                   [::execute-multiple-interceptors-test-C ::sut/noop ::sut/leave]
                   [::execute-multiple-interceptors-test-B ::sut/noop ::sut/leave]
                   [::execute-multiple-interceptors-test-A ::sut/leave]]})
                (dissoc r :leaving-at)))
         (is (<= epoch-before (:leaving-at r) epoch-after)))))))

(deftest execute-promise-based-interceptors-test
  (doseq [[key inter] [[::execute-promise-based-interceptors-test-A
                        {::sut/enter (fn [x]
                                       (prpr/success-pr
                                        (assoc x :success true)))}]
                       [::execute-promise-based-interceptors-test-B
                        {::sut/leave (fn [x]
                                       (prpr/chain-pr
                                        (prpr/success-pr x)
                                        (fn [x]
                                          (assoc x :chain true))))}]
                       [::execute-promise-based-interceptors-test-C
                        {::sut/enter (fn [x]
                                       (prpr/ddo
                                        [:let [x' (assoc x :ddo true)]]
                                        (prpr/return x')))}]]]
    (sut/register-interceptor key inter))

  (test-async
   (prpr/ddo
    [:let [chain [::execute-promise-based-interceptors-test-A
                  ::execute-promise-based-interceptors-test-B
                  ::execute-promise-based-interceptors-test-C]
           input {}]
     r (sut/execute ::app ::a-frame chain input)]
    (prpr/return
     (is (=
          (merge
           empty-interceptor-context
           {:chain true
            :success true
            :ddo true

            ::sut/history
            [[::execute-promise-based-interceptors-test-A ::sut/enter]
             [::execute-promise-based-interceptors-test-B ::sut/noop ::sut/enter]
             [::execute-promise-based-interceptors-test-C ::sut/enter]
             [::execute-promise-based-interceptors-test-C ::sut/noop ::sut/leave]
             [::execute-promise-based-interceptors-test-B ::sut/leave]
             [::execute-promise-based-interceptors-test-A ::sut/noop ::sut/leave]]})
            r))))))

(deftest execute-queue-alteration-test
  (doseq [[key inter] [[::execute-queue-alteration-test-late-arrival
                        {::sut/enter (fn [x] (assoc x :arrived :late))
                         ::sut/leave (fn [x] (assoc x :left :early))}]

                       [::execute-queue-alteration-test-alteration
                        {::sut/enter
                         (fn [x]
                           (prpr/always-pr
                            (sut/enqueue
                             x
                             [::execute-queue-alteration-test-late-arrival])))}]]]
    (sut/register-interceptor key inter))

  (test-async
   (prpr/ddo
    [:let [chain [::execute-queue-alteration-test-alteration]
           input {}]
     r (sut/execute ::app ::a-frame chain input)]
    (prpr/return
     (is (= (merge
             empty-interceptor-context
             {:arrived :late
              :left :early

              ::sut/history
              [[::execute-queue-alteration-test-alteration ::sut/enter]
               [::execute-queue-alteration-test-late-arrival ::sut/enter]
               [::execute-queue-alteration-test-late-arrival ::sut/leave]
               [::execute-queue-alteration-test-alteration ::sut/noop ::sut/leave]]})
            r))))))

(deftest execute-stack-alteration-test
  (doseq [[key inter] [[::execute-stack-alteration-test-late-arrival
                        {::sut/enter (fn [x] (assoc x :arrived :late))
                         ::sut/leave (fn [x] (assoc x :left :early))}]

                       [::execute-stack-alteration-test-alteration
                        {::sut/leave (fn [x]
                                       (prpr/always-pr
                                        (update
                                         x
                                         ::sut/stack
                                         conj
                                         ::execute-stack-alteration-test-late-arrival)))}]]]
    (sut/register-interceptor key inter))

  (test-async
   (prpr/ddo
    [:let [chain [::execute-stack-alteration-test-alteration]
           input {}]
     r (sut/execute ::app ::a-frame chain input)]
    (prpr/return
     (is (= (merge
             empty-interceptor-context
             {:left :early

              ::sut/history
              [[::execute-stack-alteration-test-alteration ::sut/noop ::sut/enter]
               [::execute-stack-alteration-test-alteration ::sut/leave]
               [::execute-stack-alteration-test-late-arrival ::sut/leave]]})
            r))))))

(deftest execute-error-handling-test
  (let [suppressed-errors (atom [])
        wrap-catch-execute (fn [chain input]
                             (reset! suppressed-errors [])
                             (prpr/catchall
                              (prpr/chain-pr
                               (sut/execute*
                                (fn [e] (throw e))
                                (fn [xs] (swap! suppressed-errors concat xs))
                                (sut/initiate ::app ::a-frame chain input))
                               (fn [r] [::ok r]))
                              (fn [e] [::error e])))]
    (test-async
     (testing "captures error in :enter interceptor"
       (doseq [[k i] [[::execute-error-handline-test-enter-boom
                       {::sut/enter
                        (fn [_] (throw (prpr/error-ex ::boom {})))}]
                      [::execute-error-handline-test-enter-unexpected-boom
                       {::sut/enter
                        (fn [_]
                          (throw (prpr/error-ex ::unexpected-boom {})))}]]]
         (sut/register-interceptor k i))

       (prpr/ddo
        [:let [chain [::execute-error-handline-test-enter-boom
                      ::execute-error-handline-test-enter-unexpected-boom
                      ]]
         [tag r] (wrap-catch-execute chain {})]
        (prpr/return
         (do (is (= ::error tag))
             (is (= {:tag ::boom
                     :value {}}
                    (-> r ex-cause ex-data)))
             (is (empty? @suppressed-errors))))))

     (testing "captures error in :leave interceptor"
       (doseq [[k i] [[::execute-error-handling-test-leave-unexpected-boom
                       {::sut/leave
                        (fn [_] (throw (prpr/error-ex ::unexpected-boom {})))}
                       ]

                      [::execute-error-handling-test-leave-boom
                       {::sut/leave
                        (fn [_] (throw (prpr/error-ex ::boom {})))}]]]
         (sut/register-interceptor k i))

       (prpr/ddo
        [:let [chain [::execute-error-handling-test-leave-unexpected-boom
                      ::execute-error-handling-test-leave-boom]]
         [tag r] (wrap-catch-execute chain {})]
        (prpr/return
         (do (is (= ::error tag))
             (is (= {:tag ::boom
                     :value {}}
                    (-> r ex-cause ex-data)))
             (is (empty? @suppressed-errors))))))

     (testing "captures errors in error handlers"
       (let [left-with (atom nil)]

         (doseq [[k i]
                 [[::execute-error-handling-test-error-handler-error-left-with
                   {::sut/error (fn [x _] (reset! left-with ::error) x)
                    ::sut/leave (fn [x] (reset! left-with ::leave) x)}]

                  [::execute-error-handling-test-error-handler-error-error
                   {::sut/error (fn [_ _] (throw (prpr/error-ex ::error-error {})))}]

                  [::execute-error-handling-test-error-handler-error-boom
                   {::sut/enter (fn [_] (throw (prpr/error-ex ::boom {})))}]]]

           (sut/register-interceptor k i))

         (prpr/ddo
          [:let [chain
                 [::execute-error-handling-test-error-handler-error-left-with
                  ::execute-error-handling-test-error-handler-error-error
                  ::execute-error-handling-test-error-handler-error-boom]]

           [tag r] (wrap-catch-execute chain {})]
          (prpr/return
           (do (is (= ::error @left-with))
               (is (= ::error tag))
               (is (= {:tag ::error-error
                       :value {}}
                      (-> r ex-cause ex-data)))
               (is (= [{:tag ::boom
                        :value {}}]
                      (map
                       (comp ex-data ex-cause)
                       @suppressed-errors))))))))

     (testing "captures error promises"
       (doseq [[k i]
               [[::execute-error-handling-test-error-promises-boom
                 {::sut/enter (fn [_] (prpr/error-pr ::boom {}))}]

                [::execute-error-handling-test-error-promises-unexpected-boom
                 {::sut/enter (fn [_] (throw (prpr/error-ex ::unexpected-boom {})))}]]]
         (sut/register-interceptor k i))

       (prpr/ddo
        [:let [chain
               [::execute-error-handling-test-error-promises-boom
                ::execute-error-handling-test-error-promises-unexpected-boom]]
         [tag r] (wrap-catch-execute chain {})]
        (prpr/return
         (do (is (= ::error tag))
             (is (= {:tag ::boom
                     :value {}}
                    (-> r ex-cause ex-data)))
             (is (empty? @suppressed-errors))))))

     (testing "throws if error not cleared"
       (doseq [[k i] [[::execute-error-handline-not-cleared-clear
                       {::sut/error (fn [c _] (sut/clear-errors c))}]
                      [::execute-error-handling-not-cleared-boom
                       {::sut/enter (fn [_] (prpr/error-pr ::boom {:fail :test}))}]]]
         (sut/register-interceptor k i))

       (prpr/ddo
        [:let [chain [::execute-error-handling-not-cleared-boom]]
         [tag _r] (wrap-catch-execute chain {})]
        (prpr/return
         (is (= ::error tag))))
       (prpr/ddo
        [:let [chain [::execute-error-handline-not-cleared-clear
                      ::execute-error-handling-not-cleared-boom]]
         [tag r] (wrap-catch-execute chain {})]
        (prpr/return
         (do (is (= ::ok tag))
             (is (= (merge
                     empty-interceptor-context
                     {::sut/history
                      [[::execute-error-handline-not-cleared-clear ::sut/noop ::sut/enter]
                       [::execute-error-handling-not-cleared-boom ::sut/enter]
                       [::execute-error-handling-not-cleared-boom ::sut/noop ::sut/error]
                       [::execute-error-handline-not-cleared-clear ::sut/error]]})
                    r)))))))))

#?(:clj
   (defmacro wrap-catch
     [form]
     `(prpr/catchall
       (prpr/ddo [ctx# ~form]
                 (prpr/return-pr
                  [::ok ctx#]))
       (fn [e#]
         (prpr/return-pr
          [::error e#])))))

(deftest resume-test

  (let [throw?-a (atom true)]

    (doseq [[key inter]
            [[::resume-test-throw-once
              {::sut/enter (fn [x]
                             (if @throw?-a
                               (do
                                 (reset! throw?-a false)
                                 (throw (ex-info "boo" {})))
                               x))
               ::sut/leave (fn [x] (assoc x :left? true))}]]]
      (sut/register-interceptor key inter))

    (test-async
     (testing "can resume after failure"
       (prpr/ddo [[tag err] (wrap-catch
                             (sut/execute
                              ::app
                              ::a-frame
                              [::resume-test-throw-once]
                              {}))

                  _ (is (= tag ::error))

                  [resume-tag
                    {resume-left? :left?
                     :as _resume-val}] (wrap-catch
                                        (sut/resume ::app ::a-frame err))]

                 (is (= ::ok resume-tag))
                 (is (= true resume-left?)))))))

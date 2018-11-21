(ns prpr.cats.prws-test
  (:require
   #?(:clj [prpr.cats.prws :as sut :refer [prwsdo]]
      :cljs [prpr.cats.prws :as sut :refer [PRWS_V_T] :refer-macros [prwsdo]])
   [cats.core :as monad :refer [>>= return]]
   [cats.context :refer [with-context]]
   [cats.monad.state :as state]
   [prpr.promise :as prpr :refer [ddo]]
   [prpr.test :refer [deftest test-async is testing]]
   [prpr.cats.reader :as reader]
   [prpr.cats.writer :as writer]
   [schema.test])
  #?(:clj
     (:import
      [prpr.cats.prws PRWS_V_T])))

#?(:clj
   (clojure.test/use-fixtures :once schema.test/validate-schemas))

(deftest tag-result-test
  (test-async
   (ddo [r (sut/tag-result
            {::monad/val :val
             ::state/state :state
             ::writer/log :log})]
     (return
      (is (= r
             (into (PRWS_V_T.)
                   {::monad/val :val
                    ::state/state :state
                    ::writer/log :log})))))))

(deftest lift-promise-test
  (test-async

   ;; promise of a plain value
   (ddo [:let [lv (sut/lift-promise (prpr/success-pr :foo))]
         r (sut/run-prws lv {::state/state :state ::reader/env :env})]
     (return
      (is (= {::monad/val :foo ::state/state :state ::writer/log nil}
             (into {} r)))))

   ;; promise of a PRWS_V - not sure about this
   (ddo [:let [lv (sut/lift-promise (sut/tag-result
                                     {::monad/val :p-val
                                      ::state/state :p-state
                                      ::writer/log [:p-log]}))]
         r (sut/run-prws lv {::state/state :state ::reader/env :env})]
     (return
      (is (= {::monad/val :p-val ::state/state :p-state ::writer/log [:p-log]}
             (into {} r)))))

   ;; promise of a PRWS - not sure about this
   (ddo [:let [lv (sut/lift-promise (prwsdo [r (reader/ask)
                                             _ (state/swap #(assoc % :renv r))
                                             _ (writer/tell [:blah])]
                                            (return :foo)))]
         r (sut/run-prws lv {::state/state {:foo 100} ::reader/env :env})]
     (return
      (is (= {::monad/val :foo ::state/state {:foo 100 :renv :env} ::writer/log [:blah]}
             (into {} r)))))))

(deftest lift-value-test
  (test-async
   (ddo [:let [lv (sut/lift-value :foo)]
         r (sut/run-prws lv {::state/state :state ::reader/env :env})]
     (return
      (is (= {::monad/val :foo ::state/state :state ::writer/log nil}
             (into {} r)))))))

(deftest lift-test
  (test-async
   ;; lift a plain value
   (ddo [:let [lv (sut/lift :foo)]
         r (sut/run-prws lv {::state/state :state ::reader/env :env})]
     (return
      (is (= {::monad/val :foo ::state/state :state ::writer/log nil}
             (into {} r)))))

   ;; lift a promise of a plain value
   (ddo [:let [lv (sut/lift (prpr/success-pr :foo))]
         r (sut/run-prws lv {::state/state :state ::reader/env :env})]
     (return
      (is (= {::monad/val :foo ::state/state :state ::writer/log nil}
             (into {} r)))))))

(deftest context-test
  (test-async
   (testing "return"
     (ddo [:let [comp (with-context sut/context
                        (return :foo))]
           r (sut/run-prws comp {::state/state :state ::reader/env :env})]
       (return
        (is (= {::monad/val :foo ::state/state :state ::writer/log nil}
               (into {} r))))))

   (testing "prwsdo"
     (ddo [:let [comp (prwsdo [a (return :foo)]
                              (return a))]
           r (sut/run-prws comp {::state/state :state ::reader/env :env})]
       (return
        (is (= {::monad/val :foo ::state/state :state ::writer/log nil}
               (into {} r))))))

   (testing "reader-ask"
     (ddo [:let [comp (prwsdo [{a :a b :b} (reader/ask)]
                              (return (+ a b)))]
           r (sut/run-prws comp {::state/state :state ::reader/env {:a 1 :b 2}})]
       (return
        (is (= {::monad/val 3 ::state/state :state ::writer/log nil}
               (into {} r))))))

   (testing "reader-local"
     (ddo [:let [comp (prwsdo [{a :a b :b} (reader/ask)]
                              (return (+ a b)))
                 l-comp (reader/local (constantly {:a 3 :b 4}) comp)]
           r (sut/run-prws l-comp {::state/state :state ::reader/env {:a 1 :b 2}})]
       (return
        (is (= {::monad/val 7 ::state/state :state ::writer/log nil}
               (into {} r))))))

   (testing "writer-tell"
     (ddo [:let [comp (prwsdo [a (writer/tell [:foo])
                               b (writer/tell [:bar])]
                              (return [a b]))]
           r (sut/run-prws comp {::state/state :state ::reader/env :env})]
       (return
        (is (= {::monad/val [nil nil] ::state/state :state ::writer/log [:foo :bar]}
               (into {} r))))))

   (testing "writer-listen"
     (ddo [:let [comp (prwsdo [_ (writer/tell [:foo])
                               _ (writer/tell [:bar])]
                              (return :val))
                 comp-2 (prwsdo [v-l (writer/listen comp)]
                                (return v-l))]
           r (sut/run-prws comp-2 {::state/state :state ::reader/env :env})]
       (return
        (is (= {::monad/val [:val [:foo :bar]]
                ::state/state :state
                ::writer/log [:foo :bar]}
               (into {} r))))))

   (testing "writer-pass"
     (ddo [:let [comp (prwsdo [_ (writer/tell [:foo])
                               _ (writer/tell [:bar])]
                              (return [:val #(map name %)]))
                 comp-2 (writer/pass comp)]
           r (sut/run-prws comp-2 {::state/state :state ::reader/env :env})]
       (return
        (is (= {::monad/val :val ::state/state :state ::writer/log ["foo" "bar"]}
               (into {} r))))))

   (testing "state-get-put"
     (ddo [:let [comp (prwsdo [{a :a :as s} (state/get)
                               old (state/put (assoc s :b (inc a)))]
                              (return old))]

           r (sut/run-prws comp {::state/state {:a 1} ::reader/env :env})]
       (return
        (is (= {::monad/val {:a 1} ::state/state {:a 1 :b 2} ::writer/log nil}
               (into {} r))))))

   (testing "state-swap"
     (ddo [:let [comp (prwsdo [old (state/swap #(assoc % :b :bar))]
                              (return old))]
           r (sut/run-prws comp {::state/state {:a 1} ::reader/env :env})]
       (return
        (is (= {::monad/val {:a 1} ::state/state {:a 1 :b :bar} ::writer/log nil}
               (into {} r))))))

   ;; monad laws taken from https://wiki.haskell.org/Monad_laws

   (testing "left identity"
     (ddo [:let [f (fn [v]
                     ;; exercise reader, writer and state
                     (prwsdo [r (reader/ask)
                              _ (state/swap #(assoc % :renv r))
                              _ (writer/tell [:blah])]
                             (return (inc v))))]

           {l-v ::monad/val
            l-s ::state/state
            l-w ::writer/log
            :as l} (sut/run-prws
                    (>>= (return sut/context 1) f)
                    {::state/state {:foo 1}
                     ::reader/env 100})

           {:as r} (sut/run-prws
                    (f 1)
                    {::state/state {:foo 1}
                     ::reader/env 100})]
       (return
        [(is (= l-v 2))
         (is (= l-s {:foo 1 :renv 100}))
         (is (= l-w [:blah]))
         (is (= l r))])))

   (testing "right identity"
     (ddo [;; exercise reader, writer and state
           :let [mv (prwsdo [r (reader/ask)
                             _ (state/swap #(assoc % :renv r))
                             _ (writer/tell [:blah])]
                            (return sut/context :foo))]
           {l-v ::monad/val
            l-s ::state/state
            l-w ::writer/log
            :as l} (sut/run-prws
                    (prwsdo [v mv]
                            (return v))
                    {::state/state {:foo 1}
                     ::reader/env 100})
           {:as r} (sut/run-prws
                    mv
                    {::state/state {:foo 1}
                     ::reader/env 100})]
       (return
        [(is (= l-v :foo))
         (is (= l-s {:foo 1 :renv 100}))
         (is (= l-w [:blah]))
         (is (= l r))])))

   (testing "associativity"
     (ddo [:let [f #(return sut/context (inc %))
                 g #(return sut/context (* 10 %))
                 ;; exercise reader, writer and state
                 mv (prwsdo [r (reader/ask)
                             _ (state/swap #(assoc % :renv r))
                             _ (writer/tell [:blah])]
                            (return sut/context 1))]
           {l-v ::monad/val
            l-s ::state/state
            l-w ::writer/log
            :as l} (sut/run-prws
                    (prwsdo [y (prwsdo [x mv]
                                       (f x))]
                            (g y))
                    {::state/state {:foo 1}
                     ::reader/env 100})
           {:as r} (sut/run-prws
                    (prwsdo [x mv]
                            (prwsdo [y (f x)]
                                    (g y)))
                    {::state/state {:foo 1}
                     ::reader/env 100})]
       (return
        [(is (= l-v 20))
         (is (= l-s {:foo 1 :renv 100}))
         (is (= l-w [:blah]))
         (is (= l r))])))))

(deftest run-prws-test
  (let [comp (prwsdo [r (reader/ask)
                      _ (state/swap #(assoc % :renv r))
                      _ (writer/tell [:blah])]
                     (return :foo))]
    (test-async
     (ddo [{v ::monad/val
            s ::state/state
            w ::writer/log} (sut/run-prws comp {::state/state {:foo 1}
                                                ::reader/env 100})]
       (is (= v :foo))
       (is (= s {:foo 1 :renv 100}))
       (is (= w [:blah]))))))

(deftest eval-prws-test
  (let [comp (prwsdo [r (reader/ask)
                      _ (state/swap #(assoc % :renv r))
                      _ (writer/tell [:blah])]
                     (return :foo))]
    (test-async
     (ddo [v (sut/eval-prws comp {::state/state {:foo 1}
                                  ::reader/env 100})]
       (is (= v :foo))))))

;; TODO need some error-handling tests

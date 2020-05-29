(ns prpr.interceptor-chain-test
  #?(:cljs (:require-macros [prpr.test]))
  (:require [prpr.test :refer [deftest test-async is testing use-fixtures]]
            [prpr.interceptor-chain :as sut]
            [prpr.promise :as prpr]
            [schema.core :as s]
            [schema.test :refer [validate-schemas]]))

#?(:clj (use-fixtures :each validate-schemas)
   :cljs (use-fixtures :once {:before (fn [] (s/set-fn-validation! true))
                              :after (fn [] (s/set-fn-validation! false))}))

(defn epoch
  []
  #?(:clj (System/currentTimeMillis)
     :cljs (.now js/Date)))

(deftest execute-empty-chain-test
  (test-async
   (prpr/ddo
    [:let [chain []
           input {:test (rand-int 9999)}]
     r (sut/execute chain input)]
    (prpr/return
     (do (is (= (merge
                 input
                 {:interceptor/queue []
                  :interceptor/stack '()})
                r)))))))

(deftest execute-single-interceptor-test
  (test-async
   (prpr/ddo
    [:let [chain [{:enter (fn [x] (assoc x :entered? true))
                   :leave (fn [x] (assoc x :left? true))}]
           input {:test (rand-int 9999)}]
     r (sut/execute chain input)]
    (prpr/return
     (do (is (= (merge
                 input
                 {:entered? true
                  :left? true}
                 {:interceptor/queue []
                  :interceptor/stack '()})
                r)))))))

(deftest execute-multiple-interceptors-test
  (test-async
   (prpr/ddo
    [:let [chain [{:name ::copy-restore
                   :enter (fn [{t :test :as x}]
                            (assoc x :test2 t))
                   :leave (fn [{t :test2 :as x}]
                            (-> x
                                (assoc :test t)
                                (dissoc :test2)))}
                  {:name ::mult
                   :enter (fn [x]
                            (update x :test * 2))}
                  {:name ::save-state
                   :enter (fn [x]
                            (update
                             x
                             :states (fnil conj [])
                             (-> (sut/dissoc-context-keys x)
                                 (dissoc :states))))}
                  {:name ::mark-leaving
                   :leave (fn [x]
                            (assoc x :leaving-at (epoch)))}]
           {t :test :as input} {:test (rand-int 9999)}
           epoch-before (epoch)]
     r (sut/execute chain input)
     :let [epoch-after (epoch)]]
    (prpr/return
     (do (is (= (merge
                 input
                 {:states [{:test (* t 2) :test2 t}]}
                 {:interceptor/queue []
                  :interceptor/stack '()})
                (dissoc r :leaving-at)))
         (is (<= epoch-before (:leaving-at r) epoch-after)))))))

(deftest execute-promise-based-interceptors-test
  (test-async
    (prpr/ddo
     [:let [chain [{:enter (fn [x]
                             (prpr/success-pr
                              (assoc x :success true)))}
                   {:leave (fn [x]
                             (prpr/chain-pr
                              (prpr/success-pr x)
                              (fn [x]
                                (assoc x :chain true))))}
                   {:enter (fn [x]
                             (prpr/ddo
                              [:let [x' (assoc x :ddo true)]]
                              (prpr/return x')))}]
            input {}]
      r (sut/execute chain input)]
     (prpr/return
      (do (is (= {:chain true
                  :success true
                  :ddo true
                  :interceptor/queue []
                  :interceptor/stack '()}
                 r)))))))

(deftest execute-queue-alteration-test
  (test-async
    (prpr/ddo
     [:let [late-arrival {:enter (fn [x] (assoc x :arrived :late))
                          :leave (fn [x] (assoc x :left :early))}
            chain [{:enter (fn [x]
                             (prpr/always-pr
                              (sut/enqueue x [late-arrival])))}]
            input {}]
      r (sut/execute chain input)]
     (prpr/return
      (do (is (= {:arrived :late
                  :left :early
                  :interceptor/queue []
                  :interceptor/stack '()}
                 r)))))))

(deftest execute-stack-alteration-test
  (test-async
    (prpr/ddo
     [:let [late-arrival {:enter (fn [x] (assoc x :arrived :late))
                          :leave (fn [x] (assoc x :left :early))}
            chain [{:leave (fn [x]
                             (prpr/always-pr
                              (update x :interceptor/stack conj late-arrival)))}]
            input {}]
      r (sut/execute chain input)]
     (prpr/return
      (do (is (= {:left :early
                  :interceptor/queue []
                  :interceptor/stack '()}
                 r)))))))

(deftest execute-error-handling-test
  (let [suppressed-errors (atom [])
        wrap-catch-execute (fn [chain input]
                             (reset! suppressed-errors [])
                             (prpr/catchall
                              (prpr/chain-pr
                               (sut/execute
                                (fn [e] (throw e))
                                (fn [xs] (swap! suppressed-errors concat xs))
                                (sut/initiate chain input))
                               (fn [r] [::ok r]))
                              (fn [e] [::error e])))]
    (test-async
     (testing "captures error in :enter interceptor"
       (prpr/ddo
        [:let [chain [{:enter (fn [_] (throw (prpr/error-ex ::boom {})))}
                      {:enter (fn [_] (throw (prpr/error-ex ::unexpected-boom {})))}]]
         [tag r] (wrap-catch-execute chain {})]
        (prpr/return
         (do (is (= ::error tag))
             (is (= {:tag ::boom
                     :value {}}
                    (ex-data r)))
             (is (empty? @suppressed-errors))))))
     (testing "captures error in :leave interceptor"
       (prpr/ddo
        [:let [chain [{:leave (fn [_] (throw (prpr/error-ex ::unexpected-boom {})))}
                      {:leave (fn [_] (throw (prpr/error-ex ::boom {})))}]]
         [tag r] (wrap-catch-execute chain {})]
        (prpr/return
         (do (is (= ::error tag))
             (is (= {:tag ::boom
                     :value {}}
                    (ex-data r)))
             (is (empty? @suppressed-errors))))))
     (testing "captures errors in error handlers"
       (prpr/ddo
        [:let [left-with (atom nil)
               chain [{:error (fn [x _] (reset! left-with ::error) x)
                       :leave (fn [x] (reset! left-with ::leave) x)}
                      {:error (fn [_ _] (throw (prpr/error-ex ::error-error {})))}
                      {:enter (fn [_] (throw (prpr/error-ex ::boom {})))}]]
         [tag r] (wrap-catch-execute chain {})]
        (prpr/return
         (do (is (= ::error @left-with))
             (is (= ::error tag))
             (is (= {:tag ::error-error
                     :value {}}
                    (ex-data r)))
             (is (= [{:tag ::boom
                      :value {}}]
                    (map ex-data @suppressed-errors)))))))
     (testing "captures error promises"
       (prpr/ddo
        [:let [chain [{:enter (fn [_] (prpr/error-pr ::boom {}))}
                      {:enter (fn [_] (throw (prpr/error-ex ::unexpected-boom {})))}]]
         [tag r] (wrap-catch-execute chain {})]
        (prpr/return
         (do (is (= ::error tag))
             (is (= {:tag ::boom
                     :value {}}
                    (ex-data r)))
             (is (empty? @suppressed-errors))))))
     (testing "throws if error not cleared"
       (prpr/ddo
        [:let [chain [{:enter (fn [_] (prpr/error-pr ::boom {:fail :test}))}]]
         [tag r] (wrap-catch-execute chain {})]
        (prpr/return
         (do (is (= ::error tag)))))
       (prpr/ddo
        [:let [chain [{:error (fn [c _] (sut/clear-errors c))}
                      {:enter (fn [_] (prpr/error-pr ::boom {:fail :test}))}]]
         [tag r] (wrap-catch-execute chain {})]
        (prpr/return
         (do (is (= ::ok tag))
             (is (= {:interceptor/queue []
                     :interceptor/stack '()}
                    r)))))))))

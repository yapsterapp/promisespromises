(ns deferst.system-test
  (:require
   [cats.core :refer [return]]
   [prpr.test :refer [deftest test-async is testing use-fixtures]]
   [clojure.set :as set]
   [schema.test]
   [prpr.promise :as pr :refer [ddo]]
   [deferst.system :as s]
   [cats.monad.state :as state]
   [cats.core :as monad]))

;; check schemas
#?(:clj
   (use-fixtures :once schema.test/validate-schemas))

(deftest empty-system-test
  (test-async
   (let [sb (s/system-builder [])
         sys (s/start-system! sb {:foo 10})
         sysmap-pr (s/system-map sys)
         x-sysmap {:foo 10}]

     (testing "null system contains config"
       (ddo [sysmap sysmap-pr]
         (return
          (is (= sysmap x-sysmap))))))))

(deftest empty-system-stops
  (test-async
   (let [sb (s/system-builder [])
         sys-pr (s/start-system! sb {:foo 10})
         stop-sys (s/stop-system! sys-pr)]

     (testing "null system has no managed objects"
       (ddo [{st ::state/state} sys-pr]
         (is (contains? st :deferst.system/system))
         (is (empty? (:deferst.system/system st)))
         (return true))))))

(deftest single-item-system
  (test-async
   (let [destructor-vals (atom [])
         ff (fn [v] [v (fn [] (swap! destructor-vals conj v))])
         sb (s/system-builder [[:a ff {:a-arg [:foo]}]])
         sys (s/start-system! sb {:foo 10})
         sysmap-pr (s/system-map sys)
         stop-sys (s/stop-system! sys)

         x-sysmap {:foo 10 :a {:a-arg 10}}
         x-dvals [{:a-arg 10}]]

     (testing "single item system has the single object"
       (ddo [sysmap sysmap-pr]
         (is (= sysmap x-sysmap))))

     (testing "single item was destroyed"
       (ddo [_ stop-sys]
         (is (= @destructor-vals x-dvals)))))))

(deftest single-item-system-with-vector-arg-specs
  (test-async
   (let [destructor-vals (atom [])
         ff (fn [v] [v (fn [] (swap! destructor-vals conj v))])
         sb (s/system-builder [[:a ff [:foo]]])
         sys (s/start-system! sb {:foo 10})
         sysmap-pr (s/system-map sys)
         stop-sys (s/stop-system! sys)
         x-sysmap {:foo 10 :a 10}
         x-dvals [10]]

     (testing "single item system has the single object"
       (ddo [sysmap sysmap-pr]
         (is (= sysmap x-sysmap))))

     (testing "single item was destroyed"
       (ddo [_ stop-sys]
         (return
          (is (= @destructor-vals x-dvals))))))))

(deftest bad-arg-specs-throw
  (test-async
   (let [destructor-vals (atom [])
         ff (fn [v] [v (fn [] (swap! destructor-vals conj v))])]

     (testing "an error is thrown describing the bad arg specs"
       (is (thrown? #?(:clj clojure.lang.ExceptionInfo :cljs :default)
                    (s/system-builder [[:a ff :foo]])))))))

(deftest single-item-system-without-destructors
  (test-async
   (let [sb (s/system-builder [[:a identity {:a-arg [:foo]}]])
         sys (s/start-system! sb {:foo 10})
         sysmap-pr (s/system-map sys)
         stop-sys (s/stop-system! sys)
         x-sysmap {:foo 10
                   :a {:a-arg 10}}]

     (testing "single item system has the single object"
       (ddo [sysmap sysmap-pr]
         (is (= sysmap x-sysmap)))))))

(deftest single-deferred-item-system
  (test-async
   (let [destructor-vals (atom [])

         ff (fn [v]
              (let [obj-destr [v (fn [] (swap! destructor-vals conj v))]]
                (pr/success-pr obj-destr)))
         sb (s/system-builder [[:a ff {:a-arg [:foo]}]])
         sys (s/start-system! sb {:foo 10})
         sysmap-pr (s/system-map sys)
         stop-sys (s/stop-system! sys)
         x-sysmap {:foo 10 :a {:a-arg 10}}]

     (testing "single item system has the single object"
       (ddo [sysmap sysmap-pr]
         (is (= sysmap x-sysmap)))))))


(deftest dependent-item-system
  (test-async
   (let [destructor-vals (atom [])
         ff (fn [v] [v (fn [] (swap! destructor-vals conj v))])
         sb (s/system-builder [[:a ff {:a-arg [:foo]}]
                               [:b ff {:b-arg [:a :a-arg]}]])
         sys (s/start-system! sb {:foo 10})
         sysmap-pr (s/system-map sys)
         stop-sys (s/stop-system! sys)
         x-sysmap {:foo 10 :a {:a-arg 10} :b {:b-arg 10}}
         x-dvals [{:b-arg 10} {:a-arg 10}]]

     (testing "dependent items are created"
       (ddo [sysmap sysmap-pr]
         (is (= sysmap x-sysmap))))

     (testing "dependent items were destroyed"
       (ddo [_ stop-sys]
         (is (= @destructor-vals x-dvals)))))))

(deftest dependent-item-system-specified-out-of-order
  (test-async
   (let [destructor-vals (atom [])
         ff (fn [v] [v (fn [] (swap! destructor-vals conj v))])
         sb (s/system-builder [[:b ff {:b-arg [:a :a-arg]}]
                               [:a ff {:a-arg [:foo]}]])
         sys (s/start-system! sb {:foo 10})
         sysmap-pr (s/system-map sys)
         stop-sys (s/stop-system! sys)
         x-sysmap {:foo 10 :a {:a-arg 10} :b {:b-arg 10}}
         x-dvals [{:b-arg 10} {:a-arg 10}]]

     (testing "dependent items are created"
       (ddo [sysmap sysmap-pr]
         (is (= sysmap x-sysmap))))

     (testing "dependent items were destroyed"
       (ddo [_ stop-sys]
         (is (= @destructor-vals x-dvals)))))))

(deftest dependent-item-system-with-circular-deps
  (test-async
   (let [ff (fn [v] v)]
     (is (thrown-with-msg?
          #?(:clj clojure.lang.ExceptionInfo :cljs :default)
          #"circular dependency"
          (s/system-builder [[:a ff {:a-arg [:b :b-arg]}]
                             [:b ff {:b-arg [:a :a-arg]}]]))))))

(deftest dependent-item-system-with-vector-arg-specs
  (test-async
   (let [destructor-vals (atom [])
         ff (fn [v] [v (fn [] (swap! destructor-vals conj v))])
         sb (s/system-builder [[:a ff [:foo]]
                               [:b ff {:b-arg [:a]}]])
         sys (s/start-system! sb {:foo 10})
         sysmap-pr (s/system-map sys)
         stop-sys (s/stop-system! sys)
         x-sysmap {:foo 10 :a 10 :b {:b-arg 10}}
         x-dvals [{:b-arg 10} 10]]

     (testing "dependent items are created"
       (ddo [sysmap sysmap-pr]
         (is (= sysmap x-sysmap))))

     (testing "dependent items were destroyed"
       (ddo [_ stop-sys]
         (is (= @destructor-vals x-dvals)))))))

(defn dependent-item-system-with-mixed-destructors-fixtures
  []
  (let [destructor-vals (atom [])
        ff (fn [v] [v (fn [] (swap! destructor-vals conj v))])
        sb (s/system-builder [[:a identity {:a-arg [:foo]}]
                              [:b ff {:b-arg [:a :a-arg]}]])
        sys (s/start-system! sb {:foo 10})
        sysmap-pr (s/system-map sys)
        stop-sys (s/stop-system! sys)
        x-sysmap {:foo 10 :a {:a-arg 10} :b {:b-arg 10}}
        x-dvals [{:b-arg 10}]]
    {:destructor-vals destructor-vals
     :ff ff
     :sb sb
     :sys sys
     :sysmap-pr sysmap-pr
     :stop-sys stop-sys
     :x-sysmap x-sysmap
     :x-dvals x-dvals}))

(deftest dependent-item-system-with-mixed-destructors-creation
  (test-async
   (let [{destructor-vals :destructor-vals
          ff :ff
          b :sb
          sys :sys
          sysmap-pr :sysmap-pr
          stop-sys :stop-sys
          x-sysmap :x-sysmap
          x-dvals :x-dvals} (dependent-item-system-with-mixed-destructors-fixtures)]

     (testing "dependent items are created"
       (ddo [sysmap sysmap-pr]
         (is (= sysmap x-sysmap)))))))

(deftest dependent-item-system-with-mixed-destructors-destruction
  (test-async
   (let [{destructor-vals :destructor-vals
          ff :ff
          b :sb
          sys :sys
          sysmap-pr :sysmap-pr
          stop-sys :stop-sys
          x-sysmap :x-sysmap
          x-dvals :x-dvals} (dependent-item-system-with-mixed-destructors-fixtures)]

     (testing "dependent items were destroyed"
       (ddo [_ stop-sys]
         (is (= @destructor-vals x-dvals)))))))

(defn composed-builders-fixtures
  []
  (let [destructor-vals (atom [])
        ff (fn [v] [v (fn [] (swap! destructor-vals conj v))])
        sb-obj-specs [[:a ff {:a-arg [:foo]}]
                      [:b identity {:b-arg [:a :a-arg]}]]
        sb2-obj-specs [[:c ff {:c-a [:a :a-arg]
                               :c-b [:b :b-arg]}]]
        x-sysmap {:foo 10 :a {:a-arg 10} :b {:b-arg 10} :c {:c-a 10 :c-b 10}}
        x-dvals [{:c-a 10 :c-b 10} {:a-arg 10}]

        sb (s/system-builder sb-obj-specs)
        sb2 (s/system-builder sb sb2-obj-specs)
        sys (s/start-system! sb2 {:foo 10})
        sysmap-pr (s/system-map sys)
        stop-sys (s/stop-system! sys)

        x-sysmap {:foo 10 :a {:a-arg 10} :b {:b-arg 10} :c {:c-a 10 :c-b 10}}
        x-dvals [{:c-a 10 :c-b 10} {:a-arg 10}]]
    {:destructor-vals destructor-vals
     :sb2 sb2
     :sys sys
     :sysmap-pr sysmap-pr
     :stop-sys stop-sys
     :x-sysmap x-sysmap
     :x-dvals x-dvals}))

(deftest composed-builders-creation
  (test-async
   (let [{destructor-vals :destructor-vals
          sb2 :sb2
          sys :sys
          sysmap-pr :sysmap-pr
          stop-sys :stop-sys
          x-sysmap :x-sysmap
          x-dvals :x-dvals} (composed-builders-fixtures)]

     (testing "items are created"
       (ddo [sysmap sysmap-pr]
         (is (= sysmap x-sysmap)))))))

(deftest composed-builders-destruction
  (test-async
   (let [{destructor-vals :destructor-vals
          sb2 :sb2
          sys :sys
          sysmap-pr :sysmap-pr
          stop-sys :stop-sys
          x-sysmap :x-sysmap
          x-dvals :x-dvals} (composed-builders-fixtures)]

     (testing "items were destroyed"
       (ddo [_ stop-sys]
         (is (= @destructor-vals x-dvals)))))))

;; (deftest unwind-on-builder-error
;;   (test-async
;;    (let [destructor-vals (atom [])
;;          ff (fn [v] [v (fn [] (swap! destructor-vals conj v))])
;;          boom (fn [v] (throw (ex-info "boom" {:boom true})))
;;          sb (s/system-builder [[:a ff {:a-arg [:foo]}]
;;                                [:b boom {:b-arg [:a :a-arg]}]])
;;          sys (s/start-system! sb {:foo 10})
;;          x-dvals [{:a-arg 10}]]
;;      (testing "system is unwound"
;;        (ddo [_ sys]
;;          (is (= @destructor-vals x-dvals))
;;          (is (thrown-with-msg?
;;               clojure.lang.ExceptionInfo
;;               #"start-system! failed and unwound"
;;               @sys)))))))

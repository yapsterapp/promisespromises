(ns prpr.test.malli
  (:require
   #?(:clj [malli.instrument :as mi])
   ))

;; defns get arity checking
#?(:cljs (defn before-fn [] true))
#?(:cljs (defn after-fn [] true))

(def instrument-fns-fixture
  #?(:clj
     (fn [f]
       (mi/instrument!)
       (f))

     :cljs
     {:before before-fn
      :after after-fn}))

(ns prpr.test.malli
  (:require
   #?(:clj [malli.instrument :as mi])
   ))

(def instrument-fns-fixture
  #?(:clj
     (fn [f]
       (mi/instrument!)
       (f))

     :cljs
     {:before (fn [])
      :afrter (fn [])}))

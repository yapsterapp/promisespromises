(ns prpr.a-frame.registry.test
  (:require
   #?(:clj [clojure.test :as t]
      :cljs [cljs.test :as t :include-macros true])
   [prpr.a-frame.registry :as registry]))

#?(:cljs
   (def snapshot-a (atom nil)))

(defn reset-registry
  "test fixture to restore the registry to it's prior state
   after running tests, which helps in two ways

   1. the registry doesn't get polluted with test handlers
   2. if used as an :each fixture, then tests don't need to
      use unique handler keys"
  [f]
  #?(:clj
     (let [snapshot @registry/registry]
       (f)
       (reset! registry/registry snapshot))

     :cljs
     {:before (fn [] (reset! snapshot-a @registry/registry))
      :after (fn [] (reset! registry/registry @snapshot-a))}))

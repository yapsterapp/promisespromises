(ns prpr3.test
  (:require-macros
   [cljs.test]
   [prpr3.util.macro]
   [promesa.core]
   [prpr3.test]
   [prpr3.test.reduce])
  (:require
   [cljs.test]
   [prpr3.util.macro]
   [promesa.core]
   [taoensso.timbre]
   [prpr3.test.reduce]))

(defn compose-fixtures
  "deals properly with cljs async map fixtures"
  [f1 f2]
  (let [{f1-before :before
         f1-after :after} (if (map? f1)
                            f1
                            {:before f1})
        {f2-before :before
         f2-after :after} (if (map? f2)
                            f2
                            {:before f2})]
    {:before (fn []
               (when (some? f1-before) (f1-before))
               (when (some? f2-before) (f2-before))
               true)
     :after (fn []
              (when (some? f1-after) (f1-after))
              (when (some? f2-after) (f2-after))
              true)}))

(defn with-log-level-fixture
  [level]
  (let [cl (or (:level taoensso.timbre/*config*)
               :info)]
    {:before (fn []
               (taoensso.timbre/set-level! level)
               true)
     :after (fn []
              (taoensso.timbre/set-level! cl)
              true)}))

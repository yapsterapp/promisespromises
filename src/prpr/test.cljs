(ns prpr.test
  (:require-macros
   [cljs.test]
   [prpr.util.macro]
   [promesa.core]
   [prpr.test]
   [prpr.test.reduce])
  (:require
   [cljs.test]
   [prpr.util.macro]
   [promesa.core]
   [taoensso.timbre]
   [prpr.test.reduce]))

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
    {:before (cljs.test/compose-fixtures f1-before f2-before)
     :after (cljs.test/compose-fixtures f1-after f2-after)}))

(defn with-log-level-fixture
  [level]
  (let [cl (or (:level taoensso.timbre/*config*)
               :info)]
    {:before (fn [] (taoensso.timbre/set-level! level))
     :after (fn [] (taoensso.timbre/set-level! cl))}))

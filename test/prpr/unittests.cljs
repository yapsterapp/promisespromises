(ns prpr.unittests
  (:require
   [cljs.test :as t]
   [prpr.promise-cljs-test]
   [prpr.cats.prws-test]))

(def success 0)

(defn ^:export run []

  (.log js/console "er-webui unit-tests started.")

  (enable-console-print!)

  (t/run-tests
   'prpr.promise-cljs-test
   'prpr.cats.monoid-test
   'prpr.cats.prws-test)

  0)

(ns prpr.test.reduce
  (:require
   [promesa.core :as pr]
   [taoensso.timbre
    #?@(:clj [:refer [error]]
        :cljs [:refer-macros [error]])]))

(defn invoke-f
  "invoke 0-args promise-returning function f
   and make sure we always see any exception trace"
  [f]
  (pr/handle
   (try
     (f)
     (catch #?(:clj Exception :cljs :default) x
       (pr/rejected x)))
   (fn [succ err]
     (if (some? err)
       (do
         (error err "test error")
         (throw err))
       [::ok succ]))))

(defn reduce-pr-fns
  "sequentially reduce a seq of
   promise-returning 0-args fns into
   Promise<[results]>"
  [fns]
  (pr/loop [rs []
            [f & remf] fns]
    (if (nil? f)
      rs
      (pr/let [r (invoke-f f)]
        (pr/recur (conj rs r) remf)))))

(ns prpr.test.reduce
  (:require
   [promesa.core :as pr]))

(defn reduce-pr-fns
  "sequentially reduce a seq of
   promise-returning 0-args fns into
   Promise<[results]>"
  [fns]
  (pr/loop [rs []
            [f & remf] fns]
    (if (nil? f)
      rs
      (pr/let [r (f)]
        (pr/recur (conj rs r) remf)))))

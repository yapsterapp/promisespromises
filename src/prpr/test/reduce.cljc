(ns prpr.test.reduce
  (:require
   [promesa.core :as pr]
   [prpr.promise :as prpr]))

(def max-depth 20)

(defn seq-of-fns?
  [v]
  (and
   (sequential? v)
   (every? fn? v)))

(defn reduce-pr-fns
  "sequentially reduce a seq of
   promise-returning 0-args fns into
   Promise<[results]>"
  [fns]
  ;; (prn "reduce-pr-fns" (count fns))
  #_{:clj-kondo/ignore [:loop-without-recur]}
  (pr/loop [rs []
            depth 0
            [f & remf] fns]
    (if (nil? f)
      rs
      (pr/let [r (prpr/always (f))]
        (cond

          ;; if a form returns a single fn (or a promise of a fn)
          ;; and we're not beyond the max-depth, then call it
          (fn? r)
          (do
            ;; (prn "single fn")
            (if (< depth max-depth)
              (pr/recur
               rs
               (inc depth)
               ;; remf is a list, so conj puts r at the front
               (conj remf r))

              (throw (ex-info "testing : max depth exceeded"
                              {:max-depth max-depth}))))

          ;; if a form returns a seq of fns, then call them
          ;; in order
          (seq-of-fns? r)
          (do
            ;; (prn "seq-of-fns")
            (if (< depth max-depth)
              (pr/recur
               rs
               (inc depth)
               (concat r remf))

              (throw (ex-info "testing : max depth exceeded"
                              {:max-depth max-depth}))))

          :else
          (do
            ;; (prn "normal result")
            (pr/recur
             (conj rs r)
             0
             remf)))))))

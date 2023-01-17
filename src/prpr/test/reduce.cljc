(ns prpr.test.reduce
  (:require
   [promesa.core :as pr]
   [prpr.promise :as prpr]
   [prpr.error :as err]))

(def max-depth 20)

(defn seq-of-fns?
  [v]
  (and
   (sequential? v)
   (every? fn? v)))

(defn reduce-pr-fns*
  "sequentially reduce a seq of
   promise-returning 0-args fns into
   Promise<[results]>"
  [fns]
  #_{:clj-kondo/ignore [:loop-without-recur]}
  (pr/loop [rs []
            depth 0
            fns fns]

    ;; (prn "reduce-pr-fns: loop" (count rs) depth (count fns))

    (let [[f & remf] fns]
      (if (nil? f)

        rs

        (prpr/catch-always
         (let [r-p (f)]
           ;; (prn "reduce-pr-fns: result-p" depth (type r-p))

           (pr/let [r r-p]
             ;; (prn "reduce-pr-fns: derefed result" depth (type r))

             (cond

               ;; if a form returns a single fn (or a promise of a fn)
               ;; and we're not beyond the max-depth, then call it
               (fn? r)
               #_{:clj-kondo/ignore [:redundant-do]}
               (do
                 ;; (prn "single fn")
                 (if (< depth max-depth)
                   (pr/recur
                    rs
                    (inc depth)
                    ;; remf is a list, so conj puts r at the front
                    (conj remf r))

                   (err/wrap-uncaught
                    (ex-info "testing : max depth exceeded"
                             {:max-depth max-depth}))))

               ;; if a form returns a seq of fns, then call them
               ;; in order
               (seq-of-fns? r)
               #_{:clj-kondo/ignore [:redundant-do]}
               (do
                 ;; (prn "seq-of-fns")
                 (if (< depth max-depth)
                   (pr/recur
                    rs
                    (inc depth)
                    (concat r remf))

                   (err/wrap-uncaught
                    (ex-info "testing : max depth exceeded"
                             {:max-depth max-depth}))))

               :else
               #_{:clj-kondo/ignore [:redundant-do]}
               (do
                 ;; (prn "normal result")
                 (pr/recur
                  (conj rs r)
                  0
                  remf)))))

         (fn [e]
           (err/wrap-uncaught e)))))))

(defn reduce-pr-fns
  [nm fns]
  ;; (println "reduce-pr-fns*: " nm)
  (pr/let [r (reduce-pr-fns* fns)]
    ;; (println "reduce-pr-fns*: " nm ": done")
    (err/unwrap r)))

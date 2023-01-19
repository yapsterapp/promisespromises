(ns prpr.promise
  #?(:cljs (:require-macros [prpr.promise]))
  (:require
   [promesa.core]
   [prpr.util.macro]))

(defmacro always
  "catch any sync exception from evaluating body, and wrap
   in an errored promise - allowing a single promise-based
   control-flow in promise chains"
  [body]
  `(prpr.util.macro/try-catch
    ~body
    (catch e# (promesa.core/rejected e#))))

(defmacro catch-always
  "catch any sync or promise error"
  [body handler]
  `(promesa.core/catch
       (always ~body)
       ~handler))

(defmacro chain-always
  "always chain"
  [body handler]
  `(promesa.core/chain
    (always ~body)
    ~handler))

(defmacro handle-always
  "handly any sync or promise error"
  [body handler]
  `(promesa.core/handle
    (always ~body)
    ~handler))

(defmacro merge-always
  "merge both branches of a promise into a variant"
  [p]
  `(handle-always
    ~p
    (fn [v# err#]
      (if (some? err#)
        [::error err#]
        [::ok v#]))))

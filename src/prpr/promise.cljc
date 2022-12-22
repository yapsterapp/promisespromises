(ns prpr.promise
  #?(:cljs
     (:require-macros [prpr.promise]))
  (:require
   [promesa.core :as pr]))

(defmacro always
  "catch any sync exception from evaluating body, and wrap
   in an errored promise - allowing a single promise-based
   control-flow in promise chains"
  [body]
  `(try
     ~body
     (catch #?(:clj Exception :cljs :default) e#
       (pr/rejected e#))))

(defmacro catch-always
  "catch any sync or promise error"
  [body handler]
  `(pr/catch
       (always ~body)
       ~handler))

(defmacro chain-always
  "always chain"
  [body handler]
  `(pr/chain
    (always ~body)
    ~handler))

(defmacro handle-always
  "handly any sync or promise error"
  [body handler]
  `(pr/handle
    (always ~body)
    ~handler))

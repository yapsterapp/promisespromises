(ns prpr.promise.platform
  (:require
   [promesa.core]
   [cats.labs.promise]))

(defn ex?
  [v]
  (instance? js/Error v))

(def pr?
  promesa.core/promise?)

(def pr-chain
  promesa.core/chain)

(def pr-all
  promesa.core/all)

(def pr-catch
  promesa.core/catch)

(def pr-finally
  promesa.core/finally)

(def pr-success
  promesa.core/promise)

(def pr-error
  promesa.core/rejected)

(def pr-factory
  promesa.core/promise)

(def pr-branch
  promesa.core/branch)

(def pr-delay
  promesa.core/delay)

(def pr-timeout
  promesa.core/timeout)

(def pr-context
  cats.labs.promise/context)

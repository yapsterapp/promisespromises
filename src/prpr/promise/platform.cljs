(ns prpr.promise.platform
  (:require
   [promesa.core]
   [cats.labs.promise]))

(defn ex?
  [v]
  (instance? js/Error v))

(def pr?
  promesa.core/promise?)

(def pr-catch
  promesa.core/catch)

(def pr-error
  promesa.core/rejected)

(def pr-context
  cats.labs.promise/context)

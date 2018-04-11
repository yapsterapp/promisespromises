(ns prpr.promise.platform
  (:require
   [manifold.deferred]
   [cats.labs.manifold]))

(defn ex?
  [v]
  (instance? Throwable v))

(def pr?
  @#'manifold.deferred/deferred?)

(def pr-catch
  @#'manifold.deferred/catch)

(def pr-finally
  @#'manifold.deferred/finally)

(def pr-success
  @#'manifold.deferred/success-deferred)

(def pr-error
  @#'manifold.deferred/error-deferred)

(def pr-context
  @#'cats.labs.manifold/deferred-context)

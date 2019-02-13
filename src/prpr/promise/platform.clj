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

(defn pr-factory
  "promesa-style promise creation - the factory-cb callback
   receives the resolve and reject fns"
  [factory-cb]
  (let [d (manifold.deferred/deferred)]
    (factory-cb
     #(manifold.deferred/success! d %)
     #(manifold.deferred/error! d %))
    d))

(defn pr-branch
  [p success-fn error-fn]
  (-> p
      (manifold.deferred/chain success-fn)
      (manifold.deferred/catch error-fn)))

(defn pr-delay
  [delay-ms value]
  (let [p (manifold.deferred/deferred)]
    (manifold.deferred/timeout!
     p
     delay-ms
     value)))

(def pr-context
  @#'cats.labs.manifold/deferred-context)

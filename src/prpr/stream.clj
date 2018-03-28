(ns prpr.stream
  (:require
   [manifold.stream :as st]
   [manifold.deferred :as d]
   [cats.core :refer [return]]
   [cats.labs.manifold :refer [deferred-context]]
   [prpr.promise :as pr :refer [ddo]]
   [taoensso.timbre :refer [warn]]))

(defn realize-stream
  [v]
  (if-not (st/stream? v)
    (return deferred-context v)
    (->> v
         st/realize-each
         (st/reduce conj []))))

(defn test-realize-stream
  "given a possibly deferred stream,
   realize it as a Deferred<vector>"
  [v]
  (ddo [v (if (d/deferred? v)
            v
            (d/success-deferred v))]

    (if-not (st/stream? v)
      (return deferred-context v)
      (->> v
           st/realize-each
           (st/reduce conj [])))))

;; a marker for errors on a stream
(defrecord StreamError [error])

(defn stream-error?
  [v]
  (instance? StreamError v))

(defmacro catch-stream-error
  "catch any errors and put them in a StreamError marker"
  [& body]
  `(prpr.promise/catch
       (fn [err#]
         (->StreamError err#))
       ~@body))

(defn log-stream-error-exemplars
  [description n source]
  (let [error-count-atom (atom 0)]
    ;; although we don't log them all, we print a total error count
    (st/on-drained
     source
     (fn []
       (when (> @error-count-atom 0)
         (warn description "total error-count:" @error-count-atom))))

    ;; log only the first n errors encountered
    (fn [v]
      (when (and (stream-error? v)
                 (<= (swap! error-count-atom inc) n))
        (warn "got a StreamError")
        (warn (:error v) description))
      v)))

(defn filter-log-stream-errors
  "log 2 exemplars of StreamErrors on the source, and then filter them out"
  [description source]
  ;; only log 2 error exemplars - just to show if they are the same really
  (let [logger (log-stream-error-exemplars description 2 source)]
    (->> source
         (st/map #(catch-stream-error %))
         (st/realize-each)
         (st/map logger)
         (st/filter #(not (stream-error? %))))))

(defn not-stream-error
  "if v is a stream-error, log it and return nil, otherwise return v"
  [description v]
  (if (stream-error? v)
    (do (warn (:error v) description)
        nil)
    v))

(defn reduce-catch
  "reduce, but with any errors caught and exemplars logged"
  ([description f source]
   (->> source
        (filter-log-stream-errors description)
        (st/reduce f)))
  ([description f init source]
   (->> source
        (filter-log-stream-errors description)
        (st/reduce f init))))

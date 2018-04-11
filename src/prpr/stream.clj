(ns prpr.stream
  (:require
   [manifold.stream :as st]
   [manifold.deferred :as d]
   [cats.core :refer [return]]
   [cats.labs.manifold :refer [deferred-context]]
   [prpr.promise :as pr :refer [ddo]]
   [taoensso.timbre :refer [info warn]])
  (:import
   [manifold.stream Callback]))

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

(defn s-first
  "consume a stream completely, returning a Deferred of the first value from
   the stream, if any. returns Deferred<no-val> if the stream is empty"
  ([source]
   (s-first nil source))
  ([no-val source]
   (st/reduce (fn [fv v]
                (if (= no-val fv)
                  v
                  fv))
              no-val
              source)))

(defn divert-stream-errors
  "kinda awkward - adapted from connect-via... takes src and
   returns [err dst]. feeds StreamErrors from src to err, and any
   other values to dst. closes err and dst when src is exhausted"
  ([src]
   (let [src             (st/realize-each src)
         err             (st/stream)
         dst             (st/stream)
         divert-callback (fn [v]
                           (if (stream-error? v)
                             (st/put! err v)
                             (st/put! dst v)))
         close-callback #(do
                           (st/close! dst)
                           (st/close! err))]
     (st/connect
      src
      (Callback. divert-callback close-callback dst nil)
      {})
     (st/connect
      src
      (Callback. (fn [_] (d/success-deferred true)) nil err nil)
      {})
     [err dst])))

(defn- log-divert-stream-errors
  "log exemplars of stream errors and divert the errors to the err
   stream. returns [err dst]"
  [description source]
  (let [logger (log-stream-error-exemplars description 2 source)]
    (->> source
         (st/map #(catch-stream-error %))
         (st/realize-each)
         (st/map logger)
         (divert-stream-errors))))

(defn reduce-all-throw
  "reduce a stream, but if there are any errors will log
   exemplars and return an error-deferred with the first error. always
   reduces the entire stream, as if any errors were filtered"
  ([description f source]
   (let [[err out] (log-divert-stream-errors description source)
         rv-d (st/reduce f out)
         e-d (s-first ::none err)]
     (ddo [e e-d
           rv rv-d]
       (if (= ::none e)
         (return rv)
         (d/error-deferred (:error e))))))

  ([description f init source]
   (let [[err out] (log-divert-stream-errors description source)
         rv-d (st/reduce f init out)
         e-d (s-first ::none err)]
     (ddo [e e-d
           rv rv-d]
       (if (= ::none e)
         (return rv)
         (d/error-deferred (:error e)))))))

(defn close-on-closed
  "close a Closeable object (such as an InputStream) when the stream
   is closed"
  [closeable stream]
  (st/on-closed
   stream
   (fn []
     (warn "close-on-closed")
     (.close closeable)))
  stream)

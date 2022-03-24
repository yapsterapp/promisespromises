(ns prpr.stream.types
  (:require
   [prpr.stream.protocols :as pt]))

;; core.async channels don't support nil values,
;; but we would like clj and cljs to be as similar
;; as possible, so we'll wrap nils when we are
;; using core.async
(deftype StreamNil []
  pt/IStreamValue
  (-value [_] nil))

(defn stream-nil
  []
  (->StreamNil))

(defn stream-nil?
  [v]
  (instance? StreamNil v))

;; neither core.async nor manifold have error-states on
;; streams/chans - so we'll model errors by putting a
;; wrapped value onto a stream and closing it immediately
;; thereafter. whenever an error value is taken from a
;; stream it will result in an errored promise or a
;; downstream stream also getting a wrapped error-value/closed
(deftype StreamError [err]
  pt/IStreamError
  (-error [_] err)
  pt/IStreamValue
  (-value [_] (throw err)))

(defn stream-error?
  [v]
  (instance? StreamError v))

(defn stream-error
  [err]
  (if (stream-error? err)
    err
    (->StreamError err)))

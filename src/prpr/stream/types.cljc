(ns prpr.stream.types
  (:require
   [promesa.core :as pr]
   [prpr.stream.protocols :as pt]))

(extend-protocol pt/IStreamValue
  #?(:clj Object :cljs default)
  (-unwrap-value [this] this)

  nil
  (-unwrap-value [_this] nil))

;; core.async channels don't support nil values,
;; but we would like clj and cljs to be as similar
;; as possible, so we'll wrap nils when we are
;; using core.async
(deftype StreamNil []
  pt/IStreamValue
  (-unwrap-value [_] nil))

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
  (-unwrap-error [_] err)
  pt/IStreamValue
  (-unwrap-value [_]
    ;; (warn err "unwrapping StreamError" (ex-data err))
    (throw err)))

(defn stream-error?
  [v]
  (instance? StreamError v))

(defn stream-error
  [err]
  (if (stream-error? err)
    err
    (->StreamError err)))

(extend-protocol pt/IStreamChunk
  #?(:clj Object
     :cljs default)
  (-chunk-flatten [this] this)

  nil
  (-chunk-flatten [_] nil))

(declare ->StreamChunk)

(deftype StreamChunk [values]
  pt/IStreamChunk

  (-chunk-values [_] values)
  (-chunk-flatten [_]
    (pr/let [realized-values (pr/all values)]
      (->StreamChunk realized-values))))

(defn stream-chunk?
  [v]
  (instance? StreamChunk v))

(defn stream-chunk
  [values]
  (let [values (vec values)]
    (when (<= (count values) 0)
      (throw (ex-info "empty chunk not allowed" {})))
    (->StreamChunk values)))

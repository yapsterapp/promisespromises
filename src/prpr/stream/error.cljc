(ns prpr.stream.error
  (:require
   [prpr.stream.protocols :as pt]))

(deftype StreamError [err]
  pt/IStreamError
  (-error [_] err))

(defn stream-error?
  [v]
  (instance? StreamError v))

(defn stream-error
  [err]
  (if (stream-error? err)
    err
    (->StreamError err)))

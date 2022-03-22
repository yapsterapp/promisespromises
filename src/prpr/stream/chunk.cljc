(ns prpr.stream.chunk
  (:require
   [promesa.core :as promise]
   [prpr.stream.protocols :as pt]))

(extend-protocol pt/IStreamChunk
  #?(:clj Object
     :cljs default)
  (-flatten [this] this)

  nil
  (-flatten [_] nil))

(declare ->StreamChunk)

(deftype StreamChunk [values]
  pt/IStreamChunk

  (-chunk-values [_] values)
  (-flatten [_]
    (promise/let [realized-values (promise/all values)]
      (->StreamChunk realized-values))))

(defn stream-chunk?
  [v]
  (instance? StreamChunk v))

(defn stream-chunk
  [values]
  (when (<= (count values) 0)
    (throw (ex-info "empty chunk not allowed" {})))
  (->StreamChunk values))

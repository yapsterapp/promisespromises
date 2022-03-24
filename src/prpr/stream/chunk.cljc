(ns prpr.stream.chunk
  (:require
   #?(:clj [clojure.core :refer [print-method]])
   [promesa.core :as promise]
   [prpr.stream.protocols :as pt]))

(def default-chunk-size 1000)

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

(deftype StreamChunkBuilder [records-a]
  pt/IStreamChunkBuilder
  (-start-chunk [_]
    (when (some? @records-a)
      (throw (ex-info "chunk already building!" {:records-a @records-a})))
    (reset! records-a (transient [])))

  (-add-to-chunk [_ val]
    (when (nil? @records-a)
      (throw (ex-info "no chunk building!" {})))
    (swap! records-a conj! val))

  (-finish-chunk [_ rf result]
    (when (nil? @records-a)
      (throw (ex-info "no chunk building!" {})))
    (let [records (persistent! @records-a)]
      (reset! records-a nil)
      (if (not-empty records)
        (rf result (chunk records))
        result)))

  (-discard-chunk [_]
    (reset! records-a nil))

  (-building-chunk? [_] (some? @records-a))

  (-chunk-state [_] @records-a))

#?(:clj
   (defmethod print-method StreamChunkBuilder [x writer]
     (.write writer "#prpr.stream.ChunkBuilder<")
     (print-method (pt/-chunk-state x) writer)
     (.write writer ">")))

(defn stream-chunk-builder
  []
  (->StreamChunkBuilder (atom nil)))

(defn chunker-xform
  "a transducer which can build chunks"
  [chunk-builder rf]
  (fn
    ([] (rf))
    ([result]
     (if (pt/-building-chunk? chunk-builder)
       (throw (ex-info "finalisation while building a chunk!" {}))
       (rf result)))
    ([result input]
     (if (pt/-building-chunk? chunk-builder)
       (do
         (pt/-add-to-chunk chunk-builder input)
         result)
       (rf result input)))))

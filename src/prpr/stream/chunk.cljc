(ns prpr.stream.chunk
  (:require
   #?(:clj [clojure.core :refer [print-method]])
   [promesa.core :as pr]
   [prpr.stream.protocols :as pt]
   [prpr.stream.impl :as impl]
   [prpr.stream.types :as types]))

(def default-chunk-size 1000)

(deftype StreamChunkBuilder [records-a]
  pt/IStreamChunkBuilder
  (-start-chunk [_]
    (when (some? @records-a)
      (throw (ex-info "chunk already building!" {:records-a @records-a})))
    (reset! records-a (transient [])))

  (-start-chunk [_ val]
    (when (some? @records-a)
      (throw (ex-info "chunk already building!" {:records-a @records-a})))
    (reset! records-a (transient [val])))

  (-add-to-chunk [_ val]
    (when (nil? @records-a)
      (throw (ex-info "no chunk building!" {})))
    (swap! records-a conj! val))

  (-finish-chunk [_]
    (when (nil? @records-a)
      (throw (ex-info "no chunk building!" {})))
    (let [records (persistent! @records-a)]
      (reset! records-a nil)
      (types/stream-chunk records)))

  (-finish-chunk [_ val]
    (when (nil? @records-a)
      (throw (ex-info "no chunk building!" {})))
    (swap! records-a conj! val)
    (let [records (persistent! @records-a)]
      (reset! records-a nil)
      (types/stream-chunk records)))

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
         (pt/-add-to-chunk chunk-builder input)
         result)
     (rf result input))))


(defn dechunk
  "given a stream of mixed unchunked-values and chunks
   return a stream of
   [::unchunked|::chunk-start|::chunk|::chunk-end val]"
  [s]
  (let [s' (impl/stream)]
    (impl/connect-via
     s
     (fn [v]
       (cond
         (types/stream-chunk? v)
         (let [vals (pt/-chunk-values v)
               n (count vals)]
           (pr/chain
            (impl/put! s' [::chunk-start (first vals)])
            (fn [_]
              (when (> n 2)
                (impl/put-all!
                 s'
                 (-> vals
                     (subvec 1 (- n 2))
                     (as-> %
                         (map (fn [v]
                                [::chunk v])
                              %))))))
            (fn [_]
              (impl/put! s' [::chunk-end (last vals)]))))

         :else
         (impl/put! s' [::unchunked v])))
     s')))

(defn rechunk
  "given a stream of
   [::unchunked|::chunk-start|::chunk|::chunk-end val]
   return a stream of unchunked values and chunks"
  [s]
  (let [s' (impl/stream)
        chunk-builder-a (atom (stream-chunk-builder))]
    (impl/connect-via
     s
     (fn [[k v]]
       (condp = k

         ::chunk-start
         (do
           (pt/-start-chunk @chunk-builder-a v)
           true)

         ::chunk
         (do
           (pt/-add-to-chunk @chunk-builder-a v)
           true)

         ::chunk-end
         (let [chunk (pt/-finish-chunk @chunk-builder-a v)]
           (impl/put! s' chunk))


         ::unchunked
         (impl/put! s' v)))
     s')))

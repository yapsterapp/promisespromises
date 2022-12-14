(ns prpr.stream.chunk
  (:require
   #?(:clj [clojure.core :refer [print-method]])
   [promesa.core :as pr]
   [prpr.error :as err]
   [prpr.stream.protocols :as pt]
   [prpr.stream.impl :as impl]
   [prpr.stream.types :as types]))

(def default-chunk-size 1000)

(deftype StreamChunkBuilder [records-a]
  pt/IStreamChunkBuilder
  (-start-chunk [_]
    (when (some? @records-a)
      (throw (err/ex-info ::chunk-already-building {:records-a @records-a})))
    (reset! records-a (transient [])))

  (-start-chunk [_ val]
    (when (some? @records-a)
      (throw (err/ex-info ::chunk-already-building {:records-a @records-a})))
    (reset! records-a (transient [val])))

  (-add-to-chunk [_ val]
    (when (nil? @records-a)
      (throw (err/ex-info ::no-chunk-building {})))
    (swap! records-a conj! val))

  (-finish-chunk [_]
    (when (nil? @records-a)
      (throw (err/ex-info ::no-chunk-building {})))
    (let [records (persistent! @records-a)]
      (reset! records-a nil)
      (types/stream-chunk records)))

  (-finish-chunk [_ val]
    (when (nil? @records-a)
      (throw (err/ex-info ::no-chunk-building {})))
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

(defn should-finish-chunk?
  "true if building a chunk and:

   - (nil? partition-by) and (>= chunk-size target-chunk-size)
   - (some? partition-by-fn)
     and (>= chunk-size target-chunk-size)
     and (not= (partition-by-fn (last chunk-state) (partition-by next-value)))"

  [chunk-builder target-chunk-size partition-by-fn next-value]
  (if (pt/-building-chunk? chunk-builder)

    (or

     (and (nil? partition-by-fn)
          (>= (count (pt/-chunk-state chunk-builder))
              target-chunk-size))

     (let [ch-data(pt/-chunk-state chunk-builder)]
       (and (some? partition-by-fn)
            (>= (count ch-data)
                target-chunk-size)
            (not= (partition-by-fn (nth ch-data (-> ch-data count dec)))
                  (partition-by-fn next-value)))))

    false))

(defn make-chunker-xform
  "return a transducer which builds chunks from a stream, optionally
   partition-by the stream ensuring that partitions never span
   chunk boundaries

   NOTE that no timeout is possible with a transducer

   - target-chunk-size : will wrap a chunk when this size is exceeded,
       or as soon as possible afterwards (if a chunk is received, or
       partition-by is given)
   - partition-by-fn : also partition-by the stream with this fn and
       ensure partitions never cross chunk boundaries"
  ([target-chunk-size]
   (make-chunker-xform target-chunk-size nil))

  ([target-chunk-size partition-by-fn]
   (let [cb (stream-chunk-builder)]
     (fn [rf]
       (fn
         ([] (rf))

         ([result]
          (when (pt/-building-chunk? cb)
            (rf result (pt/-finish-chunk cb)))
          (rf result))

         ([result input]
          (if (pt/-building-chunk? cb)

            (if (should-finish-chunk? cb target-chunk-size partition-by-fn input)
              (do
                (let [ch (pt/-finish-chunk cb)]
                  (rf result (if (some? partition-by-fn)
                               (types/stream-chunk
                                (partition-by
                                 partition-by-fn
                                 (pt/-chunk-values ch)))
                               ch)))
                (pt/-start-chunk cb input))

              (pt/-add-to-chunk cb input))

            (pt/-start-chunk cb input))

          result))))))

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

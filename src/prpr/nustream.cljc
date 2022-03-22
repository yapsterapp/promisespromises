(ns prpr.nustream
  (:require
   [clojure.core :as clj]
   [prpr.stream.protocols :as pt]
   [prpr.stream.impl :as impl]
   [prpr.stream.error :as error]
   [prpr.stream.chunk :as chunk]
   [prpr.stream.consumer :as consumer]
   [promesa.core :as promise]
   #?(:clj [prpr.stream.manifold :as stream.manifold]))
  (:refer-clojure
    :exclude [map filter mapcat reductions reduce concat]))

;; manifold higher (map,reduce ops) are implemented
;; with put!, take! and connect... we can implement
;; put!, take! and connect for core.async
;;
;; sink for connect can be a Callback that fakes
;; being a sink... and feeds vals through a function
;;
;; core.async has pipeline and pipeline-async
;;
;; layer 0 - transport
;;           manifold/stream or core.async
;;
;;           simple graphs of streams. operations, with a stream
;;           interface
;;
;;           (put! sink val)
;;           (put! sink val timeout timeout-val)
;;
;;           (take! source)
;;           (take! source default-val)
;;           (take! source defaul-val timeout timeout-val)
;;
;;           (connect source sink close-timeout-opts)
;;
;;           (error! ex) - puts an error wrapper onto the
;;           stream transport and thereafter immediately
;;           closes it
;;
;; layer 1 - error propagation.
;;           - take! returns a promise, which is failed
;;           when the error wrapper is encountered
;;
;; layer 2 - chunking
;;           handle streams of chunks equivalently to
;;           streams of plain values - have a chunk interface
;;           for common ops ?
;;
;; layer 3 - ops
;;           map
;;           reduce
;;           transform
;;           realize

(def stream impl/stream)

(defn put!
  "put a value onto a stream with backpressure - returns
   Promise<true|false> which eventually resolves to:
    - true when the value was accepted onto the stream
    - false if the stream was closed"
  ([sink val]
   (put! sink val nil nil))
  ([sink val timeout timeout-val]
   (pt/-put! sink val timeout timeout-val)))

(defn take!
  "take a value from a stream - returns Promise<value|error>
   which evantually resolves to:
   - a value when one becomes available
   - nil or default-val if the stream closes
   - timeout-val if no value becomes available in timeout ms
   - an error if the stream errored (i.e. an error occurred
     during some upstream operation)"
  ([source]
   (take! source nil nil nil))
  ([source default-val]
   (take! source default-val nil nil))
  ([source default-val timeout timeout-val]
   (->
    (pt/-take! source default-val timeout timeout-val)
    (promise/chain
     (fn [v]
       (if (error/stream-error? v)
         (throw (pt/-error v))
         v))))))

(defn connect-via
  ([source f sink]
   (pt/-connect-via source f sink nil))
  ([source f sink opts]
   (pt/-connect-via source f sink opts)))

(defn realize-each
  "convert a Stream<Promise<val>|val> into Stream<val>"
  [s]
  (let [s' (impl/stream)]
    (connect-via
     s
     (fn [v]
       (cond

         (promise/promise? v)
         (promise/chain
          v
          #(put! s' %))

         (chunk/stream-chunk? v)
         (promise/chain
          (pt/-flatten v)
          #(put! s' %))

         :else
         (put! s' v)))
     s')))

(defn transform
  "apply transform to a stream"
  ([xf s])
  ([xf buffer-size s]))

(declare zip)

(defn map
  "(map f Stream<val>) -> Stream<(f val)>"
  ([f s]
   (let [s' (impl/stream)]
     (connect-via
      s
      (fn [v]
        (cond
          (error/stream-error? v)
          (pt/-error! s' v)

          (chunk/stream-chunk? v)
          (pt/-put!
           s'
           (chunk/stream-chunk
            (mapv f (pt/-chunk-values v))))

          :else
          (pt/-put! s' (f v))))
      s')
     s'))

  ([f s & rest]
   (->> (apply consumer/chunk-zip s rest)
        (map #(apply f %)))))

(defn zip
  ([a] (map vector a))
  ([a & rest]
   (apply consumer/chunk-zip a rest)))

(defn mapcat
  ([f s])
  ([f s & rest]))

(defn concat
  [s])

(defn reductions
  ([f s])
  ([f initial-val s]))

(defn reduce
  ([f s])
  ([f initial-val s]))

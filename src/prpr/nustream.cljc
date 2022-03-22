(ns prpr.nustream
  (:require
   [clojure.core :as clj]
   [prpr.stream.protocols :as pt]
   [prpr.stream.impl :as impl]
   [prpr.stream.error :as error]
   [prpr.stream.chunk :as chunk]
   [prpr.stream.consumer :as consumer]
   [promesa.core :as pr]
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

(defn close!
  [s]
  (pt/-close! s))

(defn put!
  "put a value onto a stream with backpressure - returns
   Promise<true|false> which eventually resolves to:
    - true when the value was accepted onto the stream
    - false if the stream was closed"
  ([sink val]
   (pt/-put! sink val))
  ([sink val timeout timeout-val]
   (pt/-put! sink val timeout timeout-val)))

(defn error!
  "mark a stream as errored

  puts an marker wrapper with the error on to the stream,
  and then closes it. consuming fns will throw an error
  when they encounter it, so errors are always propagated"
  [sink err]
  (pt/-error! sink err))

(defn put-all!
  "put all values onto a stream with backpressure
   returns Promise<true|false> yielding true if all
   values were accepted onto the stream, false otherwise"
  [sink vals]
  (pr/loop [vals vals]
    (if (empty? vals)
      true
      (pr/chain
       (pt/-put! sink (first vals))
       (fn [result]
         (if result
           (pr/recur (rest vals))
           false))))))

(defn throw-if-error
  [v]
  (if (error/stream-error? v)
    (throw (pt/-error v))
    v))

(defn take!
  "take a value from a stream - returns Promise<value|error>
   which evantually resolves to:
   - a value when one becomes available
   - nil or default-val if the stream closes
   - timeout-val if no value becomes available in timeout ms
   - an error if the stream errored (i.e. an error occurred
     during some upstream operation)"
  ([source]
   (pr/chain
    (pt/-take! source)
    throw-if-error))
  ([source default-val]
   (pr/chain
    (pt/-take! source default-val)
    throw-if-error))
  ([source default-val timeout timeout-val]
   (pr/chain
    (pt/-take! source default-val timeout timeout-val)
    throw-if-error)))

(defn connect-via
  "feed all messages from src into callback on the
   understanding that they will eventually propagate into
   dst

   the return value of callback should be a promise yielding
   either true or false. when false the downstream sink
   is assumed to be closed and the connection is severed"
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

         (pr/promise? v)
         (pr/chain
          v
          #(put! s' %))

         (chunk/stream-chunk? v)
         (pr/chain
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
          (put!
           s'
           (chunk/stream-chunk
            (mapv f (pt/-chunk-values v))))

          :else
          (put! s' (f v))))
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
  ([f s]
   (let [s' (impl/stream)]
     (connect-via
      s
      (fn [v]
        (cond
          (error/stream-error? v)
          (error! s' v)

          (chunk/stream-chunk? v)
          (put-all!
           s'
           (chunk/stream-chunk
            (mapcat f (pt/-chunk-values v))))

          :else
          (put-all! s' (f v))))
      s')
     s'))
  ([f s & rest]
   (->> (apply consumer/chunk-zip s rest)
        (mapcat #(apply f %)))))

(defn concat
  [s]
  )

(defn filter
  [pred s]
  (let [s' (impl/stream)]
    (connect-via
     s
     (fn [v]
       (if (pred v)
         (put! s' v)
         true))
     s')))

(defn reductions
  ([f s])
  ([f initial-val s]))

(defn rreduce
  "alt-version of seq-reduce which returns any
   reduced value still in its wrapper, which is
   helpful for supporting reduction of chunks"
  ([f coll]
   (if-let [s (seq coll)]
     (rreduce f (first s) (next s))
     (f)))
  ([f val coll]
   (loop [val val, coll (seq coll)]
     (if coll
       (let [nval (f val (first coll))]
         (if (reduced? nval)
           nval
           (recur nval (next coll))))
       val))))

(defn reduce
  "reduce, but for streams. returns a Promise of the reduced value

   the reducing function is *not* async - it must return a plain
   value and not a promise"
  ([f s]
   (reduce f ::none s))
  ([f initial-val s]
   (-> (if (identical? ::none initial-val)
         (take! s ::none)
         (pr/promise initial-val))

       (pr/chain
        (fn [initial-val]
          (cond

            (identical? ::none initial-val)
            (f)

            (error/stream-error? initial-val)
            (throw (pt/-error initial-val))

            :else
            (pr/loop [val initial-val]
              (let [val (if (chunk/stream-chunk? initial-val)
                          (rreduce f (pt/-chunk-values initial-val))
                          val)]

                (if (reduced? val)
                  (deref val)

                  (-> (take! s ::none)
                      (pr/chain (fn [x]
                                  (cond

                                    (identical? ::none x) val

                                    (error/stream-error? x)
                                    (throw (pt/-error x))

                                    (chunk/stream-chunk? x)
                                    (let [r (rreduce f val (pt/-chunk-values x))]
                                      (if (reduced? r)
                                        (deref r)
                                        (pr/recur r)))

                                    :else
                                    (let [r (f val x)]
                                      (if (reduced? r)
                                        (deref r)
                                        (pr/recur r))))))))))))))))

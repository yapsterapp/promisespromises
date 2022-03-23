(ns prpr.nustream
  (:require
   [prpr.stream.protocols :as pt]
   [prpr.stream.impl :as impl]
   [prpr.stream.error :as error]
   [prpr.stream.chunk :as chunk]
   [prpr.stream.consumer :as consumer]
   [promesa.core :as pr]
   [taoensso.timbre :refer [error]]
   [clojure.core :as clj])
  (:refer-clojure
    :exclude [map filter mapcat reductions reduce concat]))

;; manifold's stream API (map,filter,transform,reduce ops) is
;; implemented with put!, take! and connect...
;;
;; we have had good success in rewriting the top-level stream API
;; to propagate errors on clj, but we can go further...
;;
;; and add chunking support. we can also implement put!, take!
;; and connect for core.async and potentially get a cross-platform
;; streams lib
;;
;; so the idea is to use manifold streams and core.async
;; channels as low-level message transports with
;; backpressure, and then to add
;;
;; 1. error-propagation
;;    any errors in applying map/filter/reduce fns to stream values
;;    are wrapped in a marker and propagated downstream,
;;    thereafter immediately closing the downwards stream. take! then
;;    turns any error marker into an errored promise and error
;;    propagation happens
;; 2. (mostly) transparent chunking
;;    any chunks on a stream are transparently processed as if
;;    they were values on the stream
;;
;; to implementations of (roughly) the manifold streams
;; API - map / filter / transform / reduce / realize-each with a
;; similar promise-based to manifold. we're using promesa rather
;; than manifold's own deferred because it's cross-platform
;;
;; layer 0 - transport
;;           manifold/stream.clj || core/async.cljs
;;
;;           graphs of streams with a promise-based interaction
;;           interface
;;
;;           (put! sink val) -> Promise<true|false>
;;           (put! sink val timeout timeout-val) -> Promise<true|false|timeout-val>
;;
;;           (take! source) -> Promise<val|nil>
;;           (take! source default-val) -> Promise<val|default-val>
;;           (take! source defaul-val timeout timeout-val) ->
;;             Promise<val|default-val|timeout-val>
;;
;;           (connect-via source f sink close-timeout-opts)
;;           feed messages from source into function f on the
;;           understanding that they will propagate eventually
;;           to sink (it's up to f to do this)
;;
;;           (error! ex) - puts an error wrapper onto the
;;           stream transport and thereafter immediately
;;           closes it
;;
;; layer 1 - higher level ops - propagate errors and
;;           handle chunks transparently and present a similar
;;           promise-based interface to manifold
;;           map
;;           zip
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

(defn throw-errors-preserve-reduced
  "wrap a reducing fn to reduce chunks
   - any StreamErrors in the chunk will be immediately thrown
   - a reduced value will be wrapped in another reduced, so that
     it gets returned to the outer reduction
   (modelled on clojure.core/preserving-reduced)"
  [rf]
  (fn [result input]
    (if (error/stream-error? input)
      (throw (pt/-error input))
      (let [r (rf result input)]
        (if (reduced? r)
          (reduced r)
          r)))))

(defn stream-error-capturing-stream-xform
  "Returns a transducing xform that wraps the given `xform` but captures errors
  raised when invoking any arity and passes them to the upstream `xf` fn wrapped
  in `StreamError` markers"
  [xform]
  (let [cb (chunk/stream-chunk-builder)
        c-xform (partial chunk/chunker-xform cb)
        xform (comp xform c-xform)]
    (fn [xf]
      (let [xff (xform xf)]
        (fn
          ([]
           (try
             (xff)
             (catch Throwable e
               (xf (xf) (error/stream-error e)))))
          ([rs]
           (try
             (xff rs)
             (catch Throwable e
               (xf (xf rs (error/stream-error e))))))
          ([rs msg]
           (cond
             (error/stream-error? msg)
             (xf rs msg)

             (chunk/stream-chunk? msg)
             (try
               (pt/-start-chunk cb)
               (let [_ (clojure.core/reduce
                          (throw-errors-preserve-reduced xff)
                          rs
                          (pt/-chunk-values msg))]
                 (pt/-finish-chunk cb xf rs))
               (catch Throwable e
                 (xf rs (error/stream-error e)))
               (finally
                 (pt/-discard-chunk cb)))

             :else
             (try
               (xff rs msg)
               (catch Throwable e
                 (xf rs (error/stream-error e)))))))))))

(defn transform
  "apply transform to a stream"
  ([xform s]
   (transform xform 0 s))
  ([xform buffer-size s]
   (let [s' (stream buffer-size xform)]
     (connect-via s #(put! s' %) s'))))

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

(defn filter
  "TODO add error and chunk support"
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
  "like clojure.core/reductions but for streams

   TODO add error and chunk support"
  ([f s]
   (reductions f ::none s))
  ([f initial-val s]
   (let [s' (impl/stream)
         val (atom initial-val)]
     (pr/chain
      (if (identical? ::none initial-val)
        true
        (put! s' initial-val))

      (fn [_]
        (connect-via
         s
         (fn [v]
           (if (identical? ::none @val)
             (do
               (reset! val v)
               (put! s' v))

             (-> v
                 (pr/chain
                  #(f @val %)
                  (fn [x]
                    (if (reduced? x)
                      (do
                        (reset! val @x)
                        (put! s' @x)
                        false)
                      (do
                        (reset! val x)
                        (put! s' x)))))
                 (pr/catch
                     (fn [e]
                       (error! s' e)
                       false)))))
         s'))))))

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
                          (clj/reduce
                           (@#'clj/preserving-reduced f)
                           (pt/-chunk-values initial-val))
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
                                    (let [r (clj/reduce
                                             (@#'clj/preserving-reduced f)
                                             val
                                             (pt/-chunk-values x))]
                                      (if (reduced? r)
                                        (deref r)
                                        (pr/recur r)))

                                    :else
                                    (let [r (f val x)]
                                      (if (reduced? r)
                                        (deref r)
                                        (pr/recur r))))))))))))))))

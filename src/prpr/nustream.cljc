(ns prpr.nustream
  (:refer-clojure :exclude [map mapcat filter reductions reduce])
  (:require
   [prpr.stream.protocols :as pt]
   [prpr.stream.impl :as impl]
   [prpr.stream.types :as types]
   [prpr.stream.chunk :as chunk]
   [prpr.stream.concurrency :as concurrency]
   [prpr.stream.consumer :as consumer]
   [promesa.core :as pr]
   [taoensso.timbre :refer [error]]
   [clojure.core :as clj])
  (:refer-clojure
    :exclude [map filter mapcat reductions reduce concat]))

;; differences between manifold and core.async
;;
;; - put!/take! timeouts
;;   - in manifold, timeouts cancel the operation. in core.async they
;;     don't

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
(def stream? impl/stream?)
(def close! impl/close!)
(def put! impl/put!)
(def error! impl/error!)
(def put-all! impl/put-all!)
(def take! impl/take!)
(def connect-via impl/connect-via)

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

         (types/stream-chunk? v)
         (pr/chain
          (pt/-chunk-flatten v)
          #(put! s' %))

         :else
         (put! s' v)))
     s')

    s'))

(defn throw-errors-preserve-reduced
  "wrap a reducing fn to reduce chunks
   - any StreamErrors in the chunk will be immediately thrown
   - a reduced value will be wrapped in another reduced, so that
     it gets returned to the outer reduction
   (modelled on clojure.core/preserving-reduced)"
  [rf]
  (fn [result input]
    (if (types/stream-error? input)
      (throw (pt/-unwrap-error input))
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
    (fn [rf]
      (let [rff (xform rf)]
        (fn
          ([]
           (try
             (rff)
             (catch #?(:clj Throwable :cljs :default) e
               ;; init with the underlying rf and
               ;; then immediately call the reduce arity with
               ;; the StreamError
               (rf (rf) (types/stream-error e)))))
          ([rs]
           (try
             (rff rs)
             (catch #?(:clj Throwable :cljs :default) e
               ;; first call the reduce arity of the rf
               ;; with the StreamError, and then finalize
               (rf (rf rs (types/stream-error e))))))
          ([rs msg]
           (cond
             (types/stream-error? msg)
             (rf rs msg)

             (types/stream-chunk? msg)
             (try
               (pt/-start-chunk cb)
               (let [_ (clojure.core/reduce
                          (throw-errors-preserve-reduced rff)
                          rs
                          (pt/-chunk-values msg))
                     chunk (pt/-finish-chunk cb)]
                 (if (not-empty (pt/-chunk-values chunk))
                   (rf rs chunk)
                   rs))
               (catch #?(:clj Throwable :cljs :default) e
                 (rf rs (types/stream-error e)))
               (finally
                 (pt/-discard-chunk cb)))

             :else
             (try
               (rff rs msg)
               (catch #?(:clj Throwable :cljs :default) e
                 (rf rs (types/stream-error e)))))))))))

(defn transform
  "apply transform to a stream

  TODO
   as it stands the modified transform will put all reducing function
   errors on to the result stream, but will not error the result stream.
   possible solutions...

   - pass the result stream to the xform so that it can error the stream...
   - change to use an intermediate with the transform, and downstream of
    the intermediate, error the result stream at the first error and
    close the intermediate - necessary because the xform wrapper can't
    terminate the result stream itself, it can "
  ([xform s]
   (transform xform 0 s))
  ([xform buffer-size s]
   (let [s' (stream buffer-size (stream-error-capturing-stream-xform xform))]
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
          (types/stream-error? v)
          (impl/error! s' v)

          (types/stream-chunk? v)
          (put!
           s'
           (types/stream-chunk
            (mapv f (pt/-chunk-values v))))

          :else
          (put! s' (f v))))
      s')
     s'))

  ([f s & rest]
   (->> (apply consumer/chunk-zip s rest)
        (map #(apply f %)))))

;; alternative implementation strategy, since a concurrency
;; limited fn isn't much faster than a stream...
;; (3s vs 7s for 1M messages - with simple optimisation to use
;; mutable types. it was 19s with atoms) at least,
;; not without putting lots of optimisation effort in
;;
;; stream values to an intermediate steam with no chunks, but
;; retaining chunking info
;; [::unchunked|::chunk-start|::chunk|::chunk-end val]
;; then use buffers to enforce concurrency, do a regular
;; map and rechunk after the map
;;
;; this is vanilla stream-processing function, so less likely
;; to have bugs than a concurrency limited function, and
;; sorting out disposal on the concurrency limited function
;; was also going to be difficult
(defn mapcon
  "like map, but limits the number of concurrent unresolved
   promises from application of f

   - f is a promise-returning async fn. the result promise
   of f will be resolved and the resolved result placed on
   the output.
   - n is the maximum number of simultaneous unresolved
   promises

   this works to control concurrency even when chunks are
   used - because using buffering to control concurrency
   no longer works when each buffered value can be a chunk
   or arbitrary size"
  ([f n s]
   (let [dechunked-f (fn [[chunk-k v]]
                       [chunk-k (f v)])]
     (->> s
          (chunk/dechunk)
          (map dechunked-f)
          (pt/-buffer n)
          (chunk/rechunk)))
   (map (concurrency/concurrency-limited-fn f n) s))
  ([f n s & rest]
   (let [dechunked-f (fn [[chunk-k v]]
                       [chunk-k (f v)])]
     (->> s
          (chunk/dechunk)
          (apply map dechunked-f s rest)
          (pt/-buffer n)
          (chunk/rechunk)))))

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
          (types/stream-error? v)
          (error! s' v)

          (types/stream-chunk? v)
          (put-all!
           s'
           (types/stream-chunk
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
         (pr/resolved initial-val))

       (pr/chain
        (fn [initial-val]
          (cond

            (identical? ::none initial-val)
            (f)

            (types/stream-error? initial-val)
            (throw (pt/-unwrap-error initial-val))

            :else
            (pr/loop [val initial-val]
              (let [val (if (types/stream-chunk? initial-val)
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

                                    (types/stream-error? x)
                                    (throw (pt/-unwrap-error x))

                                    (types/stream-chunk? x)
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

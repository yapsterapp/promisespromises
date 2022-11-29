(ns prpr.nustream
  (:refer-clojure :exclude [map mapcat filter reductions reduce])
  (:require
   [prpr.stream.protocols :as pt]
   [prpr.stream.impl :as impl]
   [prpr.stream.types :as types]
   [prpr.stream.chunk :as chunk]
   ;; [prpr.stream.concurrency :as concurrency]
   [prpr.stream.consumer :as consumer]
   [promesa.core :as pr]
   [clojure.core :as clj])
  (:refer-clojure
    :exclude [map filter mapcat reductions reduce concat]))

;; a clj+cljs cross-platform streams lib, in the style of manifold
;;
;; the idea is to use manifold streams (on clj) and core.async channels
;; (on cljs) as low-level errorless message transports with backpressure,
;; and then to add on top of that:
;;
;; 1. error capture and propagation
;;    any errors in applying transform/map/filter/reduce fns to stream values
;;    are wrapped in a marker and propagated downstream,
;;    thereafter immediately closing the downstream. take! and reduce
;;    turn any error marker into an errored promise and thus error
;;    propagation happens
;; 2. (mostly) transparent chunking
;;    chunks on a stream are transparently processed as if
;;    the values in the chunk were values in the stream
;; 3. a consistent API across clj + cljs
;;    the API is a manifold-like streams and promises API, but it uses
;;    promesa for the promises, (instead of manifold's deferreds) - so
;;    we get the same API across clj+cljs
;;
;; there are currently some API differences between clj + cljs
;;
;; - put!/take! timeouts
;;   - in manifold, timeouts cancel the operation. in core.async they
;;     don't

(def stream impl/stream)
(def stream? impl/stream?)
(def close! impl/close!)
(def put! impl/put!)
(def error! impl/error!)

(defn put-all!
  "puts all values onto a stream - first flattens any chunks from
   the vals, and creates a new chunk, then puts the chunk on the
   stream"
  [sink vals]
  (let [flat-vals (clojure.core/reduce
                   (fn [a v]
                     (if (types/stream-chunk? v)
                       (into a (pt/-chunk-values v))
                       (conj a v)))
                   []
                   vals)
        ch (types/stream-chunk flat-vals)]

    (impl/put! sink ch)))

(defn ->source
  "turns a collection into a stream
   (with the collection as a chunk on the stream), otherwise does nothing
   to a stream"
  [stream-or-coll]
  (if (impl/stream? stream-or-coll)
    stream-or-coll
    (let [s (impl/stream 1)]

      (pr/let [_ (put-all! s stream-or-coll)]
        (impl/close! s))

      s)))

;; NOTE take! API would ideally not return chunks, but it curently does...
;; don't currently have a good way of using a consumer/ChunkConsumer
;; in the public API, since i don't really want to wrap the underlying stream
;; or channel in something else
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

(defn safe-chunk-xform
  "- xform : a transducer
   - out : the eventual output stream

   returns a transducer which, in normal operation,  unrolls chunks and
   composes with xform. if any exception is thrown it immediately
   errors the eventual output stream with the error, and then rethrows
   the error (since there is no sensible value to return)"
  [xform out]
  (fn [out-rf]
    (let [rf (xform out-rf)]
      (fn
        ([] (try
              (rf)
              (catch #?(:clj Throwable :cljs :default) e
                (impl/error! out e)
                (throw e))))

        ([rs] (try
                (rf rs)
                (catch #?(:clj Throwable :cljs :default) e
                  (impl/error! out e)
                  (throw e))))

        ([rs v] (if (types/stream-chunk? v)
                  (try
                    (let [chunk-vals (pt/-chunk-values v)

                          ;; chunks cannot be empty!
                          chunk-vals-count (count chunk-vals)

                          chunk-vals-but-last (->> chunk-vals
                                                   (take (dec chunk-vals-count)))
                          chunk-vals-last (->> chunk-vals
                                               (drop (dec chunk-vals-count))
                                               first)

                          rs' (clojure.core/reduce rf rs chunk-vals-but-last)]
                      (rf rs' chunk-vals-last))

                    (catch #?(:clj Throwable :cljs :default) e
                      (impl/error! out e)
                      (throw e)))

                  (try
                    (rf rs v)
                    (catch #?(:clj Throwable :cljs :default) e
                      (impl/error! out e)
                      (throw e)))))))))


(defn transform
  "apply transform to a stream, returning a transformed stream

   uses the underlying manifold or core.async feature to transform
   a stream, but wraps the xform in a safe-chunk-xform which

   - unrolls chunks for the xform
   - if the xform throws an exception then immediately errors the returned
     stream with the exception

   NOTE connect-via error-handling doesn't work with xform errors, because
   the error doesn't happen in the connect-via fn, but rather in the
   manifold/core.async impl, and manifold's (at least) put! impl swallows the
   error, so connect-via sees no error. sidestepping this problem with
   the safe-chunk-xform and erroring the returned stream directly propagates the
   error, but also leads to some transformed values before the error going
   missing from the downstream, because of implicit buffering. i can't
   see an alternative impl atm, but i've also never used exceptions for
   non-exceptional control flow for streams, so i don't think it's a big
   problem"
  ([xform s]
   (transform xform 0 s))
  ([xform buffer-size s]
   (let [out (stream)
         s' (stream buffer-size (safe-chunk-xform xform out))]
     (connect-via s #(put! s' %) s')
     (connect-via s' #(put! out %) out)

     out)))

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
   or arbitrary size

   note that using this fn may have performance
   implications - it dechunks and rechunks"
  ([f n s]
   (let [dechunked-f (fn [[chunk-k v]]
                       [chunk-k (f v)])]
     (->> s
          (chunk/dechunk)
          (map dechunked-f)
          (pt/-buffer (dec n))
          (chunk/rechunk))))
  ([f n s & rest]
   (let [dechunked-f (fn [[chunk-k v]]
                       [chunk-k (f v)])]
     (->> s
          (chunk/dechunk)
          (apply map dechunked-f s rest)
          (pt/-buffer (dec n))
          (chunk/rechunk)))))

(defn zip
  "zip streams
     S<a> S<b> ... -> S<[a b ...]>

   the output stream will terminate with the first input stream
   which terminates"
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
  [pred s]
  (let [s' (impl/stream)]
    (connect-via
     s
     (fn [v]
       (cond
          (types/stream-error? v)
          (error! s' v)

          (types/stream-chunk? v)
          (let [fchunk (filter pred (pt/-chunk-values v))]
            (if (not-empty fchunk)
              (put! s' (types/stream-chunk fchunk))
              true))

          :else
          (if (pred v)
            (put! s' v)
            true)))
     s')))

(defn reduce-ex-info
  "extend cause ex-data with a reduce-id, or wrap cause in an ex-info"
  [id cause]
  (let [xd (ex-data cause)
        xm (ex-message cause)]

    (if (nil? xd)

      (ex-info "reduce error" {::reduce-id id} cause)

      (ex-info
       (or xm "reduce error")
       (assoc xd
              ::reduce-id id)
       cause))))

(defn reductions
  "like clojure.core/reductions, but for streams"
  ([id f s]
   (reductions id f ::none s))
  ([id f initial-val s]
   (let [s' (impl/stream)
         acc (atom initial-val)]

     (pr/chain
      (if (identical? ::none initial-val)
        true
        (put! s' initial-val))

      (fn [_]
        (connect-via
         s
         (fn [v]
           (if (identical? ::none @acc)

             (let [v (if (types/stream-chunk? v)
                       (clj/reduce
                        (@#'clj/preserving-reduced f)
                        (pt/-chunk-values v))
                       v)]

               (reset! acc v)

               (let [put-r (put! s' v)]
                 (if (reduced? v)
                   false
                   put-r)))

             (-> v
                 (pr/chain

                  (fn [v]
                    (let [v (if (types/stream-chunk? v)
                              (clj/reduce
                               (@#'clj/preserving-reduced f)
                               (pt/-chunk-values v))
                              v)]

                      (f @acc v)))

                  (fn [x]
                    (if (reduced? x)
                      (do
                        (reset! acc @x)
                        (pr/let [_p-r (put! s' @x)]
                          (close! s'))
                        false)
                      (do
                        (reset! acc x)
                        (put! s' x)))))
                 (pr/catch
                     (fn [e]
                       (error! s' (reduce-ex-info id e))
                       false)))))
         s')))

     s')))

(defn reduce
  "reduce, but for streams. returns a Promise of the reduced value

   NOTE the reducing function is not expected to be async - if it
   returns a promise then the promise will *not* be unwrapped, and
   unexpected things will probably happen"
  ([id f s]
   (reduce id f ::none s))
  ([id f initial-val s]
   (-> (if (identical? ::none initial-val)
         (take! s ::none)
         (pr/resolved initial-val))

       (pr/chain
        (fn [initial-val]
          ;; (prn "initial-val" initial-val)

          (cond

            (identical? ::none initial-val)
            (f)

            (types/stream-error? initial-val)
            (throw
             (pt/-unwrap-error initial-val))

            :else
            (let [;; errors in the loop binding get swallowed, so
                  ;; reduce any initial-val chunk before binding
                  initial-val (if (types/stream-chunk? initial-val)

                                (do
                                  (clj/reduce
                                   (@#'clj/preserving-reduced f)
                                   (pt/-chunk-values initial-val)))

                                initial-val)]

              #_{:clj-kondo/ignore [:loop-without-recur]}
              (pr/loop [val initial-val]

                ;; (prn "loop" val)

                (if (reduced? val)
                  (deref val)

                  (-> (take! s ::none)
                      (pr/chain (fn [x]
                                  ;; (prn "take!" val x)
                                  (cond

                                    (identical? ::none x) val

                                    (types/stream-error? x)
                                    (throw
                                     (pt/-unwrap-error x))

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
                                        (pr/recur r)))))))))))))
       (pr/catch
           (fn [e]
             (throw
              (reduce-ex-info id e)))))))

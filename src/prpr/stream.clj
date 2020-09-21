(ns prpr.stream
  (:refer-clojure
   :exclude [concat filter map mapcat reduce reductions transform])
  (:require
   [clojure.core :as clj]
   [manifold.stream :as st]
   [manifold.deferred :as d]
   [cats.core :as monad :refer [return]]
   [cats.labs.manifold :refer [deferred-context]]
   [potemkin :refer [import-vars]]
   [prpr.promise :as pr :refer [ddo]]
   [taoensso.timbre :refer [info warn]])
  (:import
   [manifold.stream Callback]))

(import-vars
 [manifold.stream
  sinkable?
  sourceable?
  ->sink
  ->source
  connect
  source-only
  sink-only
  onto
  stream?
  source?
  sink?
  description
  downstream
  weak-handle
  synchronous?
  close!
  closed?
  on-closed
  drained?
  on-drained
  put!
  put-all!
  try-put!
  take!
  try-take!
  stream
  stream*
  splice
  consume
  consume-async
  connect-via
  connect-via-proxy
  drain-into
  periodically
  zip
  reductions
  lazily-partition-by
  concat
  buffered-stream
  buffer
  batch
  throttle])

;; a marker for errors on a stream
(defrecord StreamError [error])

(defn stream-error?
  [v]
  (instance? StreamError v))

(defmacro catch-stream-error
  "catch any errors and put them in a StreamError marker"
  [& body]
  ;; (warn "catch-stream-error: wrapped")
  `(prpr.promise/catch
       (fn [err#]
         ;; (warn "catch-stream-error: caught")
         (->StreamError err#))
       ~@body))

;; safer StreamError marker producing versions of standard stream ops
(defn realize-each
  "Takes a stream of potentially deferred values, and returns a stream of
  realized values capturing and wrapping any deferred errors in StreamError
  markers."
  [s]
  (let [s' (st/stream)]
    (st/connect-via
     s
     (fn [msg]
       (-> msg
           (d/chain' #(st/put! s' %))
           (d/catch' #(st/put! s' (->StreamError %)))))
     s'
     {:description {:op "realize-each"}})
    (st/source-only s')))

(defn map
  "Equivalent to Manifold's `map` for single streams but with any errors caught
  and added to the stream as StreamErrors"
  [f s]
  (st/map
   (fn [msg]
     (if (stream-error? msg)
       msg
       (try
         (let [x (f msg)]
           (if (d/deferred? x)
             (d/catch x #(->StreamError %))
             x))
         (catch Throwable e
           (->StreamError e)))))
   s))

(defn mapcat
  "Equivalent to Manifold's `mapcat` but with errors caught and added to the
  output stream as StreamErrors

  Note: just as with Manifold's `mapcat` the mapping `f`n must be a pure fn that
  returns a non-deferred value (though the returned collection _can_ contain
  deferred values.)"
  [f s]
  (st/mapcat
   (fn [msg]
     (if (stream-error? msg)
       [msg]
       (try
         (f msg)
         (catch Throwable e
           [(->StreamError e)]))))
   s))

(defn filter
  "Equivalent to Manifold's `filter` but with errors caught and added to the
  stream as StreamErrors

  Note: just as with Manifold's `filter` the `pred` must be a _pure_ fn
  returning a non-deferred value"
  ;; unlike `map` and `mapcat` the implementation here is a straight-up copy
  ;; with modification from Manifold as the alternative seemed to be a slightly
  ;; less performant and more confusing pipeline of map/filter/map operations
  [pred s]
  (let [safe-pred (fn [msg]
                    (if (stream-error? msg)
                      [nil msg]
                      (let [ev (try
                                 (pred msg)
                                 (catch Throwable e
                                   (->StreamError e)))]
                        (if (stream-error? ev)
                          [nil ev]
                          [ev nil]))))
        xs (st/stream)]
    (st/connect-via
     s
     (fn [msg]
       (let [[t e] (safe-pred msg)]
         (case [(boolean t) (some? e)]
           [false true]  (st/put! xs e)
           [true  false] (st/put! xs msg)
           [false false] (d/success-deferred true))))
     xs
     {:description {:op "filter"}})
    (st/source-only xs)))

(defn stream-error-capturing-stream-xform
  "Returns a transducing xform that wraps the given `xform` but captures errors
  raised when invoking any arity and passes them to the upstream `xf` fn wrapped
  in `StreamError` markers"
  [xform]
  (fn [xf]
    (let [xff (xform xf)]
      (fn
        ([]
         (try
           (xff)
           (catch Throwable e
             (xf (xf) (->StreamError e)))))
        ([rs]
         (try
           (xff rs)
           (catch Throwable e
             (xf (xf rs (->StreamError e))))))
        ([rs msg]
         (if (stream-error? msg)
           (xf rs msg)
           (try
             (xff rs msg)
             (catch Throwable e
               (xf rs (->StreamError e))))))))))

(defn transform
  "Equivalent to Manifold's `transform` but with errors caught and added to the
  stream as StreamErrors"
  ([xform s]
   (transform
    xform
    0
    s))
  ([xform buffer-size s]
   (st/transform
    (stream-error-capturing-stream-xform xform)
    buffer-size
    s)))

;; utility fns for handling StreamErrors
(defn log-stream-error-exemplars
  [description n source]
  (let [error-count-atom (atom 0)]
    ;; although we don't log them all, we print a total error count
    (st/on-drained
     source
     (fn []
       (when (> @error-count-atom 0)
         (warn description "total error-count:" @error-count-atom))))

    ;; log only the first n errors encountered
    (fn [v]
      (when (and (stream-error? v)
                 (<= (swap! error-count-atom inc) n))
        (warn "got a StreamError")
        (warn (:error v) description))
      v)))

(defn filter-log-stream-errors
  "log 2 exemplars of StreamErrors on the source, and then filter them out"
  [description source]
  ;; only log 2 error exemplars - just to show if they are the same really
  (let [logger (log-stream-error-exemplars description 2 source)]
    (->> source
         (st/map #(catch-stream-error %))
         (st/realize-each)
         (st/map logger)
         (st/filter #(not (stream-error? %))))))

(defn not-stream-error
  "if v is a stream-error, log it and return nil, otherwise return v"
  [description v]
  (if (stream-error? v)
    (do (warn (:error v) description)
        nil)
    v))

(defn reduce-catch
  "reduce, but with any errors caught and exemplars logged"
  ([description f source]
   (->> source
        (filter-log-stream-errors description)
        (st/reduce f)))
  ([description f init source]
   (->> source
        (filter-log-stream-errors description)
        (st/reduce f init))))

(defn s-first
  "consume a stream completely, returning a Deferred of the first value from
   the stream, if any. returns Deferred<no-val> if the stream is empty"
  ([source]
   (s-first ::none source))
  ([no-val source]
   (st/reduce (fn [fv v]
                (if (= no-val fv)
                  v
                  fv))
              no-val
              source)))

(defn divert-stream-errors
  "kinda awkward - adapted from connect-via... takes src and
   returns [err dst]. feeds StreamErrors from src to err, and any
   other values to dst. closes err and dst when src is exhausted"
  ([src]
   (let [src             (->> src
                              (st/map #(catch-stream-error %))
                              (st/realize-each))
         err             (st/stream)
         dst             (st/stream)
         divert-callback (fn [v]
                           (if (stream-error? v)
                             (st/put! err v)
                             (st/put! dst v)))
         close-callback #(do
                           (st/close! dst)
                           (st/close! err))]
     (st/connect
      src
      (Callback. divert-callback close-callback dst nil)
      {})
     (st/connect
      src
      (Callback. (fn [_] (d/success-deferred true)) nil err nil)
      {})
     [err dst])))

(defn- log-divert-stream-errors
  "log exemplars of stream errors and divert the errors to the err
   stream. returns [err dst]"
  [description source]
  (let [logger (log-stream-error-exemplars description 2 source)]
    (->> source
         (st/map #(catch-stream-error %))
         (st/realize-each)
         (st/map logger)
         (divert-stream-errors))))

(defmacro safe-call-reducing-fn
  [err-stream rv & body]
  `(let [r# (try
              ~@body
              (catch Throwable e#
                (st/put! ~err-stream (->StreamError e#))
                ~rv))]

     (if (d/deferred? r#)
       (do
         (st/put! ~err-stream
                  (->StreamError
                   (ex-info "reducing fns must return plain (undeferred) values"
                            {:rv ~rv})))
         ~rv)

       r#)))

(defn reduce-all-throw
  "reduce a stream, but if there are any errors will log
   exemplars and return an error-deferred with the first error. always
   reduces the entire stream, as if any errors were filtered"
  ([description f source]
   (reduce-all-throw description f ::st/none source))
  ([description f init source]
   (let [[err source] (log-divert-stream-errors description source)
         rv-d (st/reduce
               (fn
                 ([]     (f))
                 ([rs i] (safe-call-reducing-fn err rs  (f rs i))))
               init
               source)
         e-d (s-first ::none err)]
     (ddo [e e-d
           rv rv-d]
       (if (= ::none e)
         (return rv)
         (d/error-deferred (:error e)))))))

(defmacro reduce
  "Macro wrapping reduce-all-throw that uses the calling location (namespace,
  line, and column) in the `description` of logged errors"
  ([f source]
   (let [ns# *ns*
         {l# :line
          c# :column} (meta &form)
         tag# (str ns# ":L" l# ":C" c#)]
     `(reduce-all-throw
       ~tag#
       ~f
       ~source)))
  ([f init source]
   (let [ns# *ns*
         {l# :line
          c# :column} (meta &form)
         tag# (str ns# ":L" l# ":C" c#)]
     `(reduce-all-throw
       ~tag#
       ~f
       ~init
       ~source))))

(defn count-all-throw
  [description source]
  (reduce-all-throw
   description
   (fn [cnt _] (inc cnt))
   0
   source))

(defn close-on-closed
  "close a Closeable object (such as an InputStream) when the stream
   is closed"
  [closeable stream]
  (st/on-closed
   stream
   (fn []
     (warn "close-on-closed")
     (.close closeable)))
  stream)

(defn stream-peek
  "peeks the first value from a stream, returning
   [val new-stream] where val is the first value and
   new-stream is a new-stream with the same content as the
   input stream. default-val is returned as the val if
   the input stream is empty"
  ([stream]
   (stream-peek stream ::closed))
  ([stream default-val]
   (ddo [:let [out (st/stream 1)]
         v (st/take! stream default-val)
         _ (monad/when (not= v default-val)
             (st/put! out v))]
     (st/connect stream out)
     (return
      [v out]))))

(defn realize-stream
  "Returns a deferred of a vector of realized items from stream
  or deferred of the passed value if it's not a stream."
  [v]
  (if-not (st/stream? v)
    (return deferred-context v)
    (->> v
         realize-each
         (reduce conj []))))

(defn test-realize-stream
  "given a possibly deferred stream,
   realize it as a Deferred<vector>"
  [v]
  (ddo [v (if (d/deferred? v)
            v
            (d/success-deferred v))]

       (if-not (st/stream? v)
         (return deferred-context v)
         (->> v
              realize-each
              (reduce conj [])))))

(defn ^:deprecated map-serially*
  "apply a function f to each of the values in
   stream s, strictly serially - only one application of f will
   be in progress at a time

   can be used to process a set of jobs serially, since using
   stream buffering to control concurrency is somewhat inexact
   (minimum of 2 ops in progress concurrently, or if you apply
   a buffer then n+3 ops in progress)

   internally makes direct use of manifold's reduce fn, which
   passes a deferred reduce result straight to the next
   reduce iteration

   WARNING: this implementation seems to have a bug and can
            fail leaving streams incompletely consumed

   a new implementation is required
  "
  [description f s]
  (let [out (stream)]
    (d/chain
     (pr/catch-error-log
      description
      (st/reduce (fn [p v]
                   (ddo [_ p ;; extract p
                         r (catch-stream-error
                            (f v)) ;; apply f and extract
                         ;; send the response to the output, backpressure
                         _ (put! out r)]
                     (return r))) ;; wrap the response for the next iteration
              (return deferred-context true)
              (->source s)))
     (fn [_]
       (close! out)))
    out))

(defmacro ^:deprecated map-serially
  [f s]
  (let [ns# *ns*
        {l# :line
         c# :column} (meta &form)
        tag# (str ns# ":L" l# ":C" c#)]
    `(map-serially* ~tag# ~f ~s)))

(defn map-concurrency-two
  [f s]
  (->> s
       (map f)
       (st/realize-each)))

(comment
  ;; use the following to convince yourself that the
  ;; minimum concurrency achievable with
  ;; buffer/realize is buffer-size+3 -
  ;; presumably due to each
  ;; stage in a stream chain introducing an implicit
  ;; buffer of 1

  (def ops-a (atom #{}))
  (def ops-history-a (atom []))

  (def op (fn [v]
            (manifold.deferred/future
              (prn "starting:" v)
              (swap! ops-a conj v)
              (swap! ops-history-a conj @ops-a)
              (Thread/sleep (* 100 v))
              (prn "finishing;" v)
              (swap! ops-a disj v)
              (swap! ops-history-a conj @ops-a)
              (inc v))))

  (reset! ops-history-a [])
  @(->> [0 1 2 3 4 5 6 7 8 9]
        (manifold.stream/->source)
        (manifold.stream/map op)
        (manifold.stream/buffer 2)
        (manifold.stream/realize-each)
        (manifold.stream/reduce conj []))
  @ops-history-a
  )

(defn buffer-concurrency
  "use realize-each and buffer to control the
   concurrency of a stream of deferred results of
   operations (probably produced by mapping a fn
   over a description of the operation)"
  [con s]
  (when (< con 3)
    (throw (ex-info "minimum concurrency is 3" {:con con})))
  (->> s
       (st/buffer (max 0 (- con 3)))
       (st/realize-each)))

(defn map-concurrently
  "uniform interface to all the concurrency options - map a fn
   (which returns a deferred result) over a stream,
   ensuring that there are a
   maximum of con unrealized results at any one time

  Supported options
  - :timeout-ms - (optional, number) time bound for processing
                  each item in the stream."
  ([con f s] (map-concurrently {} con f s))
  ([options con f s]
   (let [{timeout-ms :timeout-ms} options
         f (cond
             (nil? timeout-ms) f
             (number? timeout-ms) (fn f-with-timeout [item]
                                    (d/timeout! (f item)
                                                timeout-ms)))]
     (case con
       1 (map-serially f s)
       2 (map-concurrency-two f s)
       (->> s
            (map f)
            (buffer-concurrency con))))))

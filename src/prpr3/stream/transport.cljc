(ns prpr3.stream.transport
  "low-level transport - covers put!ing onto and
   take!ing from a stream, error propagation,
   special value wrapping/unwrapping and
   stream connection"
  (:require
   [promesa.core :as pr]
   [taoensso.timbre :refer [warn]]
   [prpr3.promise :as prpr]
   [prpr3.stream.protocols :as pt]
   [prpr3.stream.types :as types]
   #?(:clj [prpr3.stream.manifold :as stream.manifold]
      :cljs [prpr3.stream.core-async :as stream.async])
   [prpr3.stream.promesa-csp :as stream.promesa-csp])
  (:refer-clojure
   :exclude [map filter mapcat reductions reduce concat]))

(def stream-factory
  #?(:clj stream.promesa-csp/stream-factory
     ;; :clj stream.manifold/stream-factory

     :cljs stream.promesa-csp/stream-factory
     ;; :cljs stream.async/stream-factory
     ))

(defn stream
  ([]
   (pt/-stream stream-factory))
  ([buffer]
   (pt/-stream stream-factory buffer))
  ([buffer xform]
   (pt/-stream stream-factory buffer xform))
  #?(:clj
     ([buffer xform executor]
      (pt/-stream stream-factory buffer xform executor))))

(defn stream?
  [v]
  (pt/-stream? v))

(defn close!
  [s]
  (pt/-close! s))

(defn closed?
  "not in the public API, because it's so often a race condition,
   but may be sometimes useful for inspection"
  [s]
  (pt/-closed? s))

(defn put!
  "put a value onto a stream with backpressure - returns
   Promise<true|false> which eventually resolves to:
    - true when the value was accepted onto the stream
    - false if the stream was closed
    - timeout-val if the put! timed out"
  ([sink val]
   (pt/-put!
    sink
    (pt/-wrap-value sink val)))
  ([sink val timeout timeout-val]
   (pt/-put!
    sink
    (pt/-wrap-value sink val)
    timeout
    timeout-val)))

(defn error!
  "mark a stream as errored

  puts an marker wrapper with the error on to the stream,
  and then closes it. consuming fns will throw an error
  when they encounter it, so errors are always propagated

  it would be nicer if the underlying stream/channel had
  an error state, but this is the best to be done without
  wrapping the underlying stream/channel"
  [sink err]
  (let [wrapped-err (types/stream-error err)]
    (->
     (pt/-put! sink wrapped-err)
     (pr/handle
      (fn [_ _]
        (pt/-close! sink)))
     (pr/handle
      (fn [_ _]
        ;; return false so that error! can be used like a put!
        ;; in connect fns
        false)))))

(defn put-all!
  "put all values onto a stream with backpressure
   returns Promise<true|false> yielding true if all
   values were accepted onto the stream, false otherwise"
  [sink vals]
  #_{:clj-kondo/ignore [:loop-without-recur]}
  (pr/loop [vals vals]
    ;; (prn "put-all! in:" vals)
    (if (empty? vals)
      true
      (pr/chain
       (put! sink (first vals))
       (fn [result]
         ;; (prn "put-all! out:" result)
         (if result
           (pr/recur (rest vals))
           false))))))

(defn put-all-and-close!
  [sink vals]
  (pr/handle
   (put-all! sink vals)
   (fn [s e]
     (close! sink)
     (if (some? e)
       (pr/rejected e)
       s))))

(defn take!
  "take a value from a stream - returns Promise<value|error>
   which evantually resolves to:
   - a value when one becomes available
   - nil or default-val if the stream closes
   - timeout-val if no value becomes available in timeout ms
   - an error if the stream errored (i.e. an error occurred
     during some upstream operation)

   NOTE take! API would ideally not return chunks, but it curently does...
   don't currently have a good way of using a consumer/ChunkConsumer
   in the public API, since i don't really want to wrap the underlying stream
   or channel in something else"
  ([source]
   (pr/chain
    (pt/-take! source)
    pt/-unwrap-value))
  ([source default-val]
   (pr/chain
    (pt/-take! source default-val)
    pt/-unwrap-value))
  ([source default-val timeout timeout-val]
   (pr/chain
    (pt/-take! source default-val timeout timeout-val)
    pt/-unwrap-value)))

(defn unwrap-platform-error
  [x]
  (if (satisfies? pt/IPlatformErrorWrapper x)
    (pt/-unwrap-platform-error x)
    x))

(defn safe-connect-via-fn
  "return a new connect-via fn which handles errors
   in the connect fn or from the source and error!s
   the sink"
  [f sink]

  (fn [val]
    ;; (prn "safe-connect-via-fn: value" val)
    (prpr/handle-always

     ;; always apply f to unwrapped values
     (-> val
         (pt/-unwrap-value)
         (f))

     (fn [success err]
       ;; (warn "safe-connect-via-fn: handle" success err)

       (if (some? err)
         (error! sink err)

         ;; we don't need to put! the value onto sink
         ;; f will already have done that... we just
         ;; return f's return value
         success)))))

(defn connect-via
  "feed all messages from src into callback on the
   understanding that they will eventually propagate into
   dst

   the return value of callback should be a boolean or
   promise yielding a boolean. when false the downstream sink
   is assumed to be closed and the connection is severed"
  ([source f sink]
   (pt/-connect-via
    source
    (safe-connect-via-fn f sink)
    sink
    nil))
  ([source f sink opts]
   (pt/-connect-via
    source
    (safe-connect-via-fn f sink)
    sink
    opts)))

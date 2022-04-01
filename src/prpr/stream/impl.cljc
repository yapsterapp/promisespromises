(ns prpr.stream.impl
  "core implementation - covers put!ing onto and
   take!ing from a stream, error propagation,
   special value wrapping/unwrapping and
   stream connection"
  (:require
   [promesa.core :as pr]
   [prpr.stream.protocols :as pt]
   [prpr.stream.types :as types]
   #?(:clj [prpr.stream.manifold :as stream.manifold]
      :cljs [prpr.stream.core-async :as stream.async]))
  (:refer-clojure
   :exclude [map filter mapcat reductions reduce concat]))

(def stream-factory
  #?(:clj stream.manifold/stream-factory
     :cljs stream.async/stream-factory))

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

(defn put!
  "put a value onto a stream with backpressure - returns
   Promise<true|false> which eventually resolves to:
    - true when the value was accepted onto the stream
    - false if the stream was closed
    - timeout-val if the put! timed out

    note that timeout behaviour may vary by platform.
    on manifold, timed out put!s do not make it to the
    stream, whereas on core.async they may"
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
  when they encounter it, so errors are always propagated"
  [sink err]
  (let [wrapped-err (types/stream-error err)]
    (pr/chain
     (pt/-put! sink wrapped-err)
     (fn [_]
       (pt/-close! sink))
     (fn [_]
       ;; return false so that error! can be used like a put!
       ;; in connect fns
       false))))

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

(defn take!
  "take a value from a stream - returns Promise<value|error>
   which evantually resolves to:
   - a value when one becomes available
   - nil or default-val if the stream closes
   - timeout-val if no value becomes available in timeout ms
   - an error if the stream errored (i.e. an error occurred
     during some upstream operation)

   note that timeout behaviour may vary by platform. on
   manifold, timed out take!s do not eventually take anything
   from the stream, whereas on core.async they may"
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

(defn connect-via-error-fn
  "return a new connect-via fn which handles errors
   in the connect fn or from the source and error!s
   the sink"
  [f sink]
  (fn [val]
    (pr/handle

     (try
       (if (types/stream-error? val)
         ;; always propagate errors
         val

         ;; apply f to unwrapped
         ;; non-error values
         (-> val
             (pt/-unwrap-value)
             (f)))

       (catch #?(:clj Exception :cljs :default) x
         (types/stream-error x)))

     (fn [success err]

       (cond
         (some? err)
         (error! sink err)

         (types/stream-error? success)
         (error! sink success)

         (false? success)
         (pr/handle
          (close! sink)
          (fn [_ _] false))

         (true? success)
         true

         :else
         (error!
          sink
          (ex-info "illegal return from f" {:success success})))))))

(defn connect-via
  "feed all messages from src into callback on the
   understanding that they will eventually propagate into
   dst

   the return value of callback should be a promise yielding
   either true or false. when false the downstream sink
   is assumed to be closed and the connection is severed"
  ([source f sink]
   (pt/-connect-via
    source
    (connect-via-error-fn f sink)
    sink
    nil))
  ([source f sink opts]
   (pt/-connect-via
    source
    (connect-via-error-fn f sink)
    sink
    opts)))

(ns prpr.stream.core-async
  (:require
   [clojure.core.async :as async]
   [cljs.core.async.impl.channels :refer [ManyToManyChannel]]
   [prpr.stream.protocols :as pt]
   [prpr.stream.types :as types]
   [promesa.core :as pr]))

(deftype StreamFactory []
  pt/IStreamFactory
  (-stream [_]
    (async/chan))
  (-stream [_ buffer]
    (async/chan buffer))
  (-stream [_ buffer xform]
    (async/chan buffer xform)))

(def stream-factory (->StreamFactory))

(defn async-put!
  ([sink val]
   (let [r (pr/deferred)]
     (async/put! sink val #(pr/resolve! r %) true)
     r))
  ([sink val timeout timeout-val]
   (let [r (pr/deferred)
         timeout-r (pr/timeout
                    r
                    timeout
                    timeout-val)]
     (async/put! sink val #(pr/resolve! r %) true)
     timeout-r)))

(defn async-error!
  [sink err]
  (pr/chain
   (pt/-put! sink (types/stream-error err))
   (fn [_]
     (pt/-close! sink))
   (fn [_]
     ;; return false so that -error! can be used like a put!
     ;; in connect fns
     false)))

(defn async-take!
  ([source]
     (let [r (pr/deferred)]
       (async/take! source #(pr/resolve! r %) true)
       r))
    ([source default-val]
     (let [r (pr/deferred)
           dr (pr/chain r (fn [v] (if (some? v) v default-val)))]
       (async/take! source #(pr/resolve! r %) true)
       dr))
    ([source default-val timeout timeout-val]
     (let [r (pr/deferred)
           dr (pr/chain r (fn [v] (if (some? v) v default-val)))
           tdr (pr/timeout dr timeout timeout-val)]
       (async/take! source #(pr/resolve! r %) true)
       tdr)))

(defn async-close!
  [ch]
  (async/close! ch))

(defn async-connect-via
  "feed all messages from src into callback on the
   understanding that they will eventually propagate into
   dst

   the return value of callback should be a promise yielding
   either true or false. when false the downstream sink
   is assumed to be closed and the connection is severed"
  ([src callback dst]
   (async-connect-via src callback dst nil))
  ([src callback dst _opts]

   (pr/catch
       (pr/loop []
         (pr/chain
          (pt/-take! src)

          (fn [v]
            (if (nil? v)

              (do
                (pt/-close! dst)
                false)

              ;; callback is reponsible for putting
              ;; messages on to dst
              (callback v)))

          (fn [result]
            (when result
              (pr/recur)))))

       (fn [e]
         (pr/chain
          (pt/-error! dst e)
          (fn [_] (pt/-close! dst)))))))

(defn async-wrap
  "nils can't be put directly on core.async chans,
   so to present a very similar API on both clj+cljs we
   wrap nils for core.async"
  [v]
  (if (nil? v)
    (types/stream-nil)
    v))

(extend-protocol pt/IStream
  ManyToManyChannel
  (-put!
    ([sink val] (async-put! sink val))
    ([sink val timeout timeout-val] (async-put! sink val timeout timeout-val)))

  (-error!
    [sink err] (async-error! sink err))

  (-take!
    ([source] (async-take! source))
    ([source default-val] (async-take! source default-val))
    ([source default-val timeout timeout-val]
     (async-take! source default-val timeout timeout-val)))

  (-close! [this] (async-close! this))

  (-connect-via
    ([source f sink] (async-connect-via source f sink))
    ([source f sink opts] (async-connect-via source f sink opts)))

  (-wrap [v] (async-wrap v)))

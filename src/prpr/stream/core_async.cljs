(ns prpr.stream.core-async
  (:require
   [clojure.core.async :as async]
   [cljs.core.async.impl.channels :refer [ManyToManyChannel]]
   [prpr.stream.protocols :as pt]
   [prpr.stream.types :as types]
   [promesa.core :as pr]
   [taoensso.timbre :refer [fatal]]))

(deftype StreamFactory []
  pt/IStreamFactory
  (-stream [_]
    (async/chan))
  (-stream [_ buffer]
    (async/chan buffer))
  (-stream [_ buffer xform]
    (async/chan buffer xform)))

(def stream-factory (->StreamFactory))

(extend-protocol pt/IMaybeStream
  ManyToManyChannel
  (-stream? [_] true)

  default
  (-stream? [_] false))

(defn async-put!
  ([sink val]
   (let [r (pr/deferred)]
     ;; (prn "async-put!" val)
     (async/put! sink val #(pr/resolve! r %))
     r))
  ([sink val timeout timeout-val]
   (let [r (pr/deferred)
         timeout-r (pr/timeout
                    r
                    timeout
                    timeout-val)]
     (async/put! sink val #(pr/resolve! r %))
     timeout-r)))

(defn async-error!
  "this is also implemented in impl.. but circular deps..."
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
       (async/take! source #(pr/resolve! r %))
       r))
    ([source default-val]
     (let [r (pr/deferred)
           dr (pr/chain r (fn [v] (if (some? v) v default-val)))]
       (async/take! source #(pr/resolve! r %))
       dr))
    ([source default-val timeout timeout-val]
     (let [r (pr/deferred)
           dr (pr/chain r (fn [v] (if (some? v) v default-val)))
           tdr (pr/timeout dr timeout timeout-val)]
       (async/take! source #(pr/resolve! r %))
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
  ([src
    callback
    dst
    {close-src? :prpr.stream/upstream?
     :as _opts}]

   (pr/loop []
         ;; (prn "async-connect-via: pre-take!")

         (-> (pt/-take! src)

             (pr/handle
              (fn [v err]
                ;; (prn "async-connect-via: value" v err)

                (cond
                  (some? err)
                  (async-error! dst err)

                  (nil? v)
                  ;; src has closed
                  (do
                    (pt/-close! dst)
                    ::closed)

                  :else
                  ;; callback is reponsible for putting
                  ;; messages on to dst
                  (callback v))))


             (pr/handle
              (fn [result err]
                ;; (prn "async-connect-via: result" result err)

                (cond
                  (some? err)
                  (do
                    (pt/-close! src)
                    (async-error! dst err))

                  (true? result)
                  (do
                    ;; (prn "async-connect-via: recur")
                    (pr/recur))

                  :else
                  (do
                    ;; manifold default to not always closing the src
                    ;; when the connection terminates... but manifold has
                    ;; a behaviour where the src will always close when its
                    ;; last sink is removed, which means that sources don't
                    ;; leak after their sinks are removed
                    ;;
                    ;; core.async does not have this behavious, so we
                    ;; default to closing the source by default when a
                    ;; connection is broken

                    (when-not (false? close-src?)
                      (async-close! src))

                    (if (= ::closed result)
                      true
                      false)))))))))

(defn async-wrap-value
  "nils can't be put directly on core.async chans,
   so to present a very similar API on both clj+cljs we
   wrap nils for core.async"
  [v]
  (if (nil? v)
    (types/stream-nil)
    v))

(defn async-buffer
  ([ch n]
   (async/pipe
    ch
    (async/chan n))))

(extend-protocol pt/IStream
  ManyToManyChannel
  (-put!
    ([sink val] (async-put! sink val))
    ([sink val timeout timeout-val] (async-put! sink val timeout timeout-val)))

  (-take!
    ([source] (async-take! source))
    ([source default-val] (async-take! source default-val))
    ([source default-val timeout timeout-val]
     (async-take! source default-val timeout timeout-val)))

  (-close! [this] (async-close! this))

  (-connect-via
    ([source f sink] (async-connect-via source f sink))
    ([source f sink opts] (async-connect-via source f sink opts)))

  (-wrap-value [s v] (async-wrap-value v))
  (-buffer [s n] (async-buffer s n)))

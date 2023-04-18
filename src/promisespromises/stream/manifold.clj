(ns prpr3.stream.manifold
  (:require
   [manifold.deferred :as m.deferred]
   [manifold.stream :as m.stream]
   [prpr3.stream.protocols :as p]
   [promesa.core :as promise]
   [promesa.protocols :as promise.p])
  (:import
   [manifold.stream.default Stream]
   [manifold.deferred Deferred SuccessDeferred ErrorDeferred LeakAwareDeferred]
   [java.util.concurrent ExecutionException CompletionException]))

(defn manifold-stream
  ([] (m.stream/stream))
  ([buffer] (m.stream/stream buffer))
  ([buffer xform] (m.stream/stream buffer xform))
  ([buffer xform executor] (m.stream/stream buffer xform executor)))

(deftype StreamFactory []
  p/IStreamFactory
  (-stream [_] (manifold-stream))
  (-stream [_ buffer] (manifold-stream buffer))
  (-stream [_ buffer xform] (manifold-stream buffer xform))
  (-stream [_ buffer xform executor] (manifold-stream buffer xform executor)))

(def stream-factory (->StreamFactory))

(extend-protocol p/IMaybeStream
  Object
  (-stream? [v] (m.stream/stream? v)))

(defn promise->deferred
  [v]
  (if (promise/promise? v)
    (m.deferred/->deferred v)
    v))

(def default-connect-via-opts
  {;; standard manifold default
   :prpr3.stream/downstream? true
   ;; *not* the standard manifold default - but we
   ;; can easily implement this behaviour for core.async too
   ;; so going with it for cross-platform consistency
   :prpr3.stream/upstream? true})

(defn manifold-connect-via-opts
  [{downstream? :prpr3.stream/downstream?
    upstream? :prpr3.stream/upstream?
    timeout :prpr3.stream/timeout
    description :prpr3.stream/description}]
  (cond-> {}
    (some? downstream?) (assoc :downstream? downstream?)
    (some? upstream?) (assoc :upstream? upstream?)
    (some? timeout) (assoc :timeout timeout)
    (some? description) (assoc :description description)))

(extend-protocol p/IStream
  Stream
  (-closed? [s]
    (m.stream/closed? s))

  (-put!
    ([sink val]
     (m.stream/put! sink val))
    ([sink val timeout timeout-val]
     (m.stream/try-put! sink val timeout timeout-val)))

  (-take!
    ([source]
     (m.stream/take! source))
    ([source default-val]
     (m.stream/take! source default-val))
    ([source default-val timeout timeout-val]
     (m.stream/try-take! source default-val timeout timeout-val)))

  (-close! [this] (m.stream/close! this))

  (-connect-via
    ([source f sink]
     (let [f' (comp promise->deferred f)]
       (m.stream/connect-via
        source
        f'
        sink
        (manifold-connect-via-opts default-connect-via-opts))))
    ([source f sink opts]
     (let [f' (comp promise->deferred f)]
       (m.stream/connect-via
        source
        f'
        sink
        (manifold-connect-via-opts
         (merge default-connect-via-opts opts))))))

  ;; don't need to wrap anything for manifold
  (-wrap-value [_ v] v)

  (-buffer [s n]
    (m.stream/buffer s n)))

(extend-protocol p/IPlatformErrorWrapper
  ExecutionException
  (-unwrap-platform-error [this] (ex-cause this))
  CompletionException
  (-unwrap-platform-error [this] (ex-cause this)))

(defn ->promesa
  [d]
  (let [p (promise/deferred)]
    (m.deferred/on-realized
     d
     (fn [v] (promise/resolve! p v))
     (fn [err] (promise/reject! p err)))
    p))

(extend-protocol promise.p/IPromiseFactory
  Deferred
  (-promise [d]
    (->promesa d))

  SuccessDeferred
  (-promise [d]
    (->promesa d))

  ErrorDeferred
  (-promise [d]
    (->promesa d))

  LeakAwareDeferred
  (-promise [d]
    (->promesa d)))

(extend-protocol promise.p/IPromise
  Deferred
  (-bind
    ([d f]
     (-> d (->promesa) (promise.p/-bind f)))
    ([d f executor]
     (-> d (->promesa) (promise.p/-bind f executor))))

  (-map
    ([d f]
     (-> d (->promesa) (promise.p/-map f)))
    ([d f executor]
     (-> d (->promesa) (promise.p/-map f executor))))

  (-catch
    ([d f]
     (-> d (->promesa) (promise.p/-catch f)))
    ([d f executor]
     (-> d (->promesa) (promise.p/-catch f executor))))

  (-handle
    ([d f]
     (-> d (->promesa) (promise.p/-handle f)))
    ([d f executor]
     (-> d (->promesa) (promise.p/-handle f executor))))

  (-finally
    ([d f]
     (-> d (->promesa) (promise.p/-finally f)))
    ([d f executor]
     (-> d (->promesa) (promise.p/-finally f executor))))

  SuccessDeferred
  (-bind
    ([d f]
     (-> d (->promesa) (promise.p/-bind f)))
    ([d f executor]
     (-> d (->promesa) (promise.p/-bind f executor))))

  (-map
    ([d f]
     (-> d (->promesa) (promise.p/-map f)))
    ([d f executor]
     (-> d (->promesa) (promise.p/-map f executor))))

  (-catch
    ([d f]
     (-> d (->promesa) (promise.p/-catch f)))
    ([d f executor]
     (-> d (->promesa) (promise.p/-catch f executor))))

  (-handle
    ([d f]
     (-> d (->promesa) (promise.p/-handle f)))
    ([d f executor]
     (-> d (->promesa) (promise.p/-handle f executor))))

  (-finally
    ([d f]
     (-> d (->promesa) (promise.p/-finally f)))
    ([d f executor]
     (-> d (->promesa) (promise.p/-finally f executor))))

  ErrorDeferred
  (-bind
    ([d f]
     (-> d (->promesa) (promise.p/-bind f)))
    ([d f executor]
     (-> d (->promesa) (promise.p/-bind f executor))))

  (-map
    ([d f]
     (-> d (->promesa) (promise.p/-map f)))
    ([d f executor]
     (-> d (->promesa) (promise.p/-map f executor))))

  (-catch
    ([d f]
     (-> d (->promesa) (promise.p/-catch f)))
    ([d f executor]
     (-> d (->promesa) (promise.p/-catch f executor))))

  (-handle
    ([d f]
     (-> d (->promesa) (promise.p/-handle f)))
    ([d f executor]
     (-> d (->promesa) (promise.p/-handle f executor))))

  (-finally
    ([d f]
     (-> d (->promesa) (promise.p/-finally f)))
    ([d f executor]
     (-> d (->promesa) (promise.p/-finally f executor))))

  LeakAwareDeferred
  (-bind
    ([d f]
     (-> d (->promesa) (promise.p/-bind f)))
    ([d f executor]
     (-> d (->promesa) (promise.p/-bind f executor))))

  (-map
    ([d f]
     (-> d (->promesa) (promise.p/-map f)))
    ([d f executor]
     (-> d (->promesa) (promise.p/-map f executor))))

  (-catch
    ([d f]
     (-> d (->promesa) (promise.p/-catch f)))
    ([d f executor]
     (-> d (->promesa) (promise.p/-catch f executor))))

  (-handle
    ([d f]
     (-> d (->promesa) (promise.p/-handle f)))
    ([d f executor]
     (-> d (->promesa) (promise.p/-handle f executor))))

  (-finally
    ([d f]
     (-> d (->promesa) (promise.p/-finally f)))
    ([d f executor]
     (-> d (->promesa) (promise.p/-finally f executor)))))

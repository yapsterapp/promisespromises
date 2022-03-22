(ns prpr.stream.manifold
  (:require
   [manifold.deferred :as m.deferred]
   [manifold.stream :as m.stream]
   [prpr.stream.protocols :as p]
   [prpr.stream.error :as stream.err]
   [promesa.core :as promise]
   [promesa.protocols :as promise.p])
  (:import
   [manifold.stream.default Stream]
   [manifold.deferred Deferred SuccessDeferred ErrorDeferred LeakAwareDeferred]))

(deftype StreamFactory []
  p/IStreamFactory
  (-stream [_]
    (m.stream/stream))
  (-stream [_ buffer]
    (m.stream/stream buffer))
  (-stream [_ buffer xform]
    (m.stream/stream buffer xform))
  (-stream [_ buffer xform executor]
    (m.stream/stream buffer xform executor)))

(def stream-factory (->StreamFactory))

(extend-protocol p/IStream
  Stream
  (-put!
    ([sink val]
     (m.stream/put! sink val))
    ([sink val timeout timeout-val]
     (m.stream/try-put! sink val timeout timeout-val)))

  (-error!
    [sink err]
    (promise/chain
     (m.stream/put! sink (stream.err/stream-error err))
     (fn [_]
       (m.stream/close! sink))
     (fn [_]
       ;; return false so that -error! can be used like a put!
       ;; in connect fns
       false)))

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
     (m.stream/connect-via source f sink))
    ([source f sink opts]
     (m.stream/connect-via source f sink opts))))

(defn ->promesa
  [d]
  (let [p  (promise/deferred)]
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
    (->promesa d)))

(extend-protocol promise.p/IPromise
  Deferred
  (-bind
    ([d f]
     (-> d (->promesa) (promise.p/-bind f)))
    ([d f executor]
    (-> d (->promesa) (promise.p/-bind f executor))))

  (-finally
    ([d f]
     (-> d (->promesa) (promise.p/-finally f)))
    ([d f executor]
     (-> d (->promesa) (promise.p/-finally f executor))))

  (-handle
    ([d f]
     (-> d (->promesa) (promise.p/-handle f)))
    ([d f executor]
     (-> d (->promesa) (promise.p/-handle f executor))))

  (-map
    ([d f]
     (-> d (->promesa) (promise.p/-map f)))
    ([d f executor]
     (-> d (->promesa) (promise.p/-map f executor))))

  (-mapErr
    ([d f]
     (-> d (->promesa) (promise.p/-mapErr f)))
    ([d f executor]
     (-> d (->promesa) (promise.p/-mapErr f executor))))

  (-then
    ([d f]
     (-> d (->promesa) (promise.p/-then f)))
    ([d f executor]
     (-> d (->promesa) (promise.p/-then f executor))))

  (-thenErr
    ([d f]
     (-> d (->promesa) (promise.p/-thenErr f)))
    ([d f executor]
     (-> d (->promesa) (promise.p/-thenErr f executor))))

  SuccessDeferred
  (-bind
    ([d f]
     (-> d (->promesa) (promise.p/-bind f)))
    ([d f executor]
    (-> d (->promesa) (promise.p/-bind f executor))))

  (-finally
    ([d f]
     (-> d (->promesa) (promise.p/-finally f)))
    ([d f executor]
     (-> d (->promesa) (promise.p/-finally f executor))))

  (-handle
    ([d f]
     (-> d (->promesa) (promise.p/-handle f)))
    ([d f executor]
     (-> d (->promesa) (promise.p/-handle f executor))))

  (-map
    ([d f]
     (-> d (->promesa) (promise.p/-map f)))
    ([d f executor]
     (-> d (->promesa) (promise.p/-map f executor))))

  (-mapErr
    ([d f]
     (-> d (->promesa) (promise.p/-mapErr f)))
    ([d f executor]
     (-> d (->promesa) (promise.p/-mapErr f executor))))

  (-then
    ([d f]
     (-> d (->promesa) (promise.p/-then f)))
    ([d f executor]
     (-> d (->promesa) (promise.p/-then f executor))))

  (-thenErr
    ([d f]
     (-> d (->promesa) (promise.p/-thenErr f)))
    ([d f executor]
     (-> d (->promesa) (promise.p/-thenErr f executor))))

  ErrorDeferred
  (-bind
    ([d f]
     (-> d (->promesa) (promise.p/-bind f)))
    ([d f executor]
    (-> d (->promesa) (promise.p/-bind f executor))))

  (-finally
    ([d f]
     (-> d (->promesa) (promise.p/-finally f)))
    ([d f executor]
     (-> d (->promesa) (promise.p/-finally f executor))))

  (-handle
    ([d f]
     (-> d (->promesa) (promise.p/-handle f)))
    ([d f executor]
     (-> d (->promesa) (promise.p/-handle f executor))))

  (-map
    ([d f]
     (-> d (->promesa) (promise.p/-map f)))
    ([d f executor]
     (-> d (->promesa) (promise.p/-map f executor))))

  (-mapErr
    ([d f]
     (-> d (->promesa) (promise.p/-mapErr f)))
    ([d f executor]
     (-> d (->promesa) (promise.p/-mapErr f executor))))

  (-then
    ([d f]
     (-> d (->promesa) (promise.p/-then f)))
    ([d f executor]
     (-> d (->promesa) (promise.p/-then f executor))))

  (-thenErr
    ([d f]
     (-> d (->promesa) (promise.p/-thenErr f)))
    ([d f executor]
     (-> d (->promesa) (promise.p/-thenErr f executor))))

  LeakAwareDeferred
  (-bind
    ([d f]
     (-> d (->promesa) (promise.p/-bind f)))
    ([d f executor]
    (-> d (->promesa) (promise.p/-bind f executor))))

  (-finally
    ([d f]
     (-> d (->promesa) (promise.p/-finally f)))
    ([d f executor]
     (-> d (->promesa) (promise.p/-finally f executor))))

  (-handle
    ([d f]
     (-> d (->promesa) (promise.p/-handle f)))
    ([d f executor]
     (-> d (->promesa) (promise.p/-handle f executor))))

  (-map
    ([d f]
     (-> d (->promesa) (promise.p/-map f)))
    ([d f executor]
     (-> d (->promesa) (promise.p/-map f executor))))

  (-mapErr
    ([d f]
     (-> d (->promesa) (promise.p/-mapErr f)))
    ([d f executor]
     (-> d (->promesa) (promise.p/-mapErr f executor))))

  (-then
    ([d f]
     (-> d (->promesa) (promise.p/-then f)))
    ([d f executor]
     (-> d (->promesa) (promise.p/-then f executor))))

  (-thenErr
    ([d f]
     (-> d (->promesa) (promise.p/-thenErr f)))
    ([d f executor]
     (-> d (->promesa) (promise.p/-thenErr f executor)))))

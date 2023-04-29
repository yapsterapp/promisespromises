(ns promisespromises.stream.promesa-csp
  (:require
   [promesa.exec.csp :as sp]
   #?(:cljs [promesa.exec.csp.channel :refer [Channel]])
   [promisespromises.stream.protocols :as pt]
   [promisespromises.stream.types :as types]
   [promesa.core :as pr]
   [promisespromises.promise :as prpr])
  (:import
   #?(:clj [promesa.exec.csp.channel Channel])))

(defn promesa-csp-stream
  ([] (sp/chan))
  ([buffer] (sp/chan buffer))
  ([buffer xform] (sp/chan buffer xform))
  #?(:clj ([buffer xform _executor] (sp/chan buffer xform))))

(deftype StreamFactory []
  pt/IStreamFactory
  (-stream [_] (promesa-csp-stream))
  (-stream [_ buffer] (promesa-csp-stream buffer))
  (-stream [_ buffer xform] (promesa-csp-stream buffer xform))
  #?(:clj
     (-stream [_ buffer xform executor]
              (promesa-csp-stream buffer xform executor))))

(def stream-factory (->StreamFactory))

(extend-protocol pt/IMaybeStream
  Channel
  (-stream? [_] true))

(defn promesa-csp-put!
  ([sink val]
   (sp/put sink val))

  ([sink val timeout timeout-val]
   ;; (prn "promesa-csp-put!" sink val timeout timeout-val)
   (let [timeout-ch (sp/timeout-chan timeout)]

     (pr/let [[v ch] (sp/alts
                      [[sink val]
                       timeout-ch]
                      :priority true)]
       (if (= ch timeout-ch)
         timeout-val
         v)))))

(defn promesa-csp-error!
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

(defn promesa-csp-take!
  ([source]
   (sp/take source))

  ([source default-val]
   (pr/let [v (sp/take source)]
     (if (some? v) v default-val)))

  ([source default-val timeout timeout-val]
   (let [timeout-ch (sp/timeout-chan timeout)]

     (pr/let [[v ch] (sp/alts
                      [source
                       timeout-ch]
                      :priority true)]
       (cond
         (= ch timeout-ch) timeout-val
         (nil? v) default-val
         :else v)))))

(defn promesa-csp-close!
  [ch]
  (sp/close! ch))

(defn promesa-csp-connect-via
  "feed all messages from src into callback on the
   understanding that they will eventually propagate into
   dst

   the return value of callback should be a promise yielding
   either true or false. when false the downstream sink
   is assumed to be closed and the connection is severed"
  ([src callback dst]
   (promesa-csp-connect-via src callback dst nil))
  ([src
    callback
    dst
    {close-src? :promisespromises.stream/upstream?
     close-sink? :promisespromises.stream/downstream?
     :as _opts}]

   #_{:clj-kondo/ignore [:loop-without-recur]}
   (pr/loop []
     ;; (prn "promesa-csp-connect-via: pre-take!")

         (-> (pt/-take! src ::closed)

             (prpr/handle-always
              (fn [v err]
                ;; (prn "promesa-csp-connect-via: value" v err)

                (cond
                  (some? err)
                  (promesa-csp-error! dst err)

                  (= ::closed v)
                  ;; src has closed
                  (do
                    (when close-sink?
                      (pt/-close! dst))
                    ::closed)

                  :else
                  ;; callback is reponsible for putting
                  ;; messages on to dst
                  (callback v))))


             (prpr/handle-always
              (fn [result err]
                ;; (prn "promesa-csp-connect-via: result" result err)

                (cond
                  (some? err)
                  (do
                    (pt/-close! src)
                    (promesa-csp-error! dst err))

                  (true? result)
                  #_{:clj-kondo/ignore [:redundant-do]}
                  (do
                    ;; (prn "promesa-csp-connect-via: recur")
                    #_{:clj-kondo/ignore [:recur-argument-count]}
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
                      (promesa-csp-close! src))

                    (if (= ::closed result)
                      true
                      false)))))))))

(defn promesa-csp-wrap-value
  "nils can't be put directly on core.async chans,
   so to present a very similar API on both clj+cljs we
   wrap nils for core.async

   promises can be put on a core.async chan, but cause
   problems with promesa-csp-take! because auto-unwrapping
   causes Promise<nil> from the stream to be
   indistinguishable from a closed channel - so wrapping
   promises sidesteps this"
  [v]
  (cond
    (nil? v) (types/stream-nil)
    (pr/promise? v) (types/stream-promise v)
    :else v))

(defn promesa-csp-buffer
  ([ch n]
   (sp/pipe
    ch
    (sp/chan n))))

(def default-connect-via-opts
  {;; standard manifold default
   :promisespromises.stream/downstream? true
   ;; *not* the standard manifold default - but we
   ;; can easily implement this behaviour for core.async too
   ;; so going with it for cross-platform consistency
   :promisespromises.stream/upstream? true})

(extend-protocol pt/IStream
  Channel
  (-closed? [s]
    (sp/closed? s))

  (-put!
    ([sink val] (promesa-csp-put! sink val))
    ([sink val timeout timeout-val] (promesa-csp-put! sink val timeout timeout-val)))

  (-take!
    ([source] (promesa-csp-take! source))
    ([source default-val] (promesa-csp-take! source default-val))
    ([source default-val timeout timeout-val]
     (promesa-csp-take! source default-val timeout timeout-val)))

  (-close! [this] (promesa-csp-close! this))

  (-connect-via
    ([source f sink] (promesa-csp-connect-via source f sink default-connect-via-opts))
    ([source f sink opts] (promesa-csp-connect-via
                           source
                           f
                           sink
                           (merge default-connect-via-opts opts))))

  (-wrap-value [_s v] (promesa-csp-wrap-value v))
  (-buffer [s n] (promesa-csp-buffer s n)))

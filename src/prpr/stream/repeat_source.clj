(ns prpr.stream.repeat-source
  "Provides promise friendly and error safe stream of repeated fn invocations.

  See:

  * [[create-repeat-source]] for the primary interface/documentation.
  * [[repeatedly-source]] for an equivalent to [[clojure.core/repeatedly]].
  * [[iterate-source]] for an equivalent to [[clojure.core/iterate]].

  A Contrived example!

  ```clojure
  ;; slowly gather a stream of ints 1 to 10
  @(prpr.stream/reduce
    conj
    []
    (prpr.stream/transform
     (take 10)
     (iterate-source
      (fn [i]
        (prpr/delay-pr
         (* i 10)
         (inc i)))
      0)))
  ```

  Worry not about errors!

  ```
  ;; play Russian Roulette with a `repeatedly` executed fn
  ;; if this throws, regardless of when, a StreamError will
  ;; propagate
  @(prpr.stream/reduce-all-throw
    'repeat-count
    conj
    []
    (repeatedly-source
     10
     (fn []
       (if (odd? (rand-int 100))
         (throw (ex-info \"err!\" {:error :err}))
         (prpr/success-pr nil)))))
  ```

  A less contrived example of the is making repeated HTTP requests for paginated
  content, which with a few helpers could look something like:

  ```clojure
  @(prpr.stream/reduce
    conj
    []
    (iterate-source
     (fn [last-response]
      (cond
       (= :init last-response)
       (make-initial-query)

       (more-results? last-response)
       (request-next-page last-response)

       :else
       halt!))
     :init))
  ```

  Assuming you’re using an asynchronous HTTP library this makes managing the
  sequence of requests much easier! It also provides the additional benefit of
  being easy to feed into a _buffered_ stream to control the number of in-flight
  requests."
  (:require
   [manifold.deferred :as d]
   [manifold.stream.core :as stream.core]
   [prpr.promise :as prpr]
   [prpr.stream :as stream]
   [schema.core :as s])
  (:import
   (clojure.lang Fn)
   (java.util.concurrent.atomic AtomicReference)))

(s/defschema RepeatSourceOpts
  {(s/optional-key :halt-val) s/Any
   (s/optional-key :halt-on-error?) s/Bool})

(def halt! ::halt)

(defn fn-arities
  "Given a [[Fn]] returns a set of its arities.

  ```clojure
  (fn-arities identity) ;=> #{1}
  (fn-arities (fn ([]) ([x] x) ([x y] [x y]))) ;=> #{0 1 2}
  ```"
  [^Fn f]
  (->> f class .getDeclaredMethods (map (comp alength #(.getParameterTypes %))) set))

(defn repeat-take
  "A generic take/try-take function for [[RepeatSource]]s

  Takes (😒) **all** the options to appropriately handle blocking, timeouts, and
  halting repetition of the `source`."
  [source
   {last-take :last-take
    repeat-fn :repeat-fn
    blocking? :blocking?
    timeout? :timeout?
    timeout-ms :timeout-ms
    timeout-val :timeout-val
    pass-thru-timeout? :pass-thru-timeout?
    default-val :default-val
    halt-val :halt-val
    halt-on-error? :halt-on-error?
    :or {halt-val halt!
         halt-on-error? true}}]
  (let [d (d/deferred)
        d' (.getAndSet last-take d)
        f (fn next
            ([_]
             (next))
            ([]
             (if (.isDrained source)
               (d/success! d default-val)
               (prpr/catch-log
                "RepeatSource repeat-fn error"
                (fn [e]
                  (when halt-on-error?
                    (.markDrained source))
                  (d/success! d (stream/->StreamError e)))
                (when timeout?
                  (prpr/timeout-pr
                   d
                   (if pass-thru-timeout?
                     (* 1.5 timeout-ms) ;; trust but verify 😉
                     timeout-ms)
                   timeout-val))
                (-> (prpr/always-pr
                     (if pass-thru-timeout?
                       (repeat-fn timeout-ms timeout-val)
                       (repeat-fn)))
                    (prpr/chain-pr
                     (fn [result]
                       (if (= halt-val result)
                         (do
                           (.markDrained source)
                           (d/success! d default-val))
                         (d/success! d result))))
                    (prpr/catchall
                     (fn [e]
                       (when halt-on-error?
                         (.markDrained source))
                       (d/success! d (stream/->StreamError e)))))))))]
    (if (d/realized? d')
      (f)
      (d/on-realized d' f f))
    (if blocking?
      @d
      d)))

(stream.core/def-source RepeatSource
  [^Fn repeat-fn
   ^AtomicReference last-take
   opts]

  (isSynchronous
   [_]
   false)

  (description
   [this]
   {:type (str "repeat-source " repeat-fn)
    :source? true
    :drained? (stream.core/drained? this)})

  (close
   [_]
   nil)

  (take
   [this default-val blocking?]
   (repeat-take
    this
    (merge
     opts
     {:last-take last-take
      :repeat-fn repeat-fn
      :blocking? blocking?
      :default-val default-val})))

  (take
   [this default-val blocking? timeout timeout-val]
   (let [can-pass-timeout? (delay (contains? (fn-arities repeat-fn) 2))]
     (repeat-take
      this
      (merge
       opts
       {:last-take last-take
        :repeat-fn repeat-fn
        :blocking? blocking?
        :timeout? (some? timeout)
        :timeout-ms timeout
        :timeout-val timeout-val
        :pass-thru-timeout? @can-pass-timeout?
        :default-val default-val})))))

(s/defn ^:always-validate
  create-repeat-source
  "Returns a stream of repeated calls to `f` (a fn that takes no arguments.)

  Promise friendly and error safe: `f` can return promises (or non promises)
  and any errors raised will be added to the stream as a [[prpr.stream/StreamError]].

  By default the first error encountered will halt repetition, specify
  `{:halt-on-error? false}` in the `opts` to prevent that behaviour.

  If `f` returns [[halt!]] then the stream will be marked as drained and no
  further repetitions performed.

  An alternative “halt” value can be provided via the `opts`:
  `{:halt-val 'my-halt-val}`

  If the stream is `take`en from with a timeout (e.g. via [[prpr.stream/try-take!]])
  *and* `f` has a two-arity invocation then it will be called with the timeout
  duration and value. (If `f` doesn’t have a two-arity invocation it’ll be wrapped
  in a [[prpr.promise/timeout-pr]].)"
  ([f]
   (create-repeat-source f {}))
  ([f opts :- RepeatSourceOpts]
   (->RepeatSource
    f
    (AtomicReference. (d/success-deferred true))
    opts)))

(defn repeatedly-source
  "Like [[clojure.core/repeatedly]] but returning a stream *and* handling
  deferred fn results"
  ([f]
   (create-repeat-source f))
  ([n f]
   (create-repeat-source
    (let [calls (atom 0)]
      (fn repeat-fn []
        (let [call (swap! calls inc)]
          (if (< n call)
            halt!
            (f))))))))

(defn iterate-source
  "Like [[clojure.core/iterate]] but returning a stream *and* handling
  deferred fn results"
  [f x]
  (create-repeat-source
   (let [last-result (atom x)]
     (fn iterate-fn []
       (prpr/chain-pr
        (prpr/always-pr
         (f @last-result))
        (fn [r]
          (reset! last-result r)))))))

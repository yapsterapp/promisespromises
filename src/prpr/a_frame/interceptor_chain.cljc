(ns prpr.a-frame.interceptor-chain
  (:require
   #?(:clj [manifold.deferred :as d])
   #?(:cljs [promesa.core :as p])
   [schema.core :as s]
   [taoensso.timbre :refer [warn]]
   [prpr.promise :as prpr :refer [return-pr]]
   [prpr.a-frame.schema :as af.schema]
   [prpr.a-frame.registry :as registry]
   [prpr.a-frame.interceptor-chain.data :as data]
   [prpr.a-frame.interceptor-chain.data.tag-readers
    #?@(:cljs [:include-macros true])]))

;; a slightly more data-driven interceptor chain for a-frame
;;
;; the idea being that the interceptor context should be serializable,
;; and when there is an error the complete interceptor context
;; can be pretty-printed and used to resume the failed operation
;; for debugging
;;
;; interceptors are registered in the a-frame registry
;; and referenced by a keyword, much like fx and cofx
;;
;; this makes sense for a-frame, which already maintains a registry of different
;; kinds of handlers. it doesn't necessarily make sense for a general purpose
;; interceptor chain, which remains at prpr.interceptor-chain

(s/defschema InterceptorFn
  (s/pred fn?))

(s/defschema Interceptor
  "An Interceptor, all methods are optional but should be implemented as
  follows:

  * `::enter` takes 1 or 2 args:
     - context -> context
     - context -> data -> context

  * `::leave` – takes 1 or 2 args:
     - context -> context
     - context -> data -> context

  * `::error` – takes two args
     - context -> error -> context

  All methods may return either promises or plain values."
  {(s/optional-key ::name) s/Keyword
   (s/optional-key ::enter) InterceptorFn
   (s/optional-key ::leave) InterceptorFn
   (s/optional-key ::error) InterceptorFn
   s/Any s/Any})

(s/defschema InterceptorSpec
  "the interceptor chain is created with a list of InterceptorSpecs. each
   InterceptorSpec is either
   -  simple keyword, referencing a registered interceptor which will cause
      ::enter and ::leave fns to be invoked with 1-arity, or
   - a pair of [interceptor-kw interceptor-data]. if there is data
      (either ::enter-data or ::leave-data) then ::enter and ::leave will
      be invoked with their 2-arity

   providing data like this allows a pure-data (in the re-frame sense - roughly
   something which has no opaque objects and is serializable/deserializable)
   interceptor chain to be registered, which has numerous benefits"
  (s/cond-pre
   s/Keyword

   {::key s/Keyword
    ::data {(s/optional-key ::enter-data) s/Any
            (s/optional-key ::leave-data) s/Any}}))

(def interceptor-fn-keys
  [::enter ::leave ::error])

(s/defschema InterceptorFnKey
  (apply s/enum interceptor-fn-keys))

(def interceptor-fn-noop
  ::noop)

(s/defschema InterceptorFnHistoryKey
  (apply s/enum (conj interceptor-fn-keys interceptor-fn-noop)))

(s/defschema InterceptorHistoryElem
  (s/conditional
   #(= 2 (count %))
   (s/constrained [(s/one InterceptorSpec ::interceptor)
                   (s/one InterceptorFnHistoryKey ::fn)] vector?)

   :else
   (s/constrained [(s/one InterceptorSpec ::interceptor)
                   (s/one InterceptorFnHistoryKey ::fn)
                   (s/one s/Any ::fn-data)] vector?)))

(s/defschema InterceptorContext
  {af.schema/a-frame-app-ctx s/Any
   af.schema/a-frame-router s/Any
   ::queue (s/constrained [InterceptorSpec] vector?)
   ::stack (s/constrained [InterceptorSpec] list?)
   ::history (s/constrained [InterceptorHistoryElem] vector?)
   (s/optional-key ::errors) [s/Any]
   s/Any s/Any})

;; utility fns

(def opaque-context-keys
  "keys which contain opaque data"
  #{af.schema/a-frame-app-ctx
    af.schema/a-frame-router})

(def context-keys
  "The Interceptor specific keys that are added to contexts"
  (->> InterceptorContext
       keys
       (map #(if (s/optional-key? %) (s/explicit-schema-key %) %))
       (filter keyword?)
       set))

(defn dissoc-context-keys
  "Removes all interceptor related keys from `context`"
  [context]
  (apply dissoc context context-keys))

(defn sanitise-context
  "remove impure / opaque data from a context"
  [context]
  (apply dissoc context opaque-context-keys))

(defn pr-loop-context
  "Helper fn to repeat execution of `step-fn` against `context` inside a promise
  loop.

  `step-fn` should return a tuple of either `[::break <value>]` or `[::recur
  <new context>]`. The former will terminate the loop and return `<value>`, the
  later will pass `<new context>` to the next loop iteration.

  (Note: this mainly exists to abstract away and minimise the platform specific
  aspects of handling promise loops.)"
  [context step-fn]
  #?(:clj
     (d/loop [context context]
       (prpr/chain-pr
        (step-fn context)
        (fn [[t c]]
          (if (= ::break t)
            c
            (d/recur c)))))
     :cljs
     (p/loop [context context]
       (prpr/chain-pr
        (step-fn context)
        (fn [[t c]]
          (if (= ::break t)
            c
            (p/recur c)))))))

(s/defn assoc-opaque-keys
  "add the opaque keys to the interceptor context

   they are removed from reported contexts by `sanitise-context`"
  [ctx app-ctx a-frame-router]
  (merge
   ctx
   {af.schema/a-frame-app-ctx app-ctx
    af.schema/a-frame-router a-frame-router}))

(s/defn ^:always-validate initiate
  :- InterceptorContext
  "Given a sequence of [[InterceptorSpec]]s and a map of `initial-context` values,
  returns a new [[InterceptorContext]] ready to [[execute]]"
  [app-ctx
   a-frame-router
   interceptor-chain :- [InterceptorSpec]
   initial-context]

  (->
   initial-context
   (merge {::queue (vec interceptor-chain)
           ::stack '()
           ::history []})
   (assoc-opaque-keys app-ctx a-frame-router)))

(s/defn ^:always-validate enqueue
  :- InterceptorContext
  "Adds `interceptors` to the end of the interceptor queue within `context`"
  [context :- InterceptorContext
   interceptors :- [InterceptorSpec]]
  (update context ::queue into interceptors))

(s/defn ^:always-validate terminate
  :- InterceptorContext
  "Removes all queued interceptors from `context`"
  [context :- InterceptorContext]
  (assoc context ::queue []))

(s/defn ^:always-validate clear-errors
  :- InterceptorContext
  "Removes any associated `::errors` from `context`"
  [context :- InterceptorContext]
  (dissoc context ::errors))

(s/defn ^:always-validate register-interceptor
  [interceptor-key :- s/Keyword
   interceptor :- Interceptor]
  (registry/register-handler ::interceptor interceptor-key interceptor))

(defn resolve-interceptor
  [interceptor-key]
  (let [interceptor (registry/get-handler ::interceptor interceptor-key)]
    (when (nil? interceptor)
      (throw (ex-info "no interceptor" {:interceptor-key interceptor-key})))

    interceptor))

(defn interceptor-data-key
  [interceptor-fn-key]
  (case interceptor-fn-key
    ::enter ::enter-data
    ::leave ::leave-data))

(defn normalize-interceptor-spec
  "turns keyword interceptor-specs into maps
   {::key <interceptor-key>}"
  [interceptor-spec]
  (if (keyword? interceptor-spec)
    {::key interceptor-spec}
    interceptor-spec))

(defn wrap-error
  [resume-context e]
  (ex-info
   "interceptor failed"
   {::context (sanitise-context resume-context)
    ::interceptor-fn-key ::enter}
   e))

(defn unwrap-original-error
  "unwrap layers of wrapping (in case of nested :dispatch)
   to get at the causal exception"
  [e]
  (let [{ctx ::context} (ex-data e)
        cause (ex-cause e)]

    (if (and (some? ctx)
             (some? cause))

      (unwrap-original-error cause)

      e)))

(defn resolve-interceptor-data
  "resolve the interceptor data, returning either
    - [data-val] if there was data specified
    - nil if no data was specified"
  [interceptor-fn-key
   interceptor-data-specs
   context]

  (condp contains? interceptor-fn-key

    #{::enter ::leave}
    (let [data-key (interceptor-data-key interceptor-fn-key)]

      (when (contains? interceptor-data-specs data-key)
        (let [spec (get interceptor-data-specs data-key)
              data (data/resolve-data spec context)]
          ;; (warn "resolve-interceptor-data" spec data)
          [data])))

    #{::error}
    nil))

(defn interceptor-fn-history-thunk
  "returns a [<history-entry> <interceptor-fn-thunk>]"
  [interceptor-fn-key
   interceptor-spec
   context
   error]

  (let [{interceptor-kw ::key
         interceptor-data-specs ::data} (normalize-interceptor-spec
                                         interceptor-spec)

        interceptor (resolve-interceptor interceptor-kw)]

    (if-let [f (get interceptor interceptor-fn-key)]

      (condp contains? interceptor-fn-key

        #{::enter ::leave}
        (let [[data-val :as data] (resolve-interceptor-data
                                   interceptor-fn-key
                                   interceptor-data-specs
                                   context)

              thunk (if (some? data)
                      (fn [ctx] (f ctx data-val))
                      (fn [ctx] (f ctx)))]

          [(if (some? data)
             [interceptor-spec interceptor-fn-key data-val]
             [interceptor-spec interceptor-fn-key])
           thunk])

        #{::error}
        [[interceptor-spec
          interceptor-fn-key]
         (fn [ctx] (f ctx error))])

      ;; no interceptor fn, so no thunk
      [[interceptor-kw ::noop interceptor-fn-key]])))

(defn maybe-execute-interceptor-fn-thunk
  [thunk
   context]
  (if (some? thunk)
    (thunk context)
    context))

(defn maybe-execute-interceptor-fn
  "call an interceptor fn on an interceptor, resolving
   any supplied data"
  [interceptor-fn-key
   interceptor-spec
   context
   error]
  (let [[_ thunk] (interceptor-fn-history-thunk
                   interceptor-fn-key
                   interceptor-spec
                   context
                   error)]
    (maybe-execute-interceptor-fn-thunk thunk context)))

(defn wrap-interceptor-error
  "wrap an exception and add to the interceptor errors"
  [resume-context
   new-context
   e]
  (-> new-context
      terminate
      ;; add an error referencing the context,
      ;; which can be used to resume at the
      ;; point of failure
      (update ::errors conj (wrap-error resume-context e))))

;; processing fns

(s/defn enter-next
  "Executes the next `::enter` interceptor queued within `context`, returning a
  promise that will resolve to the next [[pr-loop-context]] action to take"
  [{queue ::queue
    stack ::stack
    _history ::history
    :as context} :- InterceptorContext]

  (if (empty? queue)
    (return-pr
     [::break context])

    (let [interceptor-spec (first queue)

          [history thunk] (interceptor-fn-history-thunk
                           ::enter
                           interceptor-spec
                           context
                           nil)

          new-context (-> context
                          (assoc ::queue (vec (rest queue)))
                          (assoc ::stack (conj stack interceptor-spec))
                          (update ::history conj history))]

      (-> (prpr/always-pr
           (maybe-execute-interceptor-fn-thunk thunk new-context))

          (prpr/catchall
           (partial wrap-interceptor-error context new-context))

          (prpr/chain-pr
           (fn [{queue ::queue :as c}]
             (if (empty? queue)
               [::break c]
               [::recur c])))))))

(s/defn ^:always-validate enter-all
  "Process the `:queue` of `context`, calling each `:enter` `fn` in turn.

  If an error is raised it is captured, stored in the `context`s `:error` key,
  and the queue is cleared (to prevent further processing.)"
  [context :- InterceptorContext]
  (pr-loop-context context enter-next))

(s/defn leave-next
  "Executes the next `::leave` or `::error` interceptor on the stack within
  `context`, returning a promise that will resolve to the next
  [[pr-loop-context]] action to take"
  [{stack ::stack
    _history ::history
    [error :as _errors] ::errors
    :as context} :- InterceptorContext]

  (if (empty? stack)
    (return-pr
     [::break context])

    (let [interceptor-spec (peek stack)

          interceptor-fn-key (if (some? error) ::error ::leave)

          [history thunk] (interceptor-fn-history-thunk
                           interceptor-fn-key
                           interceptor-spec
                           context
                           error)

          new-context (-> context
                          (assoc ::stack (pop stack))
                          (update ::history conj history))]

      (-> (prpr/always-pr
           (maybe-execute-interceptor-fn-thunk thunk new-context))

          (prpr/catchall
           (partial wrap-interceptor-error context new-context))

          (prpr/chain-pr
           (fn [{stack ::stack :as c}]
             (if (empty? stack)
               [::break c]
               [::recur c])))))))

(s/defn ^:always-validate leave-all
  "Process the `::stack` of `context`, calling, in LIFO order.

  If an `::error` is present in the `context` then the `::error` handling `fn`
  of the interceptors will be called otherwise the `:leave` `fn`s will be
  called.

  Any thrown errors will replace the current `::error` with stack unwinding
  continuing from that point forwards."
  [context :- InterceptorContext]
  (pr-loop-context context leave-next))

;; the main interaction fn

(defn default-error-handler
  [e]
  (throw e))

(defn default-suppressed-error-handler
  [errors]
  (warn (str "suppressed (" (count errors) ")"
                 " errors from interceptor execution"))
      (doseq [e errors]
        (warn e)))

(s/defn execute*
  ([context :- InterceptorContext]
   (execute*
    default-error-handler
    default-suppressed-error-handler
    context))

  ([error-handler
    suppressed-error-handler
    context :- InterceptorContext]

   (prpr/chain-pr
      (enter-all context)
      leave-all
      (fn [{[error & errors] ::errors :as c}]
        (if (some? error)
          (do
            (when (seq errors)
              (prpr/catch-log
               "suppressed-error-handler"
               (constantly nil)
               (suppressed-error-handler errors)))
            (prpr/return-pr (error-handler error)))
          (prpr/return-pr c))))))

(s/defn ^:always-validate execute
  "Returns a Promise encapsulating the execution of the given [[InterceptorContext]].

  Runs all `:enter` interceptor fns (in FIFO order) and then all `:leave` fns
  (in LIFO order) returning the end result `context` map.

  If an error occurs during execution `:enter` processing is terminated and the
  `:error` handlers of all executed interceptors are called (in LIFO order),
  with the original error wrapped in an ex-info with ex-data containing the
  ::context at the point of failure - this can be used to resume processing
  from the failure point

  If the resulting `context` _still_ contains an error after this processing it
  will be re-thrown when the execution promise is realised. "
  ([app-ctx
    a-frame-router
    interceptor-chain :- [InterceptorSpec]
    initial-context]
   (->> (initiate app-ctx
                  a-frame-router
                  interceptor-chain
                  initial-context)
        (execute*))))

(defn resume
  "resume a failed interceptor chain, from either
   a thrown exception or a logged resume-context"
  ([app-ctx
    a-frame-router
    err-or-resume-context]
   (resume
    app-ctx
    a-frame-router
    default-error-handler
    default-suppressed-error-handler
    err-or-resume-context))

  ([app-ctx
    a-frame-router
    error-handler
    suppressed-error-handler
    err-or-resume-context]
   (let [ctx (if (map? err-or-resume-context)
               err-or-resume-context
               (get (ex-data err-or-resume-context) ::context))]

     (when (nil? ctx)
       (throw
        (ex-info
         "no resume context in ex-data"
         {:err-or-resume-context err-or-resume-context})))

     (execute*
      error-handler
      suppressed-error-handler
      (assoc-opaque-keys ctx app-ctx a-frame-router)))))

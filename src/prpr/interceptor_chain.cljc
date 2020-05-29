(ns prpr.interceptor-chain
  (:require #?(:clj [manifold.deferred :as d])
            #?(:cljs [promesa.core :as p])
            [prpr.promise :as prpr]
            [schema.core :as s]
            [taoensso.timbre :refer [warn]]))

;; schema

(s/defschema InterceptorFn
  (s/pred fn?))

(s/defschema Interceptor
  "An Interceptor, all methods are optional but should be implemented as
  follows:

  * `:enter` – takes a single arg—the current context—and returns a new context
  * `:leave` – takes a single arg—the current context—and returns a new context
  * `:error` – takes two args: the current context and the last thrown error and
    returns a new context

  All methods may return either promises or plain values."
  {(s/optional-key :name) s/Keyword
   (s/optional-key :enter) InterceptorFn
   (s/optional-key :leave) InterceptorFn
   (s/optional-key :error) InterceptorFn
   s/Any s/Any})

(s/defschema InterceptorContext
  {:interceptor/queue (s/constrained [Interceptor] vector?)
   :interceptor/stack (s/constrained [Interceptor] list?)
   (s/optional-key :interceptor/errors) [s/Any]
   s/Any s/Any})

;; utility fns

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

(s/defn ^:always-validate initiate
  :- InterceptorContext
  "Given a sequence of [[Interceptor]]s and a map of `initial-context` values,
  returns a new [[InterceptorContext]] ready to [[execute]]"
  [interceptor-chain :- [Interceptor]
   initial-context]
  (merge
   initial-context
   {:interceptor/queue (vec interceptor-chain)
    :interceptor/stack '()}))

(s/defn ^:always-validate enqueue
  :- InterceptorContext
  "Adds `interceptors` to the end of the interceptor queue within `context`"
  [context :- InterceptorContext
   interceptors :- [Interceptor]]
  (update context :interceptor/queue into interceptors))

(s/defn ^:always-validate terminate
  :- InterceptorContext
  "Removes all queued interceptors from `context`"
  [context :- InterceptorContext]
  (assoc context :interceptor/queue []))

(s/defn ^:always-validate clear-errors
  :- InterceptorContext
  "Removes any associated `:interceptor/errors` from `context`"
  [context :- InterceptorContext]
  (dissoc context :interceptor/errors))

;; processing fns

(s/defn enter-next
  "Executes the next `:enter` interceptor queued within `context`, returning a
  promise that will resolve to the next [[pr-loop-context]] action to take"
  [context :- InterceptorContext]
  (let [{queue :interceptor/queue
         stack :interceptor/stack} context
        {f :enter :as interceptor} (first queue)
        new-stack (if (some? interceptor)
                    (conj stack interceptor)
                    stack)
        new-context (-> context
                        (assoc :interceptor/queue (vec (rest queue)))
                        (assoc :interceptor/stack new-stack))]
    (-> (prpr/always-pr
         (if (fn? f)
           (f new-context)
           (prpr/success-pr new-context)))
        (prpr/catchall
         (fn [e]
           (-> new-context
               terminate
               (update :interceptor/errors conj e))))
        (prpr/chain-pr
         (fn [{queue :interceptor/queue :as c}]
           (if (empty? queue)
             [::break c]
             [::recur c]))))))

(s/defn ^:always-validate enter-all
  "Process the `:queue` of `context`, calling each `:enter` `fn` in turn.

  If an error is raised it is captured, stored in the `context`s `:error` key,
  and the queue is cleared (to prevent further processing.)"
  [context :- InterceptorContext]
  (pr-loop-context context enter-next))

(s/defn leave-next
  "Executes the next `:leave` or `:error` interceptor on the stack within
  `context`, returning a promise that will resolve to the next
  [[pr-loop-context]] action to take"
  [context :- InterceptorContext]
  (let [{stack :interceptor/stack
         [error :as errors] :interceptor/errors} context
        {leave-f :leave
         error-f :error
         :as interceptor} (peek stack)
        new-stack (if (empty? stack)
                    stack
                    (pop stack))
        new-context (assoc context :interceptor/stack new-stack)
        f (if (some? error) error-f leave-f)]
    (-> (prpr/always-pr
         (if (fn? f)
           (if (some? error)
             (f new-context error)
             (f new-context))
           (prpr/success-pr new-context)))
        (prpr/catchall
         (fn [e]
           (-> new-context
               (update :interceptor/errors conj e))))
        (prpr/chain-pr
         (fn [{stack :interceptor/stack :as c}]
           (if (empty? stack)
             [::break c]
             [::recur c]))))))

(s/defn ^:always-validate leave-all
  "Process the `:stack` of `context`, calling, in LIFO order.

  If an `:error` is present in the `context` then the `:error` handling `fn` of
  the interceptors will be called otherwise the `:leave` `fn`s will be called.

  Any thrown errors will replace the current `:error` with stack unwinding
  continuing from that point forwards."
  [context :- InterceptorContext]
  (pr-loop-context context leave-next))

;; the main interaction fn

(s/defn ^:always-validate execute
  "Returns a Promise encapsulating the execution of the given [[InterceptorContext]].

  Runs all `:enter` interceptor fns (in FIFO order) and then all `:leave` fns
  (in LIFO order) returning the end result `context` map.

  If an error occurs during execution `:enter` processing is terminated and the
  `:error` handlers of all executed interceptors are called (in LIFO order.) If
  the resulting `context` _still_ contains an error after this processing it
  will be re-thrown when the execution promise is realised.

  As a convenience an `interceptor-chain` and `initial-context` map may be given
  instead of an initialised `context`. A new context will be created from them
  and then executed."
  ([interceptor-chain :- [Interceptor]
    initial-context]
   (->> (initiate interceptor-chain initial-context)
        execute))
  ([context :- InterceptorContext]
   (execute
    (fn [e] (throw e))
    (fn [errors]
      (warn (str "suppressed (" (count errors) ")"
                 " errors from interceptor execution"))
      (doseq [e errors]
        (warn e)))
    context))
  ([error-handler
    suppressed-error-handler
    context :- InterceptorContext]
   ;; NOTE: you’d think this should be a straightforward `chain-pr` and it would
   ;; be _except_ ClojureScript throws "No context is set." errors so we have to
   ;; use a `ddo` block instead
   (prpr/ddo
    [after-enter (enter-all context)
     after-leave (leave-all after-enter)
     :let [{[error & errors] :interceptor/errors :as c} after-leave]]
    (if (some? error)
      (do
        (when (seq errors)
          (prpr/catch-log
           "suppressed-error-handler"
           (constantly nil)
           (suppressed-error-handler errors)))
        (prpr/return (error-handler error)))
      (prpr/return c)))))

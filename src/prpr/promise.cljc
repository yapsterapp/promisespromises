(ns prpr.promise
  #?(:cljs (:require-macros [prpr.promise :refer [ddo]]
                            [schema.macros :as s.macros]))
  (:require
   [taoensso.timbre
    #?@(:clj [:refer [debug info warn error]]
        :cljs [:refer-macros [debug info warn error]])]
   [cats.core
    #?@(:clj [:refer [mlet]]
        :cljs [:refer-macros [mlet]])]
   [cats.context
    #?@(:clj [:refer [with-context]]
        :cljs [:refer-macros [with-context]])]
   [prpr.promise.platform :as platform]
   [prpr.util.variant :as vnt]
   #?(:clj [prpr.util.macro])
   [schema.core :as s]
   #?(:clj [schema.macros :as s.macros])
   [schema.utils :as s.utils]))

(defn exception?
  "true if the arg is a platform exception"
  [v]
  (platform/ex? v))

(defn promise?
  [v]
  (platform/pr? v))

(s/defschema Promise
  (s/pred promise? 'promise?))

;; for convenience
(def return cats.core/return)

(def pr-context prpr.promise.platform/pr-context)

#?(:clj
   (defmacro ddo
     "mlet with a deferred or pr-context"
     [& body]
     `(cats.context/with-context
        prpr.promise.platform/pr-context
        (cats.core/mlet ~@body))))

#?(:clj
   (defmacro dado
     "alet with a deferred or pr-context"
     [& body]
     `(cats.context/with-context
        prpr.promise.platform/pr-context
        (cats.core/alet ~@body))))

(defn error-ex
  "encodes an error-variant on to an ex-info"
  ([error]
   (cond
     (vector? error) (let [[tag value cause] error]
                       (error-ex tag value cause))
     (vnt/is-map-variant? error) (ex-info
                                  (-> error :tag str)
                                  error)
     (keyword? error) (error-ex error nil nil)
     :else (error-ex ::unknown-error error nil)))
  ([tag value] (error-ex tag value nil))
  ([tag value cause]
   (ex-info (str tag)
            (vnt/convert-vector-variant-to-map
             [tag value])
            cause)))

(defn success-pr
  "creates a promise containing the value"
  [value]
  (prpr.promise.platform/pr-success value))

(defn return-pr
  "monad/return with an explicit promise context"
  [value]
  (cats.core/return
   prpr.promise.platform/pr-context
   value))

(defn error-pr
  "creates an errored promise encoding the error-variant"
  ([error]
   (if (vector? error)
     (apply error-pr error)
     (platform/pr-error error)))
  ([tag value] (error-pr tag value nil))
  ([tag value cause]
   (platform/pr-error (error-ex tag value cause))))

(defn factory-pr
  "promesa-style promise creation - the factory-cb callback will
   receive resolve and reject fns to complete the promise"
  [factory-cb]
  (platform/pr-factory factory-cb))

(defn deferred-pr
  []
  (platform/pr-deferred))

(defn resolve!-pr
  [p v]
  (platform/pr-resolve! p v))

(defn chain-pr
  [p & fs]
  (apply platform/pr-chain p fs))

(defn branch-pr
  [p success-fn error-fn]
  (platform/pr-branch p success-fn error-fn))

(defn all-pr
  [& ps]
  (platform/pr-all ps))

(defn delay-pr
  [delay-ms value]
  (platform/pr-delay delay-ms value))

(defn timeout-pr
  ([p timeout-ms]
   (platform/pr-timeout p timeout-ms))
  ([p timeout-ms timeout-val]
   (platform/pr-timeout p timeout-ms timeout-val)))

(defn capture-pr-atom
  "debug fn which may be useful in environments where
   promise values can't be inspected - returns an atom
   which will receive a variant with the eventual value
   or error of the promise"
  [p]
  (let [r (atom ::none)]
    (branch-pr
     p
     #(reset! r [:ok %])
     #(reset! r [:error %]))
    r))

(defn decode-error-value
  "Decodes an error value to a variant: [tag value].

   If the error-value is already a variant, or an ex-info with a
  variant, it returns that directly, otherwise you will get an
   [::unknown-error] variant.

  If the input is an Exception/Error, it'll be attached to the
  variant's value metadata under :exception key.

  See [[error-variant-exception]]."
  [v]
  (let [exd (ex-data v)
        vary-meta* (fn [value]
                     (if (map? value)
                       (vary-meta value assoc :exception v)
                       value))]
    (cond
      (and exd (vnt/is-map-variant? exd))
      (-> (vnt/convert-map-variant-to-vector exd)
          (update 1 vary-meta*))

      (vnt/is-variant? v)
      v

      (vnt/is-map-variant? v)
      (vnt/convert-map-variant-to-vector v)

      ;; we hide the error in meta data to avoid
      ;; accidental leak of stacktraces to clients
      :else
      [::unknown-error
       (cond-> {:error  (if (= :schema.core/error (:type exd))
                          "#error Input does not match schema"
                          "#error Unknown Error")}
         #?(:clj (instance? Throwable v)
            :cljs (instance? js/Error v))
         (vary-meta assoc :exception v))])))

(defn error-variant-exception
  "Returns an exception attached to the error variant if possible, otherwise nil."
  [v]
  (when (and (vnt/is-variant? v) (map? (second v)))
    (-> v (nth 1) meta :exception)))

#?(:clj
   (defmacro finally
     "leakproof finally for promises - catches any errors
      during evaluation of the first arg promise, chains
      nicely"
     [pr callback]
     `(prpr.promise.platform/pr-finally
       (prpr.util.macro/try-catch
        ~pr
        (catch
            x#
            (prpr.promise.platform/pr-error x#)))
       ~callback)))

#?(:clj
   (defmacro catchall
     "leakproof catch for promises, with a promise first-arg so it
      threads/chains nicely"
     [pr error-handler]
     `(prpr.promise.platform/pr-catch
       (prpr.util.macro/try-catch
        ~pr
        (catch
            x#
            (prpr.promise.platform/pr-error x#)))
       (fn [e#] (~error-handler e#)))))

(defn handle-tag
  "if the tag-handlers have a handler for the err's tag,
   return the application of that handler to the err,
   otherwise rethrow the err

   we apply the handler to the err rather than the err's value
   so that information encoded in the err (stack, message) is
   available"
  [tag-handlers err]
  (let [[tag _data] (decode-error-value err)]
    (if (contains? tag-handlers tag)
      ((get tag-handlers tag) err)
      (throw err))))

#?(:clj
   (defmacro catch-tag
     "leakproof catch of specific tags for promises. un-handled errors
      will continue to propagate"
     [pr tag-handlers]
     `(catchall
       ~pr
       (partial handle-tag ~tag-handlers))))

#?(:clj
   (defmacro catchall-log
     "catchall, with some logging"
     [pr error-handler errordesc]
     `(catchall
       ~pr
       (fn [e#]
         (error e# ~errordesc)
         (~error-handler e#)))))

#?(:clj
   (defmacro catchall-variant
     "leakproof catch, returning an
     [:ok <result>] or
     [<error-tag> <error-desc>] variant"
     [pr]
     `(catchall
       (chain-pr ~pr (fn [r#] [:ok r#]))
       decode-error-value)))

#?(:clj
   (defmacro catchall-log-variant
     "leakproof catch, logging any error and
      returning an
     [:ok <result>] or
     [<error-tag> <error-desc>] variant"
     [pr errordesc]
     `(catchall
       (chain-pr ~pr (fn [r#] [:ok r#]))
       (fn [e]
         (error e errordesc)
         (decode-error-value e)))))

#?(:clj
   (defmacro catchall-rethrowable
     "leakproof catch, returning an
     [:ok <result>] or
     [:error <error-value>] variant which preserves any stack
     when the error-value is an Exception, and can be used
     with return-or-rethrow"
     [pr]
     `(catchall
       (chain-pr ~pr (fn [r#] [:ok r#]))
       (fn [e#] [:error e#]))))

(defn return-or-rethrow
  "takes the result of catchall-rethrowable,
   returns if it was successful and errors if not"
  [[tag result-or-error]]
  (if (= :ok tag)
    (platform/pr-success result-or-error)
    (platform/pr-error result-or-error)))

#?(:clj
   (defmacro always-pr
     "wraps body in a promise, even if the body exprs throw
      during evaluation"
     [& body]
     `(prpr.util.macro/try-catch
       (let [r# ~@body]
         (if (prpr.promise.platform/pr? r#)
           r#
           (prpr.promise.platform/pr-success r#)))
       (catch
           x#
           (prpr.promise.platform/pr-error x#)))))

#?(:clj
   (defmacro catch
     "catches any errors and returns the result of
      applying the error-handler to the error-value"
     {:style/indent [0]}
     [error-handler & body]
     `(prpr.promise.platform/pr-catch
       (prpr.util.macro/try-catch
        ~@body
        (catch
            x#
            (prpr.promise.platform/pr-error x#)))
       (fn [e#] (~error-handler e#)))))

#?(:clj
   (defmacro catch-log
     "catches any errors, logs them and returns the
      result of applying the error-handler to the error-value"
     [description error-handler & body]
     `(prpr.promise.platform/pr-catch
       (prpr.util.macro/try-catch
        ~@body
        (catch
            x#
            (prpr.promise.platform/pr-error x#)))
       (fn [e#]
         (warn e# ~description)
         (~error-handler e#)))))

#?(:clj
   (defmacro catch-error
     "catches any errors and returns the results of decode-error-value
      on the error value. has to be a macro to catch any exceptions
      in the initial evaluation of the body"
     [body]
     `(prpr.promise.platform/pr-catch
       (prpr.util.macro/try-catch
        ~body
        (catch
            x#
            (prpr.promise.platform/pr-error x#)))
       (fn [e#] (decode-error-value e#)))))

#?(:clj
   (defmacro wrap-catch-error
     "wraps any normal responses in a [tag <response>] variant, and
      catches any errors and returns the results of decode-error-value
      on the error value. has to be a macro to catch any exceptions
      in the initial evaluation of the body"
     ([body] `(wrap-catch-error :ok ~body))
     ([tag body]
      `(prpr.promise.platform/pr-catch
        (prpr.util.macro/try-catch
         (ddo [r# ~body]
           [~tag r#])
         (catch
             x#
             (prpr.promise.platform/pr-error x#)))
        (fn [e#] (decode-error-value e#))))))

#?(:clj
   (defmacro wrap-catch-error-log
     "wraps any normal responses in a [tag <response>] variant, and
      catches any errors and returns the results of decode-error-value
      on the error value. has to be a macro to catch any exceptions
      in the initial evaluation of the body"
     ([description body]
      `(wrap-catch-error-log
        ~description :ok ~body))
     ([description tag body]
      `(prpr.promise.platform/pr-catch
        (prpr.util.macro/try-catch
         (ddo [r# ~body]
           [~tag r#])
         (catch
             x#
             (prpr.promise.platform/pr-error x#)))
        (fn [e#]
          (let [v# (decode-error-value e#)]
            (warn e# ~description v#)
            v#))))))

#?(:clj
   (defmacro catch-error-log
     "If body is an error, logs a warning and returns the decoded error
  variant"
     [description body]
     `(prpr.promise.platform/pr-catch
       (prpr.util.macro/try-catch
        ~body
        (catch
            x#
            (prpr.promise.platform/pr-error x#)))
       (fn [e#]
         (let [v# (decode-error-value e#)]
           (warn e# ~description v#)
           v#)))))

#?(:clj
   (defmacro catch-xform-error
     "catches any errors and returns the results of
      (error-xform (decode-error-value error-value))"
     [error-xform body]
     `(do
        (assert ~error-xform)
        (prpr.promise.platform/pr-catch
         (prpr.util.macro/try-catch
          ~body
          (catch
              x#
              (prpr.promise.platform/pr-error x#)))
         (fn [e#]
           (~error-xform
            (decode-error-value e#)))))))

(defn handle
  "handle a variant
     - handlers : map of tag to handler

  if a handler is not available for the tag then
  an error-ex with tag ::unhandled-tag will be thrown"
  [handlers [tag value]]
  (if-let [h (get handlers tag)]
    (h value)
    (throw (error-ex ::unhandled-tag [tag value]))))

(defn handle-safe
  "safely handle a variant
    - handlers : a map of tag to handler

  if a handler is not available and a default-response fn
  is provided it will be called with the unhandled variant,
  if a default-response value is provided then a warning
  will be logged for the unhandled variant, and the
  default response value will be returned"
  [handlers default-response-or-fn [tag value]]
  (if-let [h (get handlers tag)]
    (h value)
    (if (fn? default-response-or-fn)
      (default-response-or-fn [tag value])

      (do
        (warn "unhandled variant" [tag value])
        default-response-or-fn))))

#?(:clj
   (defmacro catch-handle
     "catch-error and handle - any unhandled tags result
      in an exception"
     [handlers body]
     `(with-context prpr.promise.platform/pr-context
        (mlet [v# (prpr.promise/catch-error ~body)
               :let [r# (prpr.promise/handle ~handlers v#)]]
          (if (prpr.promise.platform/pr? r#)
            r#
            (cats.core/return r#))))))

#?(:clj
   (defmacro catch-handle-safe
     "catch-error and handle - if a tag is unhandled and
      a default-response-fn is provided then it will be called
      with the unhandled variant, if a default response value is
      provided then a warning will be logged and the default
      response value will be returned"
     ([default-response-or-fn body]
      `(catch-handle-safe {} ~default-response-or-fn ~body))
     ([handlers default-response-or-fn body]
      `(with-context prpr.promise.platform/pr-context
         (mlet [v# (prpr.promise/catch-error ~body)
                :let [r# (prpr.promise/handle-safe
                          ~handlers
                          ~default-response-or-fn
                          v#)]]
           (if (prpr.promise.platform/pr? r#)
             r#
             (cats.core/return r#)))))))

#?(:clj
   (defmacro ddefn
     "Like schema.core/defn, except that the fn will *always* be expected to
     return a promise and any given output schema will be validated against the
     value returned *inside* of that promise.

     ## Example Use

     ```clojure
     (require '[prpr.promise :as pr])
     (require '[schema.core :as s])

     (defn double-if-int [x] (if (int? x) (* x x) x))

     (pr/ddefn promise-test-fn
       :- s/Int
       [x]
       (if (= :dont-defer x)
         x
         (pr/always-pr
           (double-if-int x))))

     (s/set-fn-validation! true)

     @(promise-test-fn 2)
     ;;=> 4

     @(promise-test-fn \"2\")
     ;;=> error! \"not integer\"

     (promise-test-fn :dont-defer)
     ;;=> error! \"not a promise\"
     ```

     ## Notes

     Behind the scenes the macro creates *three* functions and chains them
     together to achieve the desired validation:

     1. `<fn-name>-call-validate-as-promise` – has the provided fn body and
        *always* validates its output as being a `Promise`.
     2. `<fn-name>-validate-realized-value` – essentially an `identity` fn that
        will validate the given value as matching the specified fn output schema
        (but only when the normal schema validation rules indicate it should.)
     3. `<fn-name>` – an ordinary `defn` that calls and chains the `*-call-*` fn
        through the `*-validate-*` fn.

     As noted above the ‘inner’ promise producing fn will always validate its
     output. If you tag the `ddefn` with `^:always-validate` then the ‘outer’ value
     validation fn will also always validate to the provided value schema
     (otherwise it will only valdate when schema fn validation is turned on.)

     If a docstring is provided it will be used for the `<fn-name>` def."
     {:style/indent :defn}
     [& defn-args]
     (let [[name & fn-args] (s.macros/normalized-defn-args &env defn-args)
           {always-validate? :always-validate} (meta name)
           output-schema (s.macros/extract-schema-form name)
           {arglists :arglists} (s.macros/process-fn- &env name fn-args)
           call-and-validate-promise-name (with-meta
                                            (symbol (str name "-call-and-validate-as-promise"))
                                            {:always-validate true})
           validate-realized-value-name (with-meta
                                          (symbol (str name "-validate-realized-value"))
                                          {:always-validate always-validate?})]
       `(do
          (s/defn ~call-and-validate-promise-name :- Promise ~@fn-args)
          (s/defn ~validate-realized-value-name :- ~output-schema [v#] v#)
          (defn ~name
            {:arglists (quote ~arglists)}
            [& args#]
            (chain-pr
             (apply ~call-and-validate-promise-name args#)
             ~validate-realized-value-name))))))

(ns prpr.promise
  #?(:cljs (:require-macros [prpr.promise :refer [ddo]]))
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
   #?(:clj [prpr.util.macro])))

(defn exception?
  "true if the arg is a platform exception"
  [v]
  (platform/ex? v))

(defn promise?
  [v]
  (platform/pr? v))

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
  ([[tag value]] (error-ex tag value))
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
  ([[tag value]] (error-pr tag value))
  ([tag value] (error-pr tag value nil))
  ([tag value cause]
   (platform/pr-error (error-ex tag value cause))))

(defn factory-pr
  "promesa-style promise creation - the factory-cb callback will
   receive resolve and reject fns to complete the promise"
  [factory-cb]
  (platform/pr-factory factory-cb))

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
  "decodes an error value to a variant. if the error-value
   is already a variant, or an ex-info with a variant,
   it returns that directly, otherwise you will get an
   [::unknown-error] variant"
  [v]
  (let [exd (ex-data v)]
    (cond
      (and exd (vnt/is-map-variant? exd))
      (vnt/convert-map-variant-to-vector exd)

      (vnt/is-variant? v)
      v

      (vnt/is-map-variant? v)
      (vnt/convert-map-variant-to-vector v)

      ;; we print any error to avoid getting
      ;; undeserializable #error tags in the EDN
      :else
      [::unknown-error {:error (with-out-str (print v))}])))

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
            (warn ~description v#)
            v#))))))

#?(:clj
   (defmacro catch-error-log
     [description body]
     `(prpr.promise.platform/pr-catch
       (prpr.util.macro/try-catch
        ~body
        (catch
            x#
            (prpr.promise.platform/pr-error x#)))
       (fn [e#]
         (let [v# (decode-error-value e#)]
           (warn ~description v#)
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

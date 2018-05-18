(ns prpr.promise
  (:require
   [taoensso.timbre
    #?@(:clj [:refer [debug info warn error]]
        :cljs [:refer-macros [debug info warn error]])]
   [cats.core
    #?@(:clj [:refer [mlet return]]
        :cljs [:refer-macros [mlet] :refer [return]])]
   [cats.context
    #?@(:clj [:refer [with-context]]
        :cljs [:refer-macros [with-context]])]
   [prpr.promise.platform :as platform
    :refer [pr-context pr-catch pr-error]]
   [prpr.util.variant :as vnt]
   #?(:clj [prpr.util.macro])))

(defn exception?
  "true if the arg is a platform exception"
  [v]
  (platform/ex? v))

(defn promise?
  [v]
  (platform/pr? v))

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
  ([tag value]
   (ex-info (str tag)
            (vnt/convert-vector-variant-to-map
             [tag value]))))

(defn success-pr
  "creates a promise containing the value"
  [value]
  (prpr.promise.platform/pr-success value))

(defn error-pr
  "creates an errored promise encoding the error-variant"
  ([[tag value]] (error-pr tag value))
  ([tag value]
   (platform/pr-error (error-ex tag value))))

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
     "finally for promises - makes sure to run the finally
      even if promise chain init throws. some exceptions
      might slip through the net - non-Exception Throwables
      on the jvm for example - but we are probably in deeper
      trouble anyway if that happens"
     [callback & body]
     `(try
        (prpr.promise.platform/pr-finally
         (do ~@body)
         ~callback)
        (catch Exception x#
          (~callback)
          (prpr.promise.platform/pr-error x#)))))

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

(ns prpr.error
  (:refer-clojure :exclude [ex-info])
  (:require
   [clojure.core :as clj]
   #?(:clj [clojure.pprint :as pprint])
   [prpr.error.protocols :as pt]))

(defn ex-info
  ([type]
   (ex-info type {} nil))
  ([type map]
   (ex-info type map nil))
  ([type map cause]
   (clj/ex-info
    (str type)
    (assoc map :error/type type)
    cause)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; define UncaughtErrorWrapper and CaughtErrorWrapper types
;;; to wrap javascript errors, preventing auto-rejection of promises
;;; when returnin an error as the value
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;


;; UncaughtErrorWrapper is an error which is wrapped with the intention
;; that it should be eventually rethrown (when unwrapped)
(deftype UncaughtErrorWrapper [err]
  pt/IErrorWrapper
  (-unwrap [_] (throw err))
  (-unwrap-value [_] err)

  ;; #?@(:clj [clojure.lang.IDeref
  ;;           (deref [_] err)]
  ;;     :cljs [IDeref
  ;;            (-deref [_] err)])
  )

#?(:clj
   (defn print-uncaught-error-wrapper
     [uew ^java.io.Writer w]
     (.write w "#UncaughtErrorWrapper [")
     (print-method (pt/-unwrap uew))
     (.write w "]")))

#?(:clj
   (defmethod print-method UncaughtErrorWrapper [this ^java.io.Writer w]
     (print-uncaught-error-wrapper this w)))

#?(:clj
   (defmethod print-dup UncaughtErrorWrapper [this ^java.io.Writer w]
     (print-uncaught-error-wrapper this w)))

#?(:clj
   (.addMethod pprint/simple-dispatch
               UncaughtErrorWrapper
               (fn [uew]
                 (print-uncaught-error-wrapper uew *out*))))

#?(:cljs
   (extend-protocol IPrintWithWriter
     UncaughtErrorWrapper
     (-pr-writer [uew writer _]
       (write-all writer "#UncaughtErrorWrapper[" (pt/-unwrap uew) "]"))))


;; CaughtErrorWrapper is an error which is wrapped with the intention
;; that it should be treated as a value and *not* rethrown when unwrapped
(deftype CaughtErrorWrapper [err]
  pt/IErrorWrapper
  (-unwrap [_] err)
  (-unwrap-value [_] err)

  ;; #?@(:clj [clojure.lang.IDeref
  ;;           (deref [_] err)]
  ;;     :cljs [IDeref
  ;;            (-deref [_] err)])
  )

#?(:clj
   (defn print-caught-error-wrapper
     [uew ^java.io.Writer w]
     (.write w "#CaughtErrorWrapper [")
     (print-method (pt/-unwrap uew))
     (.write w "]")))

#?(:clj
   (defmethod print-method CaughtErrorWrapper [this ^java.io.Writer w]
     (print-caught-error-wrapper this w)))

#?(:clj
   (defmethod print-dup CaughtErrorWrapper [this ^java.io.Writer w]
     (print-caught-error-wrapper this w)))

#?(:clj
   (.addMethod pprint/simple-dispatch
               CaughtErrorWrapper
               (fn [uew]
                 (print-caught-error-wrapper uew *out*))))

#?(:cljs
   (extend-protocol IPrintWithWriter
     CaughtErrorWrapper
     (-pr-writer [uew writer _]
       (write-all writer "#CaughtErrorWrapper[" (pt/-unwrap uew) "]"))))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; implement UncaughtErrorWrapper and CaughtErrorWrapper types
;;; behaviours
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(extend-protocol pt/IErrorWrapper
  #?(:clj Object :cljs default)
  (-unwrap [this] this)
  (-unwrap-value [this] this)

  nil
  (-unwrap [_this] nil)
  (-unwrap-value [_this] nil))

(defn uncaught-wrapper?
  [v]
  (instance? UncaughtErrorWrapper v))

(defn caught-wrapper?
  [v]
  (instance? CaughtErrorWrapper v))

(defn wrapper?
  [v]
  (or
   (instance? UncaughtErrorWrapper v)
   (instance? CaughtErrorWrapper v)))

(defn wrap-uncaught
  [err]
  (UncaughtErrorWrapper. err))

(defn wrap-caught
  [err]
  (CaughtErrorWrapper. err))

(defn unwrap
  "unwrap and maybe throw"
  [e]
  (pt/-unwrap e))

(defn unwrap-value
  "unwrap value, never throwing"
  [e]
  (pt/-unwrap-value e))

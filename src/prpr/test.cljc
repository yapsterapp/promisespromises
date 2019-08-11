(ns prpr.test
  #?@(:clj [(:require
             [prpr.util.macro :refer [if-cljs]]
             [clojure.test :as t]
             [prpr.promise :as prpr]
             [taoensso.timbre :refer [warn]])]
      :cljs [(:require
              [prpr.promise :as prpr]
              [cljs.test])
             (:require-macros
              [prpr.util.macro :refer [if-cljs]]
              [cljs.test :refer [deftest async]]
              [prpr.promise :refer [catch-error-log ddo]]
              [taoensso.timbre :refer [warn]])]))

;; lord help me
#?(:clj
   (defonce test-binding-frame (atom nil)))

#?(:clj
   (defmacro record-test-binding-frame
     [& body]
     ;; pass the actual original binding frame around, not a clone,
     ;; so that any changes on other threads make their way back to the
     ;; original thread
     `(let [orig-frame# (clojure.lang.Var/getThreadBindingFrame)]
        (try
          (reset! test-binding-frame orig-frame#)
          ~@body
          (finally
            (reset! test-binding-frame nil))))))

#?(:clj
   (defmacro with-test-binding-frame
     [& body]
     `(let [curr-frame# (clojure.lang.Var/getThreadBindingFrame)]
        (when (nil? @test-binding-frame)
          (throw
           (ex-info
            "test-async missing ? (nil test-binding-frame)"
            {})))
        (try
          (clojure.lang.Var/resetThreadBindingFrame @test-binding-frame)
          ~@body
          (finally
            (clojure.lang.Var/resetThreadBindingFrame curr-frame#))))))

;; a bunch of test macros for comparable async testing
;; in clj or cljs... some just reference the underlying clojure.test
;; macros, but are here to make ns :require forms simpler...
;; since it's cljc no reader conditionals are required, though we
;; may have to add more macros here to support further
;; clojure.test/cljs.test macros
(comment
  (ns some.ns
    (:require
     [prpr.test :refer [deftest test-async is testing]])))

#?(:clj
   (defmacro use-fixtures
     [& body]
     `(if-cljs
       (cljs.test/use-fixtures ~@body)
       (clojure.test/use-fixtures ~@body))))

;; if you make the body of test-async a form or forms that
;; each return a promise... this will complete them
#?(:clj
   (defmacro test-async
     [& ps]
     `(if-cljs
          (cljs.test/async
           done#

           ;; with just catch - promise/finally is currently broken on cljs
           (prpr.promise/catch
               (fn [e#]
                 (taoensso.timbre/warn e#)
                 (cljs.test/report {:type :error
                                    :message (str e#)
                                    :error e#})
                 (done#))
               (prpr.promise/ddo [:let [ps# [~@ps]]
                                  r# (prpr.promise.platform/pr-all ps#)]
                 (done#))))

        (let [body# (fn []
                      (with-test-binding-frame
                        (prpr.promise/all-pr ~@ps)))]
          (record-test-binding-frame
           @(body#))))))

#?(:clj
   (defmacro deftest
     [& body]
     `(if-cljs
       (cljs.test/deftest ~@body)
       (clojure.test/deftest ~@body))))

#?(:clj
   (defmacro is
     [& body]
     `(if-cljs
       (cljs.test/is ~@body)
       (with-test-binding-frame
         (clojure.test/is ~@body)))))

#?(:clj
   (defmacro testing
     [& body]
     `(if-cljs
       (cljs.test/testing ~@body)
       (with-test-binding-frame
         (clojure.test/testing ~@body)))))

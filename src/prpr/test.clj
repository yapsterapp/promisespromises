(ns prpr.test
  (:require
   [clojure.test]
   [prpr.util.macro]
   [promesa.core]
   [taoensso.timbre]
   [prpr.test.reduce]))

;; lord help me

(defonce test-binding-frame (atom nil))

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
         (reset! test-binding-frame nil)))))

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
         (clojure.lang.Var/resetThreadBindingFrame curr-frame#)))))

;; a bunch of test macros for comparable async testing
;; in clj or cljs... some just reference the underlying clojure.test
;; macros, but are here to make ns :require forms simpler...

(defmacro use-fixtures
  [& body]
  `(prpr.util.macro/if-cljs
    (cljs.test/use-fixtures ~@body)
    (clojure.test/use-fixtures ~@body)))

(defmacro test-async
  "the body of test-async is a form or forms that
   each return a plain value or a promise of a value...
   test-async serially completes them - waiting on the
   completion of any promises before proceeding to
   evaluate the next form"
  [& forms]
  (let [;; wrap each form into a 0-args fn
        fs (for [form forms]
             `(fn []
                ~form))]

    `(prpr.util.macro/if-cljs

      (cljs.test/async
       done#

       (promesa.core/handle
        (promesa.core/let [r# (prpr.test.reduce/reduce-pr-fns
                               [~@fs])])
        (fn [succ# e#]
          (when (some? e#)
            (cljs.test/report {:type :error
                               :message (str e#)
                               :error e#}))
          (done#))))

      (let [body# (fn []
                    (with-test-binding-frame
                      (prpr.test.reduce/reduce-pr-fns
                       [~@fs])))]
        (record-test-binding-frame
         @(body#))))))

(defmacro deftest*
  [nm & body]
  `(prpr.util.macro/if-cljs
    (cljs.test/deftest ~nm ~@body)
    (clojure.test/deftest ~nm ~@body)))

(defmacro deftest
  "define a test whose body yields a list of promises, which will
   be resolved before proceeding

   use the same name as clojure.test/deftest because CIDER
   recognizes it and uses it to find tests"
  [name & body]
  `(prpr.test/deftest* ~name
     (println "   " ~(str name))
     (prpr.test/test-async
      ~@body)))

(defmacro is
  [& body]
  `(prpr.util.macro/if-cljs
    (cljs.test/is ~@body)
    (with-test-binding-frame
      (clojure.test/is ~@body))))

(defmacro testing
  "each testing form is expected to have zero or more
   child forms (which may be nested testing forms), each
   of which yields a promise (or plain value), and will
   be evaluated serially in strict depth-first order"
  [s & forms]
  (when (not-empty forms)
    (let [;; wrap each form into a 0-args fn
          fs (for [form forms]
               `(fn [] ~form))]
      `(prpr.util.macro/if-cljs
        (do
          (println "      " ~s)
          (cljs.test/testing
              (prpr.test.reduce/reduce-pr-fns
               [~@fs])))
        (with-test-binding-frame
          (do
            (println "      " ~s)
            (clojure.test/testing ~s
              (prpr.test.reduce/reduce-pr-fns
               [~@fs]))))))))

(defmacro with-log-level
  "temporarily set the log-level while executing the forms"
  [log-level & forms]
  (let [;; wrap each form into a 0-args fn
        fs (for [form forms]
             `(fn [] ~form))]

    `(let [cl# (or (:level taoensso.timbre/*config*)
                   :info)]

       (taoensso.timbre/set-level! ~log-level)

       (promesa.core/finally

         ;; put a fn which can't fail at the head, so that
         ;; we only need the promise-based finally
         (prpr.test.reduce/reduce-pr-fns (into [(constantly true)]
                                               [~@fs]))

         (fn [_# _#]
           (taoensso.timbre/set-level! cl#))))))

(def compose-fixtures
  clojure.test/compose-fixtures)

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
   when evaluated return:

   <plain-value> | Promise<plain-value> | fn | Sequence<fn>

   test-async serially evaluates the forms, and any which yield
   fn or Sequence<fn> will be immediately called, also serially"
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
  "define a test whose body will be evaluated with test-async. it
   looks very like a normal sync deftest, but it is not. there are
   some caveats, viz:

   NOTE: if a let form is used to wrap a series for testing forms
   inside the deftest, then you *must* remember to surround the
   testing forms in a vector (or list), otherwise only the last
   form will be evaluated. you can use the tlet form to do this
   for you

   NOTE: use the same name as clojure.test/deftest because CIDER
   recognizes it and uses it to find tests"
  [name & body]
  `(prpr.test/deftest* ~name
     (println "   " ~(str name))
     (prpr.test/test-async
      ~@body)))

(defmacro tlet
  "a let which puts its body in a vector, to be used inside deftests
   so that all the testing forms inside the let are retained"
  [bindings & body]
  `(let ~bindings
     [ ~@body  ]))

(defmacro is
  [& body]
  `(prpr.util.macro/if-cljs
    (cljs.test/is ~@body)
    (with-test-binding-frame
      (clojure.test/is ~@body))))

(defmacro testing
  "each testing form body is evaluated in the same manner as a deftest
   body"
  [s & forms]
  (when (not-empty forms)
    (let [;; wrap each form into a 0-args fn
          fs (for [form forms]
               `(fn [] ~form))]
      `(fn []
         (prpr.util.macro/if-cljs
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
                 [~@fs])))))))))

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

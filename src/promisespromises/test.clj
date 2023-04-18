(ns promisespromises.test
  (:require
   [clojure.test]
   [promesa.core]
   [taoensso.timbre]
   [promisespromises.test.reduce]
   [promisespromises.util.macro]
   [promisespromises.promise]
   [promisespromises.promise :as prpr]))

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
  `(promisespromises.util.macro/if-cljs
    (cljs.test/use-fixtures ~@body)
    (clojure.test/use-fixtures ~@body)))

(defmacro test-async
  "the body of test-async is a form or forms that
   when evaluated return:

   <plain-value> | Promise<plain-value> | fn | Sequence<fn>

   test-async *serially* evaluates the forms, and any which yield
   fn or Sequence<fn> will be immediately called, also serially"
  [nm & forms]
  (let [;; wrap each form into a 0-args fn
        fs (for [form forms]
             `(fn [] ~form))]

    `(promisespromises.util.macro/if-cljs

      (cljs.test/async
       done#

       (promesa.core/handle
        (do
          (println "   " ~(str nm))
          (promesa.core/let [r# (promisespromises.test.reduce/reduce-pr-fns
                                 ~(str nm)
                                 [~@fs])]))
        (fn [succ# e#]
          (when (some? e#)
            (cljs.test/report {:type :error
                               :message (str e#)
                               :error e#}))
          ;; (println "   " ~(str nm) ":done")
          (done#))))

      (let [body# (fn []
                    (with-test-binding-frame
                      (promisespromises.test.reduce/reduce-pr-fns
                       ~(str nm)
                       [~@fs])))]
        (record-test-binding-frame
         (println "   " ~(str nm))
         @(body#)
         ;; (println "   " ~(str nm) ":done")
         )))))

(defmacro deftest*
  [nm & body]
  `(promisespromises.util.macro/if-cljs
    (cljs.test/deftest ~nm ~@body)
    (clojure.test/deftest ~nm ~@body)))

(defmacro deftest
  "define a test whose body will be evaluated with test-async. it
   looks very like a normal sync deftest, **but it is not**. there are
   some caveats, viz:

   NOTE: when using a let to provide common values to a series of
   testing forms, use a tlet instead - it will wrap the forms
   as a vector of 0-args fns, so none get forgotten (let only
   returns its final value) and they all get evaluated serially

   NOTE: use the same name as clojure.test/deftest because CIDER
   recognizes it and uses it to find tests"
  [nm & body]
  `(promisespromises.test/deftest* ~nm
     (promisespromises.test/test-async ~(str nm) ~@body)))

(defmacro tlet
  "a let which turns its body forms into a vector of 0-args fns,
   to be used inside deftests so that all the testing forms inside
   the let are retained and evaluated serially"
  [bindings & forms]
  (let [;; wrap each form into a 0-args fn
        fs (for [form forms]
             `(fn [] ~form))]
    `(let ~bindings
       [ ~@fs ])))

(defmacro is
  [& body]
  `(promisespromises.util.macro/if-cljs
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
         (promisespromises.util.macro/if-cljs
          (do
            (println "      " ~s)
            ;; underlying testing macro loses the result, which messes up
            ;; our ()->Promise closure reducing method, so explicitly
            ;; capture the result in an atom
            (let [r# (atom nil)]
              (cljs.test/testing
                  (reset! r# (promisespromises.test.reduce/reduce-pr-fns ~s [~@fs])))
              @r#))

          (with-test-binding-frame
            (do
              (println "      " ~s)
            ;; underlying testing macro loses the result, which messes up
            ;; our ()->Promise closure reducing method, so explicitly
            ;; capture the result in an atom
              (let [r# (atom nil)]
                (clojure.test/testing
                    (reset! r# (promisespromises.test.reduce/reduce-pr-fns ~s [~@fs])))
                @r#))))))))

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
         (promisespromises.test.reduce/reduce-pr-fns ~(str log-level) (into [(constantly true)]
                                               [~@fs]))

         (fn [_# _#]
           (taoensso.timbre/set-level! cl#))))))

(defn with-log-level-fixture
  [level]
  (fn [f]
    (let [cl (or (:level taoensso.timbre/*config*) :info)]
      ;; (println "with-log-level-fixture" cl "->" level)
      (taoensso.timbre/set-level! level)

      (try
        (f)
        (finally
          ;; (println "with-log-level-fixture:done" level "->" cl)
          (taoensso.timbre/set-level! cl))))))

(def compose-fixtures
  clojure.test/compose-fixtures)

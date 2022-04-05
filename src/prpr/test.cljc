(ns prpr.test
  #?@(:clj [(:require
             [clojure.test]
             [prpr.util.macro]
             [promesa.core]
             [taoensso.timbre]
             [prpr.test.reduce])]

      :cljs [(:require-macros
              [cljs.test]
              [prpr.util.macro]
              [promesa.core]
              [prpr.test])
             (:require
              [cljs.test]
              [prpr.util.macro]
              [promesa.core]
              [taoensso.timbre]
              [prpr.test.reduce])]))

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
;; may have to add more macros here if we need to support further
;; clojure.test/cljs.test macros
(comment
  (ns some.ns
    (:require
     [prpr.test :refer [defprtest is testing]]))

  ;; this is more complex - but gives better editor support
  ;; with CIDER, which only recognizes tests by the `deftest` form
  (ns other.ns
    (:require
     #?(:clj [prpr.test
              :refer [defprtest testing is]
              :rename {defprtest deftest}]
        :cljs [prpr.test
               :include-macros true
               :refer-macros [defprtest testing is]
               :rename-macros {defprtest deftest}])
     [prpr.nustream :as sut])))

#?(:clj
   (defmacro use-fixtures
     [& body]
     `(prpr.util.macro/if-cljs
       (cljs.test/use-fixtures ~@body)
       (clojure.test/use-fixtures ~@body))))

#?(:clj
   (defmacro test-async
     "if you make the body of test-async a form or forms that
      each return a promise... this will serially
      complete them"
     [& forms]
     (let [;; wrap each form into a 0-args fn
           fs (for [form forms]
                `(fn []
                   ~form))]

       `(prpr.util.macro/if-cljs
         (cljs.test/async
          done#

          (promesa.core/catch
              (fn [e#]
                (taoensso.timbre/warn e#)
                (cljs.test/report {:type :error
                                   :message (str e#)
                                   :error e#})
                (done#))
              (promesa.core/let [r# (prpr.test.reduce/reduce-pr-fns
                                     [~@fs])]
                (done#))))

         (let [body# (fn []
                       (with-test-binding-frame
                         (prpr.test.reduce/reduce-pr-fns
                          [~@fs])))]
           (record-test-binding-frame
            @(body#)))))))

#?(:clj
   (defmacro deftest*
     [& body]
     `(prpr.util.macro/if-cljs
       (cljs.test/deftest ~@body)
       (clojure.test/deftest ~@body))))

#?(:clj
   (defmacro deftest
     "define a test whose body yields a list of promises, which will
      be resolved before proceeding

      use the same name as clojure.test/deftest because CIDER
      recognizes it and uses it to find tests"
     [name & body]
     `(prpr.test/deftest* ~name
        (prpr.test/test-async
         ~@body))))

#?(:clj
   (defmacro is
     [& body]
     `(prpr.util.macro/if-cljs
       (cljs.test/is ~@body)
       (with-test-binding-frame
         (clojure.test/is ~@body)))))

#?(:clj
   (defmacro testing
     [& body]
     `(prpr.util.macro/if-cljs
       (cljs.test/testing ~@body)
       (with-test-binding-frame
         (clojure.test/testing ~@body)))))

#?(:clj
   (defmacro with-log-level
     "set the log-level while executing the body"
     [log-level & body]
     `(let [cl# (or (:level timbre/*config*)
                    :info)]
        (try
          (timbre/set-level! ~log-level)
          ~@body
          (finally
            (timbre/set-level! cl#))))))


(defn compose-fixtures
  "deals properly with cljs async map fixtures"
  [f1 f2]
  #?(:clj (clojure.test/compose-fixtures f1 f2)

     :cljs (let [{f1-before :before
                  f1-after :after} (if (map? f1)
                                     f1
                                     {:before f1})
                 {f2-before :before
                  f2-after :after} (if (map? f2)
                                     f2
                                     {:before f2})]
             {:before (cljs.test/compose-fixtures f1-before f2-before)
              :after (cljs.test/compose-fixtures f1-after f2-after)})))

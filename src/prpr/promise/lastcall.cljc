(ns prpr.promise.lastcall
  #?(:cljs (:require-macros [prpr.promise.lastcall]))
  (:require
   #?(:clj [clojure.tools.macro :refer [name-with-attributes]])
   [promesa.core :as pr]
   [prpr.error :as err]))

(defn lastcall-fn-impl
  [in-flight-atom fn-name params p]
  (reset! in-flight-atom p)
  (pr/create
   (fn [resolve reject]
     (pr/handle
      p
      (fn [v e]

        (if (some? e)

          (if (= @in-flight-atom p)
            (reject e)
            (reject
             (err/ex-info
              ::cancelled
              {:fn fn-name
               :params params})))

          (if (= @in-flight-atom p)

            (resolve v)

            (reject
             (err/ex-info
              ::cancelled
              {:fn fn-name
               :params params})))))))))

#?(:clj
   (defn lastcall-fn*
     "a lastcall-fn is a fn which returns a promise of its result, and tracks
      invocations such that if the results of multiple invocations are
      unresolved at any point in time, only the last invocation will return
      a result - all the others will error with
      [:cancelled {:fn <fn-name> :params <params>}]

     provides a no-args version of the fn which will cancel any outstanding
     unresolved promises (where cancel means they will return [:cancelled ...]
     errors, nothing more - they will not be interrupted)"
     [def-sym fn-name params-body]
     (let [fn-sym (-> fn-name name symbol)
           [fn-sym [params & body]] (name-with-attributes fn-sym params-body)]

       `(let [in-flight-atom# (atom nil)]
          (~def-sym ~fn-sym
            ([]
             (prpr.promise.lastcall/lastcall-fn-impl
              in-flight-atom#
              (quote ~fn-name)
              nil
              (pr/rejected
               (prpr.error/ex-info
                ::cancelled
                {:fn (quote ~fn-name)
                 :params nil})))
             (pr/resolved true))
            (~params
             (let [val-p# ~@body]
               (prpr.promise.lastcall/lastcall-fn-impl
                in-flight-atom#
                (quote ~fn-name)
                ~params
                val-p#))))))))

#?(:clj
   (defmacro def-lastcall-fn
     [name & params-body]
     (lastcall-fn* 'defn name params-body)))

#?(:clj
   (defmacro lastcall-fn
     [name & params-body]
     (lastcall-fn* 'fn name params-body)))

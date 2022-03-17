(ns prpr.util.test
  #?(:require-macros
     [prpr.util.test])
  (:require
   [taoensso.timbre :as timbre
    :refer [debug info warn error]]))

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

(defn with-log-level-fixture
  "call with a log-level to return a fixture
   to set that log-level"
  [log-level]
  #?(:clj
       (fn [f]
         (with-log-level log-level (f)))

       :cljs
       (let [{restore-level :level} timbre/*config*]
         {:before (fn [] (timbre/set-level! log-level))
          :after (fn [] (timbre/set-level! restore-level))})))

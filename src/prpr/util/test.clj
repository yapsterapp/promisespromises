(ns prpr.util.test
  (:require
   [taoensso.timbre :as timbre
    :refer [trace debug info warn error]]
   [clojure.test :as t]))

(defmacro with-log-level
  "set the log-level while executing the body"
  [log-level & body]
  `(let [cl# (or (:level timbre/*config*)
                 :info)]
     (try
       (timbre/set-level! ~log-level)
       ~@body
       (finally
         (timbre/set-level! cl#)))))

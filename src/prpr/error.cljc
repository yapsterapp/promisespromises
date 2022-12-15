(ns prpr.error
  (:refer-clojure :exclude [ex-info])
  (:require
   [clojure.core :as clj]))

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

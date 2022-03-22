(ns prpr.stream.impl
  (:require
   [clojure.core :as clj]
   [prpr.stream.protocols :as pt]
   [promesa.core :as promise]
   #?(:clj [prpr.stream.manifold :as stream.manifold]
      :cljs [prpr.stream.core-async :as stream.async]))
  (:refer-clojure
    :exclude [map filter mapcat reductions reduce concat]))


(def stream-factory
  #?(:clj stream.manifold/stream-factory
     :cljs stream.async/stream-factory))

(defn stream
  ([]
   (pt/-stream stream-factory))
  ([buffer]
   (pt/-stream stream-factory buffer))
  ([buffer xform]
   (pt/-stream stream-factory buffer xform))
  #?(:clj
     ([buffer xform executor]
       (pt/-stream stream-factory buffer xform executor))))

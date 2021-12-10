(ns prpr.a-frame.cofx.resolve-arg
  #?(:cljs (:require-macros [prpr.a-frame.cofx.resolve-arg.tag-readers]))
  (:require
   [prpr.a-frame.cofx.resolve-arg.protocols :refer [ICofxPath]]
   [prpr.a-frame.cofx.resolve-arg.tag-readers]))

(defn cofx-path?
  [v]
  (satisfies? ICofxPath v))

(defmulti resolve-cofx-arg
  (fn [spec _coeffects]
    (cond
      (cofx-path? spec) ::resolve-cofx-path
      (vector? spec) ::resolve-vector
      (map? spec) ::resolve-map
      :else :default)))

(defmethod resolve-cofx-arg ::resolve-cofx-path
  [spec coeffects]
  (get-in coeffects (:path spec)))

(defmethod resolve-cofx-arg :default
  [spec _coeffects]
  spec)

(defmethod resolve-cofx-arg ::resolve-vector
  [spec coeffects]
  (mapv
   #(resolve-cofx-arg % coeffects)
   spec))

(defmethod resolve-cofx-arg ::resolve-map
  [spec coeffects]
  (into
   {}
   (for [[k v] spec]
     [k (resolve-cofx-arg v coeffects)])))

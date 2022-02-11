(ns prpr.schema
  (:require
   [schema.core :as s]))

(defn key-if-optional
  "if k is optional-key? return the contained key,
   otherwise return k"
  [k]
  (if (s/optional-key? k)
    (:k k)
    k))

(defn merge-map-schemas*
  "optional-key sensitive map-schema merging for two maps"
  [a b]
  (reduce
   (fn [a* [bkorg bv]]
     (let [bk (key-if-optional bkorg)
           bkopt (s/optional-key bk)]
       (-> a*
           (dissoc bk bkopt)
           (assoc bkorg bv))))
   a
   b))

(defn merge-map-schemas
  "optional-key sensitive map-schema merging for any number of maps"
  [a & bs]
  (reduce merge-map-schemas* a bs))

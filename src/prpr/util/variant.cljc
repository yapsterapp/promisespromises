(ns prpr.util.variant
  (:require
   [clojure.string :as str]))

(defn kw-or-str->str
  [kw-or-str]
  (cond
    (string? kw-or-str) kw-or-str
    (keyword? kw-or-str) (str/join
                          "/"
                          (->> [(namespace kw-or-str) (name kw-or-str)]
                               (filter identity)))
    ;; conditionals shouldn't throw - just return falsey
    :else nil))

(defn check-tag-value
  "checks if tag value v is equivalent to the specified tag. supports
   tags as keywords or strings"
  [tag v]
  (let [tag-str (kw-or-str->str tag)
        v-str (kw-or-str->str v)]
    (= tag-str v-str)))

(defn is-variant?
  [v]
  (and (sequential? v)
       (keyword? (first v))))

(defn is-variant-with-tag?
  [tag v]
  (and (is-variant? v)
       (= tag (first v))))

(defn check-variant
  "returns a predicate which will match a variant with
   tag name n (where a variant type is [tag value])"
  [tag]
  (when-not (or (keyword? tag) (string? tag))
    (throw (ex-info "only keyword or string tags are supported" {:tag tag})))
  (fn [r]
    (let [t (when (sequential? r) (first r))]
      (check-tag-value tag t))))

(defn is-map-variant?
  [m]
  (and (map? m)
       (or (some? (:tag m))
           (some? (get m "tag")))
       (or (contains? m :value)
           (contains? m "value"))))

(defn check-map-variant
  "returns a predicate which will match a variant with
   tag name n, where a variant type is {:tag <tag> :value <value>}.
   map variants are required because onyx only processes maps"
  [tag]
  (when-not (or (keyword? tag) (string? tag))
    (throw (ex-info "only keyword or string tags are supported" {:tag tag})))
  (fn [r]
    (when-let [t (when (map? r) (or (:tag r) (get r "tag")))]
      (check-tag-value tag t))))

(defn convert-map-variant-to-vector
  "onyx messages are always {:tag <tag> :value <value>} versions
   of variants... convert to [tag value] for client"
  [m]
  (when-not (is-map-variant? m)
    (throw (ex-info "m must be a map with :tag and :value keys" {:m m})))
  (let [t (or (:tag m) (get m "tag"))
        v (or (:value m) (get m "value"))]
    (when t [t v])))

(defn convert-vector-variant-to-map
  [v]
  (when-not (and (vector? v)
                 (>= (count v) 1))
    (throw (ex-info "v must be a vector or size>=1" {:v v})))
  (let [[tag value] v]
    (when tag {:tag tag :value value})))

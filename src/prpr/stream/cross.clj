(ns prpr.stream.cross
  (:require
   [cats.core :as m :refer [return]]
   [clojure.set :as set]
   [taoensso.timbre :refer [log trace debug info warn error]]
   [manifold
    [deferred :as d]
    [stream :as s]]
   [prpr.promise :as pr :refer [ddo]])
  (:import
   [manifold.stream.core IEventSource]))

(defprotocol ISortedStream
  "defines how to handle values on a sorted stream"
  (-stream [this] "returns the manifold stream")
  (-take! [_] [_ default-val]
    "take a value from the stream")
  (-close! [s] "close the stream")
  (-key [_ v]
    "extracts the sort-key from the value -
     the head-values with the lowest sort-key will
     be given to the selector-fn to select which
     values will compose the next output-value")
  (-merge [_ output-value skey-val]
    "merge a selected value for the stream with stream-key into the output value"))

;; default ISortedStream uses values as keys directly
;; and merges by replacement.

(extend-type IEventSource
  ISortedStream
  (-stream [s] s)
  (-take!
    ([s] (s/take! s ::drained))
    ([s default-val] (s/take! s default-val)))
  (-close! [s] (s/close! s))
  (-key [_ v] v)
  (-merge [_ output-value skey-val] (conj output-value skey-val)))

(defrecord SortedStream [stream key-fn merge-fn]
  ISortedStream
  (-stream [_] stream)
  (-take! [_] (s/take! stream ::drained))
  (-take! [_ default-val] (s/take! stream default-val))
  (-close! [_] (s/close! stream))
  (-key [_ v] (key-fn v))
  (-merge [_ output-value skey-val] (merge-fn output-value skey-val)))

(defn sorted-stream
  "create an ISortedStream from a manifold stream with a
   supplied key-fn and merge-fn"
  [key-fn merge-fn stream]
  (assert (or
           (instance? SortedStream stream)
           (instance? IEventSource stream)))
  (if (instance? SortedStream stream)
    stream
    (map->SortedStream {:stream stream
                        :key-fn key-fn
                        :merge-fn merge-fn})))

(defn- intermediate-stream
  "given a sorted-stream, create an intermediate-stream,
   which preserves the key and merge fns and arranges
   the manifold connections"
  [dst stream-or-sorted-stream]
  (let [i (s/stream)]

    (@#'manifold.stream/connect-via-proxy
     (-stream stream-or-sorted-stream)
     i
     dst
     {:description {:op "cross-streams"}})

    (if (instance? SortedStream stream-or-sorted-stream)
      (assoc stream-or-sorted-stream :stream i)
      i)))

(defn select-first
  "selector-fn which takes the first element from the offered set of elements"
  [skey-head-values]
  (let [[sk v] (first skey-head-values)]
    (if sk
      [sk]
      [])))

(defn select-all
  "selector-fn which takes all offered elements"
  [skey-head-values]
  (keys skey-head-values))

(defn min-key-val
  "uses the comparator to find the minimum key value from ks"
  [key-comparator-fn ks]
  ;; (info "keys" (vec ks))
  (reduce (fn [mk k]
            (cond
              (nil? mk) k

              (<= (key-comparator-fn mk k) 0) mk

              :else k))
          nil
          ks))

(defn head-values
  "given a map {skey stream}
   async fetch two initial values from each stream
   returning a map {skey [head-value next-value]}. this sets up the
   skey-head-values structure for next-output-value"
  [skey-streams]
  ;; (info "streams" streams)
  (ddo [ivs (->> (for [[sk s] skey-streams]
                   (ddo [hv (-take! s)
                         nv (-take! s)]
                     (return [sk [hv nv]])))
                 (apply d/zip))]
    (return
     (into {} ivs))))

(def vconj (fnil conj []))

(defn next-values
  "given a map {skey stream}, a map {skey [hv nv?]}, and a set of skeys, async fetch
   next values for the streams with the given keys, returning
   a map {skey next-value}"
  [skey-streams skey-head-values next-skey-set]
  (ddo [nvs (->> (for [sk (set next-skey-set)]
                   (let [[ohv onv] (get skey-head-values sk)]
                     (if (or (= ::drained ohv)
                             (= ::drained onv))
                       (d/success-deferred [sk [::drained]])
                       (ddo [nv (-take! (get skey-streams sk))]
                         (return [sk [onv nv]])))))
                 (apply d/zip))]
    (return
     (into {} nvs))))

(defn next-output-value
  "given skey-streams and the current head-values from those streams
   in skey-head-values, offers the {skey head-value}s with the lowest
   (according to key-comparator-fn) key values to the selector-fn, which
   returns #{skey}, the skeys selected for output

   the head-values for the skeys selected for output are then reduced on
   to the init-output-value

  - gets head-value keys with -key,a
   - gives the head-values with the lowest (according to comparator-fn)
     key value to (selector-fn [[idx head-value]+]) to return [idx*]
   - uses -merge to join selected head-values together into an output value
   - returns [output-value next-values] or ::drained"
  [key-comparator-fn selector-fn init-output-value skey-streams skey-head-values]
  (ddo [:let [;; _ (warn "skey-streams" skey-streams)
              ;; _ (warn "skey-head-values" skey-head-values)

              active-skey-head-values (->> skey-head-values
                                           (filter
                                            (fn [[sk [hv nv]]]
                                              (not= ::drained hv)))
                                           (into {}))

              min-k (->> active-skey-head-values
                         (map (fn [[sk [hv nv]]] (-key (get skey-streams sk) hv)))
                         (min-key-val key-comparator-fn))

              ;; _ (warn "min-k" min-k)

              min-k-skey-head-values (->> active-skey-head-values
                                          (filter
                                           (fn [[sk [hv nv]]]
                                             (= min-k
                                                (-key (get skey-streams sk) hv))))
                                          (into {}))

              ;; _ (warn "min-k-skey-head-values" min-k-skey-head-values)

              min-k-skeys (set (keys min-k-skey-head-values))
              selected-skeys (set (selector-fn min-k-skey-head-values))

              _ (when (and (not-empty min-k-skeys)
                           (or (empty? selected-skeys)
                               (not= selected-skeys
                                     (set/intersection
                                      selected-skeys
                                      min-k-skeys))))
                  (throw
                   (ex-info "selector-fn failed to choose"
                            {:selected-skeys selected-skeys
                             :min-k-skey-head-values min-k-skey-head-values})))

              ;; _ (warn "selected-skeys" selected-skeys)

              output-value (->> (for [sk selected-skeys]
                                  [sk (get active-skey-head-values sk)])
                                (reduce (fn [o [sk [hv nv]]]
                                          ;; (info "o sk hv" [o sk hv])
                                          (-merge (get skey-streams sk) o [sk hv]))
                                        init-output-value))

              ;; _ (warn "output-value" output-value)

              ;; now - are there any nvs with the same key value as the hv
              ;; if so, advance just those streams because
              ;;    it's a 1-many or many-1 join with more outputs
              ;; if not, advance all the streams contributing to the output value

              active-skey-next-values (->> skey-head-values
                                           (filter
                                            (fn [[sk [_ nv]]]
                                              (and nv (not= ::drained nv))))
                                           (into {}))

              ;; _ (warn "active-skey-next-values" active-skey-next-values)

              min-k-skey-next-values (->> active-skey-next-values
                                          (filter
                                           (fn [[sk [_ nv]]]
                                             (= min-k
                                                (-key (get skey-streams sk) nv))))
                                          (into {}))

              ;; _ (warn "min-k-skey-next-values" min-k-skey-next-values)

              min-k-nv-skeys (set (keys min-k-skey-next-values))

              ;; _ (warn "min-k-nv-skeys" min-k-nv-skeys)

              _ (when (> (count min-k-nv-skeys) 1)
                  (throw (pr/error-ex
                          ::many-many-join
                          {:message "many-many join not supported"
                           :skey-head-values skey-head-values})))

              advance-skeys (or (not-empty min-k-nv-skeys)
                                selected-skeys)]

        next-selected-vals (next-values skey-streams skey-head-values advance-skeys)
        :let [next-skey-head-values (merge
                                     skey-head-values
                                     next-selected-vals)]]

    ;; (warn "next-output-value" [output-value next-skey-head-values])

    (if (not-empty selected-skeys)
      (return [output-value next-skey-head-values])
      (return [::drained]))))

(defn cross-streams
  "join values from some sorted streams onto a new sorted stream

   given keyed streams in {skey stream} values will be take from the head of
   each stream, and the values with the lowest key value (keys being extracted with
   -key and compared with key-comparator-fn) will be passed to selector-fn as:

   (selector-fn {skey head-value})

   selector-fn returns a list of skeys to identify the values to be combined
   into the output value by reducing with

   (reduce #(-merge source-stream output-value [skey head-value])
           init-output-value
           selected-skey-head-values)

   repeat until all streams are drained

   different selector-fns and ISortedStream impls can give various
   behaviours, such as sort-merge or join"
  ([selector-fn init-output-value skey-streams]
   (cross-streams compare selector-fn init-output-value skey-streams))
  ([key-comparator-fn selector-fn init-output-value skey-streams]
   ;; (info "cross-streams" {:key-comparator-fn key-comparator-fn
   ;;                        :selector-fn selector-fn
   ;;                        :init-output-value init-output-value
   ;;                        :skey-streams skey-streams})
   (let [dst (s/stream)
         skey-intermediates (->>
                             (for [[sk s] skey-streams]
                               [sk (intermediate-stream dst s)])
                             (into {}))]

     (pr/catch
         (fn [e]
           (warn e "error crossing the streams. what did you expect?" )
           (doseq [s (concat [dst]
                             (vals skey-streams)
                             (vals skey-intermediates))]
             (try (-close! s)
                  (catch Exception x
                    (warn x "error closing errored streams"))))
           (throw e))

         (d/loop [skey-next-vals (head-values skey-intermediates)]

           (d/chain'
            skey-next-vals
            (fn [sk-nvs]
              ;; (info "sk-nvs" sk-nvs)
              (next-output-value
               key-comparator-fn
               selector-fn
               init-output-value
               skey-intermediates
               sk-nvs))
            (fn [[ov sk-nvs]]
              ;; (info "ov sk-nvs" [ov sk-nvs])
              (if (= ::drained ov)
                (do
                  ;; (info "closing!")
                  (s/close! dst)
                  false)
                (do
                  (s/put! dst ov)
                  (d/recur sk-nvs)))))))

     (d/success-deferred
      (s/source-only
       dst)))))

(defn sort-merge-streams
  "the merge phase of a merge-sort, with streams.

   given some streams each with content sorted by a key (extracted by key-fn),
   produces a new stream with all elements from the input streams, also
   sorted by the same key"
  ([skey-streams] (sort-merge-streams compare identity skey-streams))
  ([key-fn skey-streams] (sort-merge-streams compare key-fn skey-streams))
  ([key-comparator-fn key-fn skey-streams]
   (cross-streams
    key-comparator-fn
    select-first ;; choose only the first of the lowest-key head-items
    nil ;; init-value gets ignored by merge-fn
    ;; since there will only be a single chosen item, merge ignores the
    ;; merged-value arg
    (->> (for [[sk s] skey-streams]
           [sk (sorted-stream key-fn (fn [_ [_ v]] v) s)])
         (into {})))))

(defn full-outer-join-streams
  "full-outer-joins records from multiple streams, which must already
   be sorted by key"
  ([skey-streams] (full-outer-join-streams compare identity skey-streams))
  ([key-fn skey-streams] (full-outer-join-streams compare key-fn skey-streams))
  ([key-comparator-fn key-fn skey-streams]
   (cross-streams
    key-comparator-fn
    select-all ;; choose all offered lowest-key head-items
    {} ;; merge the chosen items into a map with conj
    (->> (for [[sk s] skey-streams]
           [sk (sorted-stream key-fn conj s)])
         (into {})))))

;; TODO
;; n-left-join - only output elements with matching keys on n leftmost streams
;; left-join - n-left-join where n=1
;; set/intersection - n-left-join where n=no of streams
;; set/union - outer join
;; set/difference - two-way join where output a record only if not present on right stream

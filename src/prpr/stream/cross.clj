(ns prpr.stream.cross
  (:require
   [cats.core :as m :refer [return]]
   [cats.labs.manifold :refer [deferred-context]]
   [clojure.set :as set]
   [clojure.math.combinatorics :as combo]
   [linked.core :as linked]
   [taoensso.timbre :refer [log trace debug info warn error]]
   [manifold
    [deferred :as d]
    [stream :as s]]
   [prpr.promise :as pr :refer [ddo]])
  (:import
   [linked.map LinkedMap]
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

(defn coerce-linked-map
  "coerce a map to a linked/map with reliable insertion-based ordering"
  [m]
  (cond
    (instance? LinkedMap m)
    m

    (sequential? m)
    (into (linked/map) m)

    (map? m)
    (into (linked/map) (sort m))

    :else
    (throw
     (pr/error-ex ::coerce-to-linked-map-failed
                  {:m m}))))

(defn select-first
  "selector-fn which takes the first element from the offered set of elements"
  [skey-head-values]
  ;; (info "select-first" skey-head-values)
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

(defn buffer-values
  "given a stream and a [k buf] structure, retrieve values from
   the stream matching key k into buf, until a value not matching k
   is encountered. returns Deferred<[[k updated-buf] [nk nk-buf]]>"
  [key-comparator-fn s [sort-key buf-v]]

  (d/loop [[k buf] [sort-key buf-v]]

    (d/chain'
     (-take! s)
     (fn [v]
       (cond
         ;; initialising with an empty stream
         (and (nil? k)
              (= ::drained v))
         [::drained]

         ;; initialising
         (nil? k)
         (d/recur [(-key s v) [v]])

         ;; end-of-stream
         (= ::drained v)
         [[k buf] ::drained]

         ;; another value with the key
         (= k (-key s v))
         (d/recur [k (conj buf v)])

         ;; no more values with the key
         :else
         (do
           (let [nk (-key s v)]
;;              (warn "compare" k nk)
             (when (> (key-comparator-fn k nk) 0)
               (throw
                (pr/error-ex ::stream-not-sorted {:this [k buf]
                                                  :next [nk [v]]})))
             [[k buf] [nk [v]]])))))))

(defn init-stream-buffers
  "given a map {skey stream}
   async fetch values from each stream
   returning a map {skey [[head-key head-values]
                          [next-key first-next-value]]}.
   this sets up the skey-streambufs structure for next-output-values"
  [key-comparator-fn skey-streams]
  ;; (info "streams" streams)
  (ddo [:let [skey-streams (coerce-linked-map skey-streams)]
        ivs (->> (for [[sk s] skey-streams]
                   (ddo [bvs (buffer-values
                              key-comparator-fn
                              s
                              nil)]
                     (return [sk bvs])))
                 (apply d/zip))]
    (return
     (into (linked/map) ivs))))

(defn advance-stream-buffers
  "given a map {skey stream}, a map {skey [[hk hvs] [nk nk-buf]]},
   and a set next-skey-set of skeys to update the stream-buffers for,
   async fetch next values for the streams with the given keys,
   returning an updated map {skey [[nk nvs] [nnk nnk-buf]]}"
  [key-comparator-fn skey-streams skey-streambufs next-skey-set]
  ;; (warn "skey-streambufs" skey-streambufs)
  ;; (warn "next-skey-sets" (vec next-skey-set))
  (ddo [:let [skey-streams (coerce-linked-map skey-streams)
              skey-streambufs (coerce-linked-map skey-streambufs)]
        next-skey-set (set next-skey-set)
        nvs (->> (for [sk (keys skey-streambufs)]
                   (if (next-skey-set sk)
                     (ddo [:let [[h n] (get skey-streambufs sk)]

                           bvs (if (and (not= ::drained h)
                                        (not= ::drained n))
                                 (buffer-values
                                  key-comparator-fn
                                  (get skey-streams sk)
                                  n)
                                 (return [::drained]))]
                       (return [sk bvs]))
                     (return
                      [sk (get skey-streambufs sk)])))
                 (apply d/zip))]
    ;; (warn "nvs" (into {} nvs))
    (return
     (into (linked/map) nvs))))

(defn min-key-skey-values
  "returns a map {skey [val+]} with entries for
   each stream which has a head buffer with vals matching the minimum
   value -key across all streams. uses key-comparator-fn to compare
   value -keys"
  [key-comparator-fn
   skey-streams
   skey-streambufs]
  ;; (warn "skey-streams" skey-streams)
  ;; (warn "skey-streambufs" skey-streambufs)
  (let [skey-streams (coerce-linked-map skey-streams)
        skey-streambufs (coerce-linked-map skey-streambufs)
        active-skey-streambufs (->> skey-streambufs
                                    (filter
                                     (fn [[sk [hkvs nkv]]]
                                       (not= ::drained hkvs)))
                                    (into (linked/map)))

        min-k (->> active-skey-streambufs
                   (map (fn [[sk [[hk hvs] nkv]]] hk))
                   (min-key-val key-comparator-fn))

        ;; _ (warn "min-k" min-k)

        min-k-skey-vals (->> active-skey-streambufs
                             (filter
                              (fn [[sk [[hk hvs] nkv]]]
                                (= min-k hk)))
                             (map (fn [[sk [[hk hvs] nkv]]]
                                    [sk hvs]))
                             (into (linked/map)))]

    ;; (warn "min-k-skey-vals" min-k-skey-vals)
    min-k-skey-vals))

(defn select-skey-values
  "uses the selector-fn to select 1 or more values from an {skey [values...]}
   map. errors if there are values to select and the selector-fn doesn't do
   something sensible"
  [selector-fn
   skey-values]
  (let [skey-values (coerce-linked-map skey-values)
        skeys (set (keys skey-values))
        selected-skeys (set (selector-fn skey-values))
        ;; _ (warn "selected-skeys" selected-skeys)

        _ (when (and (not-empty skeys)
                     (or (empty? selected-skeys)
                         (not= selected-skeys
                               (set/intersection
                                selected-skeys
                                skeys))))
            (throw
             (pr/error-ex ::selector-fn-failed
                          {:message "selector-fn failed to choose well"
                           :skey-values skey-values
                           :selected-skeys selected-skeys})))]
    (select-keys
     skey-values
     selected-skeys)))

(defn merge-stream-objects
  "given a seq of [skey val] objects, merge the vals
   using the -merge operation defined on the stream from
   skey-streams {skey stream}"
  [init-output-value skey-streams skey-vals]
  (let [skey-streams (coerce-linked-map skey-streams)
        skey-vals (coerce-linked-map skey-vals)]
    (->> skey-vals
         (reduce (fn [o [sk v]]
                   (-merge (get skey-streams sk) o [sk v]))
                 init-output-value))))

(defn head-values-cartesian-product-merge
  "given a map skey-values {skey [val+]} of vectors of records
   for the same key from different streams,
   first produce a cartesian product and then merge each product value
   to a single value"
  [init-output-value skey-streams skey-values]
  ;; do the reduce in the order of skey-streams (if it was passed
  ;; as a seq rather than a map)
  ;; (warn "key-values-cartesian-product" init-output-value skey-streams skey-values)
  (let [skey-streams (coerce-linked-map skey-streams)

        ordered-skeys (->> skey-streams
                           (map (fn [[sk _]] sk))
                           (filter (set (keys skey-values))))
        ordered-skey-hvals (for [sk ordered-skeys]
                             (for [hv (get skey-values sk)]
                               [sk hv]))

        ;; cartesian-product produces '(()) if empty - bug?
        cp (if (not-empty ordered-skey-hvals)
             (apply combo/cartesian-product ordered-skey-hvals)
             '())]

    ;; (warn "ordered-skey-hvals" (vec ordered-skey-hvals))
    ;; (warn "cartesian-product" (vec cp))

    (for [skey-vals cp]
      (merge-stream-objects init-output-value skey-streams skey-vals))))

(defn next-output-values
  "given skey-streams and the current head values from those streams
   in skey-streambufs, offers the {skey head-value}s with the lowest
   (according to key-comparator-fn) key values to the select-skey-values
   which uses the selector-fn to choose the same-key head-values from
   one or more stream

   then a cartesian product of selected values for each stream are
   merged to give the output-values and the streambufs are updated
   accordingly

   - returns [output-value next-skey-streambufs] or [::drained]"
  [key-comparator-fn selector-fn init-output-value skey-streams skey-streambufs]
  (ddo [:let [skey-streams (coerce-linked-map skey-streams)
              skey-streambufs (coerce-linked-map skey-streambufs)

              mkskvs (min-key-skey-values
                      key-comparator-fn
                      skey-streams
                      skey-streambufs)

              ;; _ (warn "mkskvs" mkskvs)

              selected-skey-values (select-skey-values
                                    selector-fn
                                    mkskvs)

              ;; _ (warn "selected-skey-values" selected-skey-values)

              output-values (head-values-cartesian-product-merge
                             init-output-value
                             skey-streams
                             selected-skey-values)

              ;; _ (warn "output-values" (vec output-values))
              ]

        next-skey-streambufs (advance-stream-buffers
                              key-comparator-fn
                              skey-streams
                              skey-streambufs
                              (keys selected-skey-values))]
    ;; (warn "next-skey-streambufs" next-skey-streambufs)
    ;; (warn "next-output-values" [output-values next-skey-streambufs])

    (if (not-empty (keys selected-skey-values))
      (return [output-values next-skey-streambufs])
      (return [::drained]))))

(defn cross-streams
  "join values from some sorted streams onto a new sorted stream

   given keyed streams in {skey stream} values with the same key
   (as determined by -key) will be take from the head of
   each stream, and the values from each stream with the lowest key value
   (keys being compared with key-comparator-fn)
   will be passed to the selector-fn which selects one or more of them,
   as input to a cartesian product of the selected values from each stream.
   the output tuples of the cartesian product are then merged into a
   stream of output values

   different selector-fns and ISortedStream impls can give different
   behaviours, such as sort-merge or join"
  ([selector-fn init-output-value skey-streams]
   (cross-streams compare selector-fn init-output-value skey-streams))
  ([key-comparator-fn selector-fn init-output-value skey-streams]
   ;; (info "cross-streams" {:key-comparator-fn key-comparator-fn
   ;;                        :selector-fn selector-fn
   ;;                        :init-output-value init-output-value
   ;;                        :skey-streams skey-streams})
   (let [skey-streams (coerce-linked-map skey-streams)

         dst (s/stream)
         skey-intermediates (->>
                             (for [[sk s] skey-streams]
                               [sk (intermediate-stream dst s)])
                             (into (linked/map)))]

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

         (d/loop [skey-streambufs (init-stream-buffers
                                   key-comparator-fn
                                   skey-intermediates)]

           (d/chain'
            skey-streambufs
            (fn [sk-nvs]
              ;; (info "sk-nvs" sk-nvs)
              (next-output-values
               key-comparator-fn
               selector-fn
               init-output-value
               skey-intermediates
               sk-nvs))
            (fn [[ovs sk-nvs]]
              ;; (info "ovs sk-nvs" [ovs sk-nvs])
              (if (= ::drained ovs)
                (do
                  ;; (info "closing!")
                  (s/close! dst)
                  false)
                (do
                  (doseq [ov ovs]
                    (s/put! dst ov))
                  (d/recur sk-nvs)))))))

     (d/success-deferred
      (s/source-only
       dst)))))

(defn sort-merge-streams
  "the merge phase of a merge-sort, with streams.

   given some streams each with content sorted by a key (extracted by key-fn),
   produces a new stream with all elements from the input streams, also
   sorted by the same key

   for elements with the same key, skey-streams order is used"
  ([skey-streams] (sort-merge-streams compare identity skey-streams))
  ([key-fn skey-streams] (sort-merge-streams compare key-fn skey-streams))
  ([key-comparator-fn key-fn skey-streams]
   (let [skey-streams (coerce-linked-map skey-streams)]
     (cross-streams
      key-comparator-fn
      select-first ;; choose only the first of the lowest-key head-items
      nil ;; init-value gets ignored by merge-fn
      ;; since there will only be a single chosen item, merge ignores the
      ;; merged-value arg
      (->> (for [[sk s] skey-streams]
             [sk (sorted-stream key-fn (fn [_ [_ v]] v) s)])
           (into (linked/map)))))))

(defn full-outer-join-streams
  "full-outer-joins records from multiple streams, which must already
   be sorted by key. 1-1, 1-many, many-1 and many-many joins are all supported.
   all elements from all streams for each join key will be buffered in memory"
  ([skey-streams] (full-outer-join-streams compare identity skey-streams))
  ([key-fn skey-streams] (full-outer-join-streams compare key-fn skey-streams))
  ([key-comparator-fn key-fn skey-streams]
   (let [skey-streams (coerce-linked-map skey-streams)]
     (cross-streams
      key-comparator-fn
      select-all ;; choose all offered lowest-key head-items
      {} ;; merge the chosen items into a map with conj
      (->> (for [[sk s] skey-streams]
             [sk (sorted-stream key-fn conj s)])
           (into (linked/map)))))))

;; TODO
;; n-left-join - only output elements with matching keys on n leftmost streams
;; left-join - n-left-join where n=1
;; set/intersection - n-left-join where n=no of streams
;; set/union - outer join
;; set/difference - two-way join where output a record only if not present on right stream

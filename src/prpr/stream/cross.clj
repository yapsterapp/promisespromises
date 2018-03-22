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
   [manifold.stream.core :as stream.core]
   [prpr.promise :as pr :refer [ddo]]
   [manifold.stream :as stream])
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
     values will compose the next output-value"))

;; default ISortedStream uses values as keys directly
;; and merges by replacement.

(extend-type IEventSource
  ISortedStream
  (-stream [s] s)
  (-take!
    ([s] (s/take! s ::drained))
    ([s default-val] (s/take! s default-val)))
  (-close! [s] (s/close! s))
  (-key [_ v] v))

(defrecord SortedStream [stream key-fn merge-fn]

  IEventSource
  (take [_ default-val blocking?]
    (.take stream default-val blocking?))
  (take [_ default-val blocking? timeout timeout-val]
    (.take stream default-val blocking? timeout timeout-val))
  (isDrained [_]
    (.isDrained stream))
  (onDrained [_ callback]
    (.onDrained stream callback))
  (connector [_ sink]
    (fn [_ sink options]
      (stream/connect stream sink options)))

  ISortedStream
  (-stream [_] stream)
  (-take! [_] (s/take! stream ::drained))
  (-take! [_ default-val] (s/take! stream default-val))
  (-close! [_] (s/close! stream))
  (-key [_ v] (key-fn v)))

(defn sorted-stream
  "create an ISortedStream from a manifold stream with a
   supplied key-fn and merge-fn"
  [key-fn stream]
  (assert (or
           (instance? SortedStream stream)
           (instance? IEventSource stream)))
  (if (instance? SortedStream stream)
    stream
    (map->SortedStream {:stream stream
                        :key-fn key-fn})))

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

(defn set-select-all
  [skey-head-values]
  (let [set? (->> (for [[sk vs] skey-head-values]
                    (count vs))
                  (every? #(= % 1)))]
    (when-not set?
      (throw
       (pr/error-ex ::not-a-set
                    {:skey-head-values skey-head-values})))
    (keys skey-head-values)))

(defn min-key-val
  "uses the comparator to find the minimum key value from ks"
  [key-compare-fn ks]
  ;; (info "keys" (vec ks))
  (reduce (fn [mk k]
            (cond
              (nil? mk) k

              (<= (key-compare-fn mk k) 0) mk

              :else k))
          nil
          ks))

(defn buffer-values
  "given a stream and a [k buf] structure, retrieve values from
   the stream matching key k into buf, until a value not matching k
   is encountered. returns Deferred<[[k updated-buf] [nk nk-buf]]>"
  [key-compare-fn stream-key stream [sort-key buf-v]]

  (d/loop [[k buf] [sort-key buf-v]]

    (d/chain'
     (-take! stream)
     (fn [v]
       (cond
         ;; initialising with an empty stream
         (and (nil? k)
              (= ::drained v))
         [::drained]

         ;; initialising
         (nil? k)
         (d/recur [(-key stream v) [v]])

         ;; end-of-stream
         (= ::drained v)
         [[k buf] ::drained]

         ;; another value with the key
         (= k (-key stream v))
         (d/recur [k (conj buf v)])

         ;; no more values with the key
         :else
         (do
           (let [nk (-key stream v)]
;;              (warn "compare" k nk)
             (when (> (key-compare-fn k nk) 0)
               (throw
                (pr/error-ex ::stream-not-sorted {:stream-key stream-key
                                                  :this [k buf]
                                                  :next [nk [v]]})))
             [[k buf] [nk [v]]])))))))

(defn init-stream-buffers
  "given a map {skey stream}
   async fetch values from each stream
   returning a map {skey [[head-key head-values]
                          [next-key first-next-value]]}.
   this sets up the skey-streambufs structure for next-output-values"
  [key-compare-fn skey-streams]
  ;; (info "streams" streams)
  (ddo [:let [skey-streams (coerce-linked-map skey-streams)]
        ivs (->> (for [[sk s] skey-streams]
                   (ddo [bvs (buffer-values
                              key-compare-fn
                              sk
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
  [key-compare-fn skey-streams skey-streambufs next-skey-set]
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
                                  key-compare-fn
                                  sk
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
   value -key across all streams. uses key-compare-fn to compare
   value -keys"
  [key-compare-fn
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
                   (min-key-val key-compare-fn))

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
  [init-output-value merge-fn skey-streams skey-vals]
  (let [skey-vals (coerce-linked-map skey-vals)]
    (->> skey-vals
         (reduce (fn [o [sk v]]
                   (merge-fn o sk v))
                 init-output-value))))

(defn head-values-cartesian-product-merge
  "given a map skey-values {skey [val+]} of vectors of records
   for the same key from different streams,
   first produce a cartesian product and then merge each product value
   to a single value, finishing off each merge with finish-merge-fn and
   then sorting the merged values with product-sort-fn"
  [init-output-value
   merge-fn
   finish-merge-fn
   product-sort-fn
   skey-streams
   skey-values]
  ;; do the reduce in the order of skey-streams (if it was passed
  ;; as a seq rather than a map)
  ;; (warn "key-values-cartesian-product" init-output-value skey-streams skey-values)
  (ddo [:let [skey-streams (coerce-linked-map skey-streams)

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

        ;; allow for async finish-merge-fn
        merged (->> (for [skey-vals cp]
                      ((or finish-merge-fn identity)
                       (merge-stream-objects
                        init-output-value
                        merge-fn
                        skey-streams
                        skey-vals)))
                    (apply d/zip))]
    (->> merged
         ;; finish-merge-fn can return nil to remove records
         (filter identity)
         ((or product-sort-fn identity))
         return)))

(defn next-output-values
  "given skey-streams and the current head values from those streams
   in skey-streambufs, offers the {skey head-value}s with the lowest
   (according to key-compare-fn) key values to the select-skey-values
   which uses the selector-fn to choose the same-key head-values from
   one or more stream

   then a cartesian product of selected values for each stream are
   merged to give the output-values and the streambufs are updated
   accordingly

   - returns [output-value next-skey-streambufs] or [::drained]"
  [{key-compare-fn :key-compare-fn
    selector-fn :selector-fn
    finish-merge-fn :finish-merge-fn
    product-sort-fn :product-sort-fn
    skey-streams :skey-streams
    skey-streambufs :skey-streambufs
    :as args}]
  (ddo [:let [skey-streams (coerce-linked-map skey-streams)
              skey-streambufs (coerce-linked-map skey-streambufs)

              mkskvs (min-key-skey-values
                      key-compare-fn
                      skey-streams
                      skey-streambufs)

              ;; _ (warn "mkskvs" mkskvs)

              selected-skey-values (select-skey-values
                                    selector-fn
                                    mkskvs)

              ;; _ (warn "selected-skey-values" selected-skey-values)
              ]

        output-values (head-values-cartesian-product-merge
                       {}
                       assoc
                       finish-merge-fn
                       product-sort-fn
                       skey-streams
                       selected-skey-values)
        :let [
              ;; _ (warn "output-values" (vec output-values))
              ]

        next-skey-streambufs (advance-stream-buffers
                              key-compare-fn
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
   (keys being compared with key-compare-fn)
   will be passed to the selector-fn which selects one or more of them,
   as input to a cartesian product of the selected values from each stream.
   the output tuples of the cartesian product are then merged into a
   stream of output values and the stream of output values is passed to
   product-sort-fn to be sorted into the correct result order

   different selector-fns and ISortedStream impls can give different
   behaviours, such as sort-merge or join"
  [{key-compare-fn :key-compare-fn
    selector-fn :selector-fn
    finish-merge-fn :finish-merge-fn
    product-sort-fn :product-sort-fn
    skey-streams :skey-streams
    :as args}]
  ;; (info "cross-streams" args)
  (let [skey-streams (coerce-linked-map skey-streams)

        dst (s/stream)
        skey-intermediates (->>
                            (for [[sk s] skey-streams]
                              [sk (intermediate-stream dst s)])
                            (into (linked/map)))]

    (pr/catch
        (fn [e]
          (warn e "error crossing the streams" )
          (doseq [s (concat [dst]
                            (vals skey-streams)
                            (vals skey-intermediates))]
            (try (-close! s)
                 (catch Exception x
                   (warn x "error closing errored streams"))))
          (throw e))

        (d/loop [skey-streambufs (init-stream-buffers
                                  key-compare-fn
                                  skey-intermediates)]

          (d/chain'
           skey-streambufs
           (fn [sk-nvs]
             ;; (info "sk-nvs" sk-nvs)
             (next-output-values
              (assoc args
                     :skey-streams skey-intermediates
                     :skey-streambufs sk-nvs)))
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
      dst))))

(defn sort-merge-streams
  "the merge phase of a merge-sort, with streams.

   given some streams each with content sorted by a key (extracted by key-fn),
   produces a new stream with all elements from the input streams, also
   sorted by the same key

   for elements with the same key, skey-streams order is used"
  [{key-compare-fn :key-compare-fn
    default-key-fn :default-key-fn
    skey-streams :skey-streams
    :or {key-compare-fn compare
         default-key-fn identity}
    :as args}]
  (let [skey-streams (coerce-linked-map skey-streams)]
    (cross-streams
     (-> args
         (dissoc :default-key-fn)
         (assoc
          :key-compare-fn key-compare-fn
          ;; choose only the first of the lowest-key head-items
          :selector-fn select-first
          ;; there should only be a single value in the merged output
          :finish-merge-fn (fn [m]
                             (assert (= 1 (count m)))
                             (let [[k v] (first m)]
                               v))
          :skey-streams (->> (for [[sk s] skey-streams]
                               [sk
                                (sorted-stream
                                 default-key-fn
                                 s)])
                             (into (linked/map))))))))

(defn full-outer-join-streams
  "full-outer-joins records from multiple streams, which must already
   be sorted by key. 1-1, 1-many, many-1 and many-many joins are all supported.
   all elements from all streams for each join key will be buffered in memory"
  [{key-compare-fn :key-compare-fn
    default-key-fn :default-key-fn
    finish-merge-fn :finish-merge-fn
    product-sort-fn :product-sort-fn
    skey-streams :skey-streams
    :or {key-compare-fn compare
         default-key-fn identity}
    :as args}]
  (let [skey-streams (coerce-linked-map skey-streams)]
    (cross-streams
     (-> args
         (dissoc :default-key-fn)
         (assoc
          :key-compare-fn key-compare-fn
          ;; choose all offered lowest-key head-items
          :selector-fn select-all
          :skey-streams (->> (for [[sk s] skey-streams]
                               [sk
                                (sorted-stream
                                 default-key-fn
                                 s)])
                             (into (linked/map))))))))

(defn n-left-join-streams
  [{key-compare-fn :key-compare-fn
    default-key-fn :default-key-fn
    finish-merge-fn :finish-merge-fn
    product-sort-fn :product-sort-fn
    skey-streams :skey-streams
    n :n
    :or {key-compare-fn compare
         default-key-fn identity
         n 1}
    :as args}]
  (let [skey-streams (coerce-linked-map skey-streams)

        ;; only return a record if there is a value
        ;; in the joined record from each of the
        ;; n leftmost streams
        n-left-join-finish-merge-fn
        (fn [skey-vals]
          (let [n-left-skeys (take n (keys skey-streams))
                satisfies-n-left? (->> (for [sk n-left-skeys]
                                         (get skey-vals sk))
                                       (every? some?))]
            (when satisfies-n-left?
              ((or finish-merge-fn identity)
               skey-vals))))]
    (cross-streams
     (-> args
         (dissoc :default-key-fn)
         (assoc
          :key-compare-fn key-compare-fn
          ;; choose all offered lowest-key head-items
          :selector-fn select-all
          :finish-merge-fn n-left-join-finish-merge-fn
          :skey-streams (->> (for [[sk s] skey-streams]
                               [sk
                                (sorted-stream
                                 default-key-fn
                                 s)])
                             (into (linked/map))))))))

(defn inner-join-streams
  [{key-compare-fn :key-compare-fn
    default-key-fn :default-key-fn
    finish-merge-fn :finish-merge-fn
    product-sort-fn :product-sort-fn
    skey-streams :skey-streams
    :or {key-compare-fn compare
         default-key-fn identity}
    :as args}]
  (n-left-join-streams
   (assoc args :n (count skey-streams))))

(defn set-streams-intersect
  [{key-compare-fn :key-compare-fn
    default-key-fn :default-key-fn
    finish-merge-fn :finish-merge-fn
    product-sort-fn :product-sort-fn
    skey-streams :skey-streams
    :or {key-compare-fn compare
         default-key-fn identity}
    :as args}]
  (let [skey-streams (coerce-linked-map skey-streams)

        ;; only return a record if there is a value
        ;; in the joined record from every stream
        intersect-finish-merge-fn
        (fn [skey-vals]
          (let [all-skeys (keys skey-streams)
                satisfies-intersect? (->> (for [sk all-skeys]
                                            (get skey-vals sk))
                                          (every? some?))]
            (when satisfies-intersect?
              ((or finish-merge-fn identity)
               skey-vals))))]
    (cross-streams
     (-> args
         (dissoc :default-key-fn)
         (assoc
          :key-compare-fn key-compare-fn
          ;; choose all offered lowest-key head-items
          ;; bork if there are multiple values for a key on any
          ;; one stream i.e. it's not a set
          :selector-fn set-select-all
          :finish-merge-fn intersect-finish-merge-fn
          :skey-streams (->> (for [[sk s] skey-streams]
                               [sk
                                (sorted-stream
                                 default-key-fn
                                 s)])
                             (into (linked/map))))))))

(defn set-streams-union
  [{key-compare-fn :key-compare-fn
    default-key-fn :default-key-fn
    finish-merge-fn :finish-merge-fn
    product-sort-fn :product-sort-fn
    skey-streams :skey-streams
    :or {key-compare-fn compare
         default-key-fn identity}
    :as args}]
  (let [skey-streams (coerce-linked-map skey-streams)]
    (cross-streams
     (-> args
         (dissoc :default-key-fn)
         (assoc
          :key-compare-fn key-compare-fn
          ;; choose all offered lowest-key head-items
          ;; bork if there are multiple values for a key on any
          ;; one stream i.e. it's not a set
          :selector-fn set-select-all
          :finish-merge-fn finish-merge-fn
          :skey-streams (->> (for [[sk s] skey-streams]
                               [sk
                                (sorted-stream
                                 default-key-fn
                                 s)])
                             (into (linked/map))))))))

(defn set-streams-difference
  [{key-compare-fn :key-compare-fn
    default-key-fn :default-key-fn
    finish-merge-fn :finish-merge-fn
    product-sort-fn :product-sort-fn
    skey-streams :skey-streams
    :or {key-compare-fn compare
         default-key-fn identity}
    :as args}]
  (let [skey-streams (coerce-linked-map skey-streams)

        ;; only return a record if there is a record in
        ;; the first stream and no other stream
        difference-finish-merge-fn
        (fn [skey-vals]
          (let [all-skeys (keys skey-streams)
                first-skey (first all-skeys)
                rest-skeys (rest all-skeys)

                first-val (get skey-vals first-skey)
                has-rest-val? (some #(get skey-vals %) rest-skeys)]
            (when (and first-val (not has-rest-val?))
              ((or finish-merge-fn identity)
               skey-vals))))]
    (cross-streams
     (-> args
         (dissoc :default-key-fn)
         (assoc
          :key-compare-fn key-compare-fn
          ;; choose all offered lowest-key head-items
          ;; bork if there are multiple values for a key on any
          ;; one stream i.e. it's not a set
          :selector-fn set-select-all
          :finish-merge-fn difference-finish-merge-fn
          :skey-streams (->> (for [[sk s] skey-streams]
                               [sk
                                (sorted-stream
                                 default-key-fn
                                 s)])
                             (into (linked/map))))))))

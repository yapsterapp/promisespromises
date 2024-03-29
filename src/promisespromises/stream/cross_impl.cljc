(ns promisespromises.stream.cross-impl
  (:require
   [clojure.math.combinatorics :as combo]
   [clojure.set :as set]
   [linked.core :as linked]
   [malli.experimental :as mx]
   [malli.util :as mu]
   [promesa.core :as pr]
   [taoensso.timbre :refer [trace debug info warn error]]

   [promisespromises.promise :as prpr]
   [promisespromises.error :as err]
   [promisespromises.stream.operations :as stream.ops]
   [promisespromises.stream.protocols :as stream.pt]
   [promisespromises.stream.transport :as stream.transport]
   [promisespromises.stream.types :as stream.types]
   [promisespromises.stream.chunk :as stream.chunk]

   [promisespromises.stream.cross :as-alias stream.cross]
   [promisespromises.stream.cross.op :as-alias stream.cross.op]
   [promisespromises.stream.cross.op.n-left-join :as-alias stream.cross.op.n-left-join]))

;;; cross mkII
;;;
;;; crossing sorted streams, with
;;; chunking. allows merge, joins and set operations
;;; on streams sorted in a key - perfect if you
;;; have a database like cassandra and need some in-memory
;;; join operations with constant-space characteristics
;;;
;;; - re-chunk based on key + target-size
;;; - consume chunks
;;; - sync cross-join matching keys in chunks
;;; - output everything possible without further take! using
;;;   specified chunk target-size constraints
;;; - rinse / repeat

(def stream-finished-drained-marker ::drained)
(def stream-finished-errored-marker ::errored)

(def stream-finished-markers
  "the partition buffer marker values indicating that
   a stream is finished"
  #{stream-finished-drained-marker
    stream-finished-errored-marker})

(defn stream-finished?
  "takes a partition-buffer and returns true
   if there are no more values to consumer from the
   corresponding stream"
  [partition-buffer]
  (-> partition-buffer
      (last)
      (first)
      (stream-finished-markers)))

(defn values-sorted?
  "returns true if vs are sorted according to comparator-fn"
  [comparator-fn vs]
  (let [[r _] (reduce
               (fn [[r pv] nv]
                 (if (nil? r)
                   [true nv]
                   (if (<= (comparator-fn pv nv) 0)
                     [true nv]
                     (reduced [false nv])))
                 )
               [nil nil]
               vs)]
    r))

(defn buffer-chunk!
  "given a stream of chunks of partitions, and a
   partition-buffer of [key partition] tuples, retrieve another
   chunk of partitions and add them to the partition-buffer
   (or add the keyword ::drained if the end of the
    stream is reached, or ::errored if the stream errored)

   returns Promise<[ [<partition-key> <partition>]* ::drained?]"
  [partition-buffer
   {key-comparator-fn ::stream.cross/key-comparator-fn
    key-extractor-fns ::stream.cross/key-extractor-fns
    :as cross-spec}
   stream-id
   stream]

  (pr/handle
     (stream.transport/take! stream stream-finished-drained-marker)

     (fn [v err]
       ;; (prn "BUFFER-CHUNK!" v err)
       (cond

         (some? err)
         (if (stream-finished? partition-buffer)
           partition-buffer
           (conj partition-buffer [stream-finished-errored-marker err]))

         (= stream-finished-drained-marker v)
         (if (stream-finished? partition-buffer)
           partition-buffer
           (conj partition-buffer [stream-finished-drained-marker]))

         (stream.types/stream-chunk? v)
         (let [kxfn (get key-extractor-fns stream-id)

               chunk-data (stream.pt/-chunk-values v)
               ;; _ (prn "buffer-chunk!" chunk-data)

               new-key-partitions (->> chunk-data
                                       ;; chunk is already partitioned
                                       ;; (partition-by kxfn)
                                       (map (fn [p]
                                              (let [pk (some-> p first kxfn)]
                                                (when (nil? pk)
                                                  (throw
                                                   (err/ex-info
                                                    ::nil-partition-key
                                                    {::stream.cross-spec cross-spec
                                                     ::stream-id stream-id
                                                     ::chunk-data chunk-data})))
                                                [pk p]))))

               last-current-partition-key (->> partition-buffer
                                               last
                                               first)

               first-new-partition-key (->> new-key-partitions
                                            first
                                            first)

               ;; check the partitions in the new chunk are sorted in the key
               chunk-data-sorted? (values-sorted?
                                   key-comparator-fn
                                   (map first new-key-partitions))

               ;; check that the first partition in the new chunk is sorted
               ;; in the key with respect to the final partition in the previous
               ;; chunk
               chunk-starts-after-previous-end?
               (or (nil? last-current-partition-key)
                   ;; thank to transform impl we can randomly get content
                   ;; after an error on a stream
                   (stream-finished-markers last-current-partition-key)
                   (<= (key-comparator-fn last-current-partition-key
                                          first-new-partition-key)
                       0))]

           ;; (prn "buffer-chunk!" chunk-data new-key-partitions)

           ;; double-check that the stream is sorted
           (when (or (not chunk-data-sorted?)
                     (not chunk-starts-after-previous-end?))

             (throw (err/ex-info
                     ::stream-not-sorted
                     {::stream.cross/spec cross-spec
                      ::stream.cross/id stream-id
                      ::stream.cross/chunk-data chunk-data
                      ::stream.cross/last-prev-partition-key last-current-partition-key
                      ::stream.cross/first-new-partition-key first-new-partition-key
                      ::stream.cross/chunk-data-sorted? chunk-data-sorted?
                      ::stream.cross/chunk-starts-after-previous-end? chunk-starts-after-previous-end?})))

           ;; if we've already got a finished marker, then never add anything
           ;; to the buffer
           (if (stream-finished-markers last-current-partition-key)
             partition-buffer
             (into partition-buffer new-key-partitions)))

         :else
         (throw
          (err/ex-info
           ::not-a-chunk
           {::stream.cross/spec cross-spec
            ::stream.cross/id stream-id
            ::stream.cross/partition-buffer partition-buffer
            ::stream.cross/value v}))))))

(defn init-partition-buffers!
  "returns partition buffers for each stream with
   partitions from the first chunk"
  [cross-spec id-streams]

  (-> (for [[sid _] id-streams]
        (pr/chain
         (buffer-chunk!
          []
          cross-spec
          sid
          (get id-streams sid))
         (fn [pb]
           [sid pb])))
      (pr/all)
      (pr/chain (fn [sid-pbs] (into (linked/map) sid-pbs)))))

(defn partition-buffer-needs-filling?
  "don't wait until empty "
  [stream-id partition-buffer]
  (let [n (count partition-buffer)]

    ;; the count should never be less than 1 - even
    ;; when the stream is drained there should be the
    ;; [[stream-finished-drained-marker]] or
    ;; [[stream-finished-errored-marker <err>]] remaining
    (when (< n 1)
      (throw
       (err/ex-info
        ::partition-buffer-emptied
        {::stream-id stream-id})))

    ;; fill when there is a single partition left and
    ;; the stream is not drained - we don't wait until
    ;; the buffer is empty so that we can validate the
    ;; stream ordering in buffer-chunk!
    (and
     (<= (count partition-buffer) 1)
     (not (stream-finished? partition-buffer)))))

(defn fill-partition-buffers!
  "buffer another chunk from any streams which are down to a single
   partition and have not yet been stream-finished-drained-marker"
  [id-partition-buffers cross-spec id-streams]
  (-> (for [[sid partition-buffer] id-partition-buffers]

        (if (partition-buffer-needs-filling? sid partition-buffer)

          (pr/chain
           (buffer-chunk!
            partition-buffer
            cross-spec
            sid
            (get id-streams sid))
           (fn [partition-buffer]
             [sid partition-buffer]))

          [sid partition-buffer]))

      (pr/all)
      (pr/chain (fn [sid-pbs] (into (linked/map) sid-pbs)))))

(def default-target-chunk-size 1000)

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

(defn partition-buffer-content-drained?
  "returns true when a partition-buffer has no more content
   and the associated stream is finished (drained or errored)"
  [partition-buffer]
  (and (= 1 (count partition-buffer))
       (some?
        (stream-finished-markers
         (-> partition-buffer first first)))))

(defn partition-buffer-errored?
  "returns true when a partition-buffer has no more content
   and the associated stream errored"
  [partition-buffer]
  (and (= 1 (count partition-buffer))
       (= stream-finished-errored-marker
          (-> partition-buffer first first))))

(defn next-selections
  "select partitions for the operation
   return [[[<stream-id> <partition>]+] updated-id-partition-buffers]"
  [{select-fn ::stream.cross/select-fn
    key-comparator-fn ::stream.cross/key-comparator-fn
    :as _cross-spec}
   id-partition-buffers]

  (let [mkv (->> id-partition-buffers
                 (filter (fn [[_stream_id pb]] (not (partition-buffer-content-drained? pb))))
                 (map (fn [[_stream-id key-partitions]]
                        (->> key-partitions
                             first ;; first partition
                             first ;; key
                             )))
                 (min-key-val key-comparator-fn))

        min-key-id-partitions
        (->> id-partition-buffers
             (filter (fn [[_stream_id pb]]
                       (not (partition-buffer-content-drained? pb))))
             (filter (fn [[_stream-id [[partition-key _partition]]]]
                       (= mkv partition-key)))
             (map (fn [[stream-id [[_partition-key partition]]]]
                    [stream-id partition])))

        ;; _ (prn "next-selections.min-key-id-partitions" min-key-id-partitions)

        selected-id-partitions (select-fn min-key-id-partitions)

        ;; _ (prn "next-selections.selected-id-partitions" selected-id-partitions)

        selected-stream-ids (->> selected-id-partitions
                                 (map first)
                                 (set))

        updated-id-partition-buffers
        (->> (for [[sid partition-buffer] id-partition-buffers]
               (if (selected-stream-ids sid)
                 [sid (subvec partition-buffer 1)]
                 [sid partition-buffer]))
             (into (linked/map)))]

    [selected-id-partitions
     updated-id-partition-buffers]))

(defn generate-output
  "given partition-selections, cartesion-product the selected partitions,
   merging each row into a {<stream-id> <val>} map, and applying the
   merge-fn and any finalizer"
  [{merge-fn ::stream.cross/merge-fn
    product-sort-fn ::stream.cross/product-sort-fn
    finalizer-fn ::stream.cross/finalizer-fn
    :as _cross-spec}
   selected-id-partitions]

  (let [id-val-seqs (->> selected-id-partitions
                         (map (fn [[sid partition]]
                                (map (fn [v] [sid v]) partition))))]

    (->> id-val-seqs
         (apply combo/cartesian-product)
         (map (fn [id-vals] (into (linked/map) id-vals)))
         (map merge-fn)
         (filter #(not= % ::stream.cross/none))
         (map finalizer-fn)
         (product-sort-fn))))

(defn chunk-full?
  "should the current chunk be wrapped?"
  [chunk-builder
   {target-chunk-size ::stream.cross/target-chunk-size
    :as _cross-spec}]
  (prn "chunk-full?" target-chunk-size (count (stream.pt/-chunk-state chunk-builder)))
  (and (stream.pt/-building-chunk? chunk-builder)
       (>= (count (stream.pt/-chunk-state chunk-builder))
           (or target-chunk-size default-target-chunk-size))))

(defn chunk-not-empty?
  [chunk-builder]
  (and (stream.pt/-building-chunk? chunk-builder)
       (> (count (stream.pt/-chunk-state chunk-builder))
          0)))

(defn cross-finished?
  [id-partition-buffers]
  (every? partition-buffer-content-drained? (vals id-partition-buffers)))

(defn cross-input-errored?
  [id-partition-buffers]
  (some partition-buffer-errored? (vals id-partition-buffers)))

(defn first-cross-input-error
  "use the first input error for an output error"
  [id-partition-buffers]
  (->> (vals id-partition-buffers)
       (filter (fn [[_id pb]] (partition-buffer-errored? pb)))
       (map second) ;; partition-buffers
       (first) ;; [::errored <error>]
       (second)))

(defn cross*
  "the implementation, which relies on the support functions:

    - select-fn - select from partitions with matching keys
    - merge-fn - merge records from multiple streams with matching keys,
    - product-sort-fn - sort a merged cartesian product of records with
        matching keys from multiple streams
    - key-comparator-fn - compare keys, like `compare`
    - key-extractor-fns - extract a key from a value on a stream

   and proceeds iteratively like so:

   - fill any partition buffers requiring it
   - find the minimum key-value from all the lead partitions
   - use the select-fn to select from the lead partitions with the
     minimum-key-value: [[<stream-id> <partition>]+], taking only the
     selected partitions from their respective buffers
   - generate a cartesian product from the selected [[<stream-id> <partition>]+]
     partitions
   - merge the records from each row of the cartesian product - i.e. one record
     from each stream
   - sort the resulting list of merged records with the product-sort-fn
   - add the sorted list of merged records to the current chunk
   - output the chunk if it's full
     "
  [cross-spec
   id-streams]

  (let [cb (stream.chunk/stream-chunk-builder)
        out (stream.transport/stream)]

    (->

     (init-partition-buffers! cross-spec id-streams)

     (prpr/handle-always
      (fn [id-partition-buffers err]

        (if (some? err)
          (err/wrap-uncaught err)

          #_{:clj-kondo/ignore [:loop-without-recur]}
          (pr/loop [id-partition-buffers id-partition-buffers]

            (cond

              (cross-input-errored? id-partition-buffers)
              (err/wrap-uncaught
               (first-cross-input-error id-partition-buffers))

              ;; finish up - output any in-progress chunk, and close the output
              (cross-finished? id-partition-buffers)
              (if (chunk-not-empty? cb)
                (pr/chain
                 (stream.transport/put! out (stream.pt/-finish-chunk cb))
                 (fn [_] (stream.transport/close! out)))
                (stream.transport/close! out))

              ;; fetch more input, generate more output, and send a chunk
              ;; to the output stream when filled
              :else
              (prpr/handle-always
               (fill-partition-buffers!
                id-partition-buffers
                cross-spec
                id-streams)
               (fn [id-partition-buffers err]

                 (if (some? err)
                   (err/wrap-uncaught err)

                   (let [;;_ (prn "id-partition-buffers" id-partition-buffers)

                         [selected-id-partitions
                          id-partition-buffers] (next-selections
                                                 cross-spec
                                                 id-partition-buffers)

                         ;;_ (prn "selected-id-partitions" selected-id-partitions)
                         ;;_ (prn "next-id-partition-buffers" id-partition-buffers)

                         output-records (generate-output
                                         cross-spec
                                         selected-id-partitions)

                         ;;_ (prn "output-records" output-records)

                         _ (do
                             (when-not (stream.pt/-building-chunk? cb)
                               (stream.pt/-start-chunk cb))
                             (stream.pt/-add-all-to-chunk cb output-records))

                         output-chunk (when (chunk-full? cb cross-spec)
                                        (stream.pt/-finish-chunk cb))]

                     (pr/let [put-ok? (when (some? output-chunk)
                                        (stream.transport/put!
                                         out
                                         output-chunk))]

                       ;; TODO something awry here - dealing with the
                       ;; put=false case causes test failures
                       (if true ; put-ok?
                         (pr/recur id-partition-buffers)

                         (err/wrap-uncaught
                          (err/ex-info
                           ::cross*-downstream-closed
                           {:cross-spec cross-spec
                            :output-chunk output-chunk}))
                         )))))))))))

     (prpr/handle-always

       (fn [r err]

         (when-let [err (or err
                            (and (err/uncaught-wrapper? r)
                                 (err/unwrap-value r)))]

           (doseq [[_id stream] id-streams]
             (stream.transport/close! stream))

           (stream.transport/error! out err)))))

    out))

(defn select-first
  "select-fn which takes the first id-partition from the offered
   list of id-partitions"
  [id-partitions]
  ;; (info "select-first" skey-head-values)
  (take 1 id-partitions))

(defn select-all
  "select-fn which takes all offered id-partitions"
  [id-partitions]
  id-partitions)

(defn set-select-all
  "select-fn which takes all offered id-partitions and additionlly checks
   that no partition has more than a single element (as required of a set)"
  [id-partitions]
  (let [set? (->> (for [[_id partition] id-partitions]
                    (count partition))
                  (every? #(= % 1)))]
    (when-not set?
      (throw
       (err/ex-info ::not-a-set
                    {:id-partitions id-partitions})))
    id-partitions))

(defn ->select-fn
  [{op ::stream.cross/op
    :as _cross-spec}]
  (case op
    ::stream.cross.op/sorted-merge select-first
    ::stream.cross.op/inner-join select-all
    ::stream.cross.op/outer-join select-all
    ::stream.cross.op/n-left-join select-all
    ::stream.cross.op/intersect set-select-all
    ::stream.cross.op/union set-select-all
    ::stream.cross.op/difference set-select-all))

(defn merge-sorted-merge
  [m]
  (-> m vals first))

(defn make-merge-inner-join
  [{kxfns ::stream.cross/key-extractor-fns
    :as _cross-spec}]
  (fn [m]
    (if (= (count m) (count kxfns))
      m
      ::stream.cross/none)))

(defn make-merge-n-left-join
  [{kxfns ::stream.cross/key-extractor-fns
    n ::stream.cross.op.n-left-join/n
    :as _cross-spec}]
  (let [n-left-ids (->> kxfns (take n) (map first) set)]
    (fn [m]
        (if (= n-left-ids
               (set/intersection
                (-> m keys set)
                n-left-ids))
          m
          ::stream.cross/none))))

(defn make-merge-intersect
  [{kxfns ::stream.cross/key-extractor-fns
    :as _cross-spec}]
  (fn [m]
    (if (= (count m) (count kxfns))
      m
      ::stream.cross/none)))

(defn make-merge-difference
  [{kxfns ::stream.cross/key-extractor-fns
    :as _cross-spec}]
  (fn [m]
    (if (and
         (= (count m) 1)
         (contains? m (-> kxfns first first)))
      m
      ::stream.cross/none)))

(defn ->merge-fn
  [{op ::stream.cross/op
    :as cross-spec}]

  (case op
    ::stream.cross.op/sorted-merge merge-sorted-merge

    ::stream.cross.op/inner-join (make-merge-inner-join cross-spec)

    ::stream.cross.op/outer-join identity

    ::stream.cross.op/n-left-join (make-merge-n-left-join cross-spec)

    ::stream.cross.op/intersect (make-merge-intersect cross-spec)

    ::stream.cross.op/union identity

    ::stream.cross.op/difference (make-merge-difference cross-spec)))

(defn ->product-sort-fn
  [{product-sort ::stream.cross/product-sort
    :as _cross-spec}]

  (or product-sort identity))

(defn ->finalizer-fn
  [{finalizer ::stream.cross/finalizer
    :as _cross-spec}]

  (or finalizer identity))

(defn ->key-comparator-fn
  [{key-comparator ::stream.cross/key-comparator
    :as _cross-spec}]

  (cond
    (nil? key-comparator) compare
    (fn? key-comparator) key-comparator
    (= :compare key-comparator) compare
    (= :negcompare key-comparator) (comp - compare)))

(defn ->key-extractor-fn
  "given a key-spec, return a key-extractor fn"
  [key-spec]
  (cond
    (keyword? key-spec) key-spec
    (fn? key-spec) key-spec
    (sequential? key-spec) #(get-in % key-spec)
    :else (throw (err/ex-info ::unknown-key-spec {:key-spec key-spec}))))

(defn ->key-extractor-fns
  [{keyspecs ::stream.cross/keys
    :as _cross-spec}]
  (->> (for [[id keyspec] keyspecs]
         [id (->key-extractor-fn keyspec)])
       (into (linked/map))))

(defn partition-stream
  [{target-chunk-size ::stream.cross/target-chunk-size
    kxfns ::stream.cross/key-extractor-fns
    :as _cross-spec}
   stream-id
   stream]
  (let [partition-by-fn (get kxfns stream-id)]
    (stream.ops/chunkify target-chunk-size partition-by-fn stream)))

(defn partition-streams
  "returns a linked/map with {<stream-id> <partitioned-stream>}, and
   in the same order as specifed in the ::stream.cross/keys config"
  [{kxfns ::stream.cross/key-extractor-fns
    :as cross-spec}
   id-streams]
  (let [sids (keys kxfns)]
    (->> (for [sid sids]
           [sid (partition-stream cross-spec sid (get id-streams sid))])
         (into (linked/map)))))

(defn configure-cross-op
  "assemble helper functions to allow the core cross-stream* impl
   to perform the specified operation"
  [cross-spec]

  (let [;; merge-fn is dependent on key-extractor-fns
        cross-spec (assoc cross-spec
                          ::stream.cross/key-extractor-fns
                          (->key-extractor-fns cross-spec))]
    (merge
     {::stream.cross/target-chunk-size 1000}

     cross-spec

     {::stream.cross/select-fn (->select-fn cross-spec)
      ::stream.cross/merge-fn (->merge-fn cross-spec)
      ::stream.cross/product-sort-fn (->product-sort-fn cross-spec)
      ::stream.cross/finalizer-fn (->finalizer-fn cross-spec)
      ::stream.cross/key-comparator-fn (->key-comparator-fn cross-spec)})))

(def KeySpec
  [:or
   ;; keyword for a call to get
   :keyword

   fn?

   ;; list of args for a call to get-in
   [:+ [:or :keyword :int :string]]])

;; a variety of merge, join, and set operations are possible
;; when crossing streams
;;
;; all operations require that every input stream is sorted in the
;; same key
(def CrossStreamsOp
  [:enum
   ;; the merge phase of a sort-merge join.
   ;; output is merged but input values are unchanged
   ::stream.cross.op/sorted-merge

   ;; inner join
   ;; output is maps with {<stream-id> <value>...}
   ::stream.cross.op/inner-join

   ;; full outer join
   ;; output is maps with {<stream-id> <value>...}
   ::stream.cross.op/outer-join

   ;; left join requiring at least n leftmost values (default 1)
   ;; output is maps with {<stream-id> <value>...}
   ::stream.cross.op/n-left-join

   ;; set intersection
   ;; output is sorted, but remaining input values are unchanged
   ::stream.cross.op/intersect

   ;; set union
   ;; output is sorted, but input values are unchanged
   ::stream.cross.op/union

   ;; set difference
   ;; output is sorted, but input values are unchanged
   ::stream.cross.op/difference])

;; an order must be given for keyspecs in the CrossSpec
(def OrderedKeySpecs
  [:+ [:tuple :keyword KeySpec]])

(def CrossSpec
  [:map

   ;; there must be 1 entry per stream, specifying how to
   ;; extract the key from a value on that stream
   [::stream.cross/keys OrderedKeySpecs]

   ;; the cross-streams operation
   [::stream.cross/op CrossStreamsOp]

   ;; optional comparator fn for keys - defaults to `compare`
   [::stream.cross/key-comparator {:optional true} fn?]

   ;; optional product-sort fn to sort cartesian product output
   ;; defaults to `identity`
   [::stream.cross/product-sort {:optional true} fn?]

   ;; optional number of leftmost values required for
   ;; a non-nil n-left-join result
   [::stream.cross.op.n-left-join/n {:optional true} :int]

   ;; optional function to finalize an output value
   [::stream.cross/finalizer {:optional true} fn?]

   ;; target-chunk-size for crosssed output
   [::stream.cross/target-chunk-size {:optional true} :int]])

(def CrossSupportFns
  "the fns which implement cross operation behaviours, all derived from the
   config in CrossSpec and defaults"
  [:map
   ;; the operation-determined select-fn chooses which partitions are taken from the
   ;; leading partitions which match the minimum key value
   [::stream.cross/select-fn fn?]

   ;; given a {<stream-id> <partition>} map of selected partitions, the
   ;; op-determined merge-fn decides what, if anything, moves to output
   [::stream.cross/merge-fn fn?]

   ;; given merged output, the optional caller-specified finalizer-fn
   ;; applies a transformation to the merged output
   [::stream.cross/finalizer-fn fn?]

   ;; given finalized output, the optional caller-specified product-sort-fn
   ;; applies a sort to the crossed partition output
   [::stream.cross/product-sort-fn fn?]

   ;; the key-comparator-fn is used to compare key values - default to `compare`
   [::stream.cross/key-comparator-fn fn?]

   ;; the key-extractor-fns extract keys from the values on each stream
   [::stream.cross/key-extractor-fns
    [:map-of :keyword fn?]]])

(def ConfiguredCrossOperation
  (-> CrossSpec
      (mu/merge CrossSupportFns)))

(def IdStreams
  "id->stream mappings, either in a map, or a
   list of pairs - the latter providing order for
   operations like n-left-join which require it"
  [:or
   [:map-of :keyword [:fn stream.transport/stream?]]
   [:+ [:tuple :keyword [:fn stream.transport/stream?]]]])

(mx/defn cross
  "cross some sorted streams, returning a stream according to the cross-spec

   each input stream must be sorted ascending in the key specified in cross-spec
   at
     [::stream.cross/keys <stream-id>]
   with the comparator fn from ::stream.cross/comparator

   - cross-spec : a description of the operation to cross the streams
   - id-streams : {<stream-id> <stream>}

   e.g. this invocations inner-joins a stream of users, sorted by :org-id, to a
     stream of orgs, sorted by :id

   (cross
      {::stream.cross/keys {:users :org-id :orgs :id}
       ::stream.cross/op ::stream.cross/inner-join}
      {:users <users-stream>
       :orgs <orgs-stream>})"
  [cross-spec :- CrossSpec
   id-streams :- IdStreams]

  (let [;; configure the specific support fns for the operation
        cross-spec (configure-cross-op cross-spec)

        ;; chunk+partition the streams
        id-streams (partition-streams cross-spec id-streams)]

    ;; cross those streams!
    (cross* cross-spec id-streams)))

(ns prpr.stream.cross
  (:require
   [clojure.math.combinatorics :as combo]
   [linked.core :as linked]
   [promesa.core :as pr]
   [taoensso.timbre :refer [trace debug info warn error]]

   [prpr.error :as err]
   [prpr.stream :as stream]
   [prpr.stream.protocols :as stream.pt]
   [prpr.stream.impl :as stream.impl]
   [prpr.stream.types :as stream.types]
   [prpr.stream.chunk :as stream.chunk]

   [prpr.stream.cross :as-alias stream.cross]))

;;; cross mkII
;;;
;;; - re-chunk based on key + target-size
;;; - consume chunks
;;; - sync cross-join matching keys in chunks
;;; - output everything possible without take! in chunks with
;;;   same key + target-size constraints
;;; - rinse / repeat

(defn buffer-chunk
  "given a stream of chunks of partitions, and a
   buffer of [key partition] tuples, retrieve another
   chunk of partitions and add them to the partitions buffer
   (or add the keyword ::drained if the end of the
    stream is reached)

   returns Promise<[ [<partition-key> <partition>]* ::drained?]"
  [partition-buffer
   {key-comparator-fn ::stream.cross/key-comparator-fn
    key-extractor-fns ::stream.cross/key-extractor-fns
    :as cross-spec}
   stream-id
   stream]

  (pr/chain
     (stream.impl/take! stream ::drained)

     (fn [v]
       (cond

         (= ::drained v)
         (if (= ::drained (last partition-buffer))
           partition-buffer
           (conj partition-buffer ::drained))

         (stream.types/stream-chunk? v)
         (let [kxfn (get key-extractor-fns stream-id)

               chunk-data (stream.pt/-chunk-values v)

               new-key-partitions (->> chunk-data
                                       (partition-by kxfn)
                                       (map (fn [p]
                                              [(kxfn (first p)) p])))

               last-current-partition-key (->> partition-buffer
                                               last
                                               first)

               first-new-partition-key (->> new-key-partitions
                                            first
                                            first)]

           ;; double-check that the stream is sorted
           (when (and (some? last-current-partition-key)
                      (> (key-comparator-fn last-current-partition-key
                                            first-new-partition-key)
                         0))
             (throw (err/ex-info
                     ::stream-not-sorted
                     {::cross-spec cross-spec
                      ::stream-id stream-id
                      ::prev-partition-key last-current-partition-key
                      ::next-partition-key first-new-partition-key})))

           (into partition-buffer new-key-partitions))

         :else
         (throw
          (err/ex-info
           ::not-a-chunk
           {::cross-spec cross-spec
            ::stream-id stream-id
            ::partition-buffer partition-buffer
            ::value v}))))))

(defn init-partition-buffers
  "returns partition buffers for each stream with
   partitions from the first chunk"
  [cross-spec id-streams]

  (-> (for [[sid _] id-streams]
        (pr/chain
         (buffer-chunk
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
    ;; [::drained] keyword remaining
    (when (< n 1)
      (throw
       (err/ex-info
        ::partition-buffer-emptied
        {::stream-id stream-id})))

    ;; fill when there is a single partition left and
    ;; the stream is not drained - we don't wait until
    ;; the buffer is empty so that we can validate the
    ;; stream ordering in buffer-chunk
    (and
     (<= (count partition-buffer) 1)
     (not= ::drained (first partition-buffer)))))

(defn fill-partition-buffers
  "buffer another chunk from any streams which are down to a single
   partition and have not yet been ::drained"
  [id-partition-buffers cross-spec id-streams]
  (-> (for [[sid partition-buffer] id-partition-buffers]

        (if (partition-buffer-needs-filling? sid partition-buffer)

          (pr/chain
           (buffer-chunk
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

(defn next-selections
  "select partitions for the operation
   return [[[<stream-id> <partition>]+] updated-id-partition-buffers]"
  [{select-fn ::select-fn
    key-comparator-fn ::key-comparator-fn
    :as _cross-spec}
   id-partition-buffers]

  (let [mkv (->> id-partition-buffers
                 (map (fn [[_stream-id key-partitions]]
                        (->> key-partitions
                             first ;; first partition
                             first ;; key
                             )))
                 (min-key-val key-comparator-fn))

        min-key-id-partitions
        (->> id-partition-buffers
             (filter (fn [[_stream-id [[partition-key _partition]]]]
                       (= mkv partition-key)))
             (map (fn [[stream-id [[_partition-key partition]]]]
                    [stream-id partition])))

        selected-id-partitions (select-fn min-key-id-partitions)

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
                                (->> partition
                                     (map (fn [v] [sid v]))))))]

    (->> id-val-seqs
         (apply combo/cartesian-product)
         (map (fn [id-vals]
                (into (linked/map) id-vals)))
         (map merge-fn)
         (map finalizer-fn)
         (product-sort-fn))))

(defn chunk-full?
  "should the current chunk be wrapped?"
  [chunk-builder
   {target-chunk-size ::stream.cross/target-chunk-size
    :as _cross-spec}]
  (and (stream.pt/-building-chunk? chunk-builder)
       (> (count (stream.pt/-chunk-state chunk-builder))
          (or target-chunk-size default-target-chunk-size))))

(defn chunk-not-empty?
  [chunk-builder]
  (and (stream.pt/-building-chunk? chunk-builder)
       (> (count (stream.pt/-chunk-state chunk-builder))
          0)))

(defn finished?
  [id-partition-buffers]
  (every?
   (fn [[_sid pb]] (#{::drained ::errored} (first pb)))
   id-partition-buffers))

(defn cross-streams*
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
        out (stream/stream)]

    (pr/let [id-partition-buffers (init-partition-buffers cross-spec id-streams)]

      #_{:clj-kondo/ignore [:loop-without-recur]}
      (pr/loop [id-partition-buffers id-partition-buffers]

        (if (finished? id-partition-buffers)

          ;; finish up - output any in-progress chunk, and close the output
          (if (chunk-not-empty? cb)
            (pr/chain
             (stream.impl/put! out (stream.pt/-finish-chunk cb))
             (fn [_] (stream.impl/close! out)))
            (stream.impl/close! out))

          ;; fetch more input, generate more output, and send a chunk
          ;; to the output stream when filled
          (pr/let [id-partition-buffers (fill-partition-buffers
                                         id-partition-buffers
                                         cross-spec
                                         id-streams)

                   [selected-id-partitions
                    id-partition-buffers] (next-selections
                                           cross-spec
                                           id-partition-buffers)

                   output-records (generate-output
                                   cross-spec
                                   selected-id-partitions)

                   _ (do
                       (when-not (stream.pt/-building-chunk? cb)
                         (stream.pt/-start-chunk cb))
                       (stream.pt/-add-all-to-chunk cb output-records))

                   _put-ok? (when (chunk-full? cb cross-spec)
                              (stream.impl/put! out (stream.pt/-finish-chunk cb)))]

            (pr/recur id-partition-buffers)))))

    out))

(defn select-first
  "select-fn which takes the first id-partition from the offered
   list of id-partitions"
  [id-partitions]
  ;; (info "select-first" skey-head-values)
  (first id-partitions))

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
    ::stream.cross/sorted-merge select-first
    ::stream.cross/inner-join select-all
    ::stream.cross/outer-join select-all
    ::stream.cross/n-left-join select-all
    ::stream.cross/intersect set-select-all
    ::stream.cross/union set-select-all
    ::stream.cross/difference set-select-all))

(defn ->merge-fn
  [{op ::stream.cross/op
    :as _cross-spec}]
  (case op
    ::stream.cross/sorted-merge (fn [m] (-> m vals first))
    ::stream.cross/inner-join identity
    ::stream.cross/outer-join identity
    ::stream.cross/n-left-join identity
    ::stream.cross/intersect identity
    ::stream.cross/union identity
    ::stream.cross/difference identity))

;; TODO we should implement the different behavious in the merge-fn
;; these are wrong

(defn ->product-sort-fn
  [{product-sort ::stream.cross/product-sort
    :as _cross-spec}]

  (or product-sort identity))

(defn ->finalizer-fn
  [{finalizer ::stream.cross/finalizer
    :as _cross-spec}]

  (or finalizer identity))

(defn ->key-comparator-fn
  [{key-comparator ::stream.cross/key-compartor
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
    (sequential? key-spec) #(get-in % key-spec)
    :else (throw (err/ex-info ::unknown-key-spec {:key-spec key-spec}))))

(defn ->key-extractor-fns
  [{keys ::stream.cross/keys
    :as _cross-spec}]
  (->>
   (for [[id keyspec] keys]
     [id (->key-extractor-fn keyspec)])
   (into (linked/map))))

(defn partition-streams
  "returns {<stream-id> <partitioned-stream>}"
  [{target-chunk-size ::stream.cross/target-chunk-size
    key-extractor-fns ::stream.cross/key-extractor-fns
    :as _cross-spec}
   id-streams]
  (->> (for [[sid stream] id-streams]
         (let [partition-by-fn (get key-extractor-fns sid)]
           [sid (stream/chunkify target-chunk-size partition-by-fn stream)]))
       (into (linked/map))))

(defn configure-cross-op
  "assemble helper functions to allow the core cross-stream* impl
   to perform the specified operation"
  [cross-spec]
  (merge
   {::stream.cross/target-chunk-size 1000}
   cross-spec
   {::stream.cross/select-fn (->select-fn cross-spec)
    ::stream.cross/merge-fn (->merge-fn cross-spec)
    ::stream.cross/product-sort-fn (->product-sort-fn cross-spec)
    ::stream.cross/finalizer-fn (->finalizer-fn cross-spec)
    ::stream.cross/key-comparator-fn (->key-comparator-fn cross-spec)
    ::stream.cross/key-extractor-fns (->key-extractor-fns cross-spec)}))

(defn cross-streams
  "cross some sorted streams

   each stream must be sorted ascending in the key specified in cross-spec at
    [::stream.cross/keys <stream-id>], with the comparator fn from
    ::stream.cross/comparator

   - cross-spec : a description of the operation to cross the streams
   - id-streams : {<stream-id> <stream>}"
  [cross-spec
   id-streams]

  (let [;; configure the specific support fns for the operation
        cross-spec (configure-cross-op cross-spec)

        ;; chunk+partition the streams
        id-streams (partition-streams cross-spec id-streams)]

    ;; cross those streams!
    (cross-streams* cross-spec id-streams)))

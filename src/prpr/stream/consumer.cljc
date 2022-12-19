(ns prpr.stream.consumer
  "consume from multiple streams in a chunk and
   error sensitive way"
  (:require
   [promesa.core :as pr]
   [prpr.stream.protocols :as pt]
   [prpr.stream.transport :as transport]
   [prpr.stream.types :as types]))

;; maintains some state
;; uses a buffer list of unconsumed values allowing
;; - chunk values to be added to the end of the buffer
;; - values to be removed from the front of the buffer
;; - pushback to be prepended to the front buffer
;; - end-of-stream to be marked with :prpr.stream/end in the buffer
;; - errors to be marked with a stream-error in the buffer

(deftype ValueConsumer [s buf-a]
  pt/IValueConsumer
  (-peek-value [_]
    (let [buf @buf-a]
      (if (not-empty buf)

        (first buf)

        (pr/let [v (transport/take! s :prpr.stream/end)]
          (cond

            ;; terminal conditions
            (or (= :prpr.stream/end v)
                (types/stream-error? v))
            (do
              (swap! buf-a conj v)
              v)

            ;; append all of chunk to buf
            (types/stream-chunk? v)
            (let [[v :as vals] (list* (pt/-chunk-values v))]
              (reset! buf-a vals)
              v)

            ;; append value to buf
            :else
            (do
              (swap! buf-a conj v)
              v))))))

  (-take-value! [_]
    (let [[fv :as buf] @buf-a]
      (if (not-empty buf)

        (cond

          ;; terminal conditions
          (or (= :prpr.stream/end fv)
              (types/stream-error? fv))
          fv

          ;; take first val from buf
          :else
          (do
            (swap! buf-a rest)
            fv))

        (pr/let [v (transport/take! s :prpr.stream/end)]
          (cond

            ;; terminal conditions
            (or (= :prpr.stream/end v)
                (types/stream-error? v))
            (do
              (swap! buf-a conj v)
              v)

            ;; append rest of chunk to buf, return first val
            (types/stream-chunk? v)
            (let [[v & rvs] (list* (pt/-chunk-values v))]
              (reset! buf-a rvs)
              v)

            ;; return plain value
            :else
            v))))))

(defn value-consumer
  [s]
  (->ValueConsumer s (atom '())))

(deftype ChunkConsumer [s buf-a]
  pt/IChunkConsumer
  (-peek-chunk [_]
    (let [buf @buf-a]
      (if (not-empty buf)
        (first buf)
        (pr/let [v (transport/take! s :prpr.stream/end)]
          (swap! buf-a conj v)
          v))))

  (-take-chunk! [_]
    (let [[fv :as buf] @buf-a]
      (if (not-empty buf)
        (cond

          ;; terminal conditions
          (or (= :prpr.stream/end fv)
              (types/stream-error? fv))
          fv

          ;; consume first val from buf
          :else
          (do
            (swap! buf-a rest)
            fv))

        (pr/let [;; _ (info "about to -take!")
                 v (transport/take! s :prpr.stream/end)]
          (cond

            ;; buffer and return terminal conditions
            (or (= :prpr.stream/end v)
                (types/stream-error? v))
            (do
              ;; (info "terminal" v)
              (swap! buf-a conj v)
              v)

            ;; return chunk or plain value
            :else
            v)))))

  (-pushback-chunk! [_ chunk-or-val]
    (when (or (= :prpr.stream/end chunk-or-val)
              (types/stream-error? chunk-or-val))
      (throw
       (ex-info "can't pushback EOS or error"
                {:chunk-or-val chunk-or-val})))

    (swap! buf-a conj chunk-or-val)))

(defn chunk-consumer
  [s]
  (->ChunkConsumer s (atom '())))

(defn chunk-zip
  "zip values from input streams to vectors on the output stream in
   a chunk and error sensitive way

   (chunk-zip Stream<a> Stream<b> Stream<c> ...) -> Stream<[a b c ...]>

   if the inputs are all chunked then the output will also be chunked.
   the size of the output chunks will be determined by the smallest
   chunk size of the inputs

   one of the more complex consumption patterns - not dissimilar to
   a join in that it consumes multiple streams, consumes partial chunks
   and pushes unconsumed values 'back' onto streams"
  [& srcs]

  (let [;; we feed the sources into intermediates so we can close
        ;; them without affecting any objects we don't own (the srcs),
        ;; which, in turn, will *maybe* cause the upstream streams
        ;; to close by default, but only if the intermediates were
        ;; their only downstream channels (manifold behaviour)
        intermediates (repeatedly
                       (count srcs)
                       transport/stream)

        out (transport/stream)
        consumers (->> intermediates
                       (map chunk-consumer)
                       (into []))

        close-all (fn []
                    (->> (conj intermediates out)
                         (map pt/-close!)
                         (pr/all)))]

    (doseq [[src interm] (map vector srcs intermediates)]
      (pt/-connect-via
       src
       (fn [v]
         ;; (prn "connect-via-interm" v)
         (pt/-put! interm v))
       interm))

    (pr/catch
        #_{:clj-kondo/ignore [:loop-without-recur]}
        (pr/loop []

          (pr/chain

           ;; get a vector of chunk-or-values from sources
           (->> consumers
                (map pt/-take-chunk!)
                (pr/all))

           ;; output the biggest possible chunk of zipped values
           (fn [chunk-or-vals]
             ;; (info "chunk-or-vals" chunk-or-vals)

             (let [;; has any source ended ?
                   end? (some #(= :prpr.stream/end %) chunk-or-vals)

                   ;; were there any errors ?
                   errors? (some types/stream-error? chunk-or-vals)

                   ;; did all the sources supply chunks ?
                   all-chunks? (every? types/stream-chunk? chunk-or-vals)

                   ;; the largest possible output chunk size is the
                   ;; size of the smallest source chunk
                   output-chunk-size (if all-chunks?
                                       (->> chunk-or-vals
                                            (filter types/stream-chunk?)
                                            (map pt/-chunk-values)
                                            (map count)
                                            (apply min))
                                       1)]

               (cond

                 ;; one or more inputs has ended - close the output normally
                 end?
                 (pr/chain
                  (close-all)
                  (fn [_] false))

                 ;; one or more inputs has errored - error and close the output
                 errors?
                 (let [first-err (->> chunk-or-vals (filter types/stream-error?) first)]
                   (pr/chain
                    (transport/error! out first-err)
                    (fn [_] (close-all))
                    (fn [_] false)))

                 ;; all the inputs are chunks - so output a new chunk of
                 ;; zipped values with size of the smallest input chunks,
                 ;; and push the remainders of the chunks back on to the
                 ;; consumers
                 all-chunks?
                 (let [vs-rems (for [corv chunk-or-vals]
                                 (let [vals (pt/-chunk-values corv)]
                                   [(subvec vals 0 output-chunk-size)
                                    (when (> (count vals) output-chunk-size)
                                      (types/stream-chunk
                                       (subvec vals output-chunk-size)))]))
                       vs (map first vs-rems)
                       rems (map second vs-rems)

                       zipped-vs (apply map vector vs)
                       zipped-vs-chunk (types/stream-chunk
                                        zipped-vs)]

                   (doseq [[consumer rem] (map vector consumers rems)]
                     (when (some? rem)
                       (pt/-pushback-chunk! consumer rem)))

                   (pt/-put! out zipped-vs-chunk))

                 ;; at least one plain val, so no chunks on
                 ;; output - take one value from each input and
                 ;; push any chunk remainders back onto the
                 ;; consumers
                 :else
                 (let [v-rems (for [corv chunk-or-vals]
                                (if (types/stream-chunk? corv)
                                  (let [cvs (pt/-chunk-values corv)
                                        ;; use subvec rather than
                                        ;; descructuring to get remaining
                                        ;; values, or we get a seq
                                        [fv rvs] [(first cvs)
                                                  (subvec cvs 1)]]
                                    [fv (types/stream-chunk rvs)])
                                  [corv nil]))
                       vs (map first v-rems)
                       rems (map second v-rems)
                       zipped-vs (vec vs)]

                   (doseq [[consumer rem] (map vector consumers rems)]
                     (when (some? rem)
                       (pt/-pushback-chunk! consumer rem)))

                   (pt/-put! out zipped-vs)))))

           ;; recur if there is more to come
           (fn [result]
             (when result
               (pr/recur)))))

        ;; catchall cleanup
        (fn [e]
          ;; (error e "zip error")
          (transport/error! out e)
          (close-all)))

    out))

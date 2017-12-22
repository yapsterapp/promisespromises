(ns prpr.stream.partition-by
  (:refer-clojure
   :exclude [partition-by])
  (:require
   [taoensso.timbre :refer [log trace debug info warn error]]
   [manifold
    [deferred :as d]
    [stream :as s]])
  (:import
   [manifold.stream.core IEventSource]))


(defn partition-by
  "would have like to use a clojure.core/partition-by transducer
   but https://github.com/ztellman/manifold/issues/146
   so... copied and simplified from
   manifold.stream/partition-by"
  [f s]
  (let [in (s/stream)
        out (s/stream)]

    (@#'manifold.stream/connect-via-proxy s in out {:description {:op "partition-by"}})

    (d/loop [prev ::x, acc []]
      (d/chain' (s/take! in ::none)
        (fn [msg]
          (if (identical? ::none msg)
            (if (not-empty acc)
              (d/chain' (s/put! out acc)
                        (fn [_]
                          (s/close! out)))
              (s/close! out))
            (let [curr (try
                         (f msg)
                         (catch Throwable e
                           (s/close! in)
                           (s/close! out)
                           (error e "error in partition-by")
                           ::error))]
              (when-not (identical? ::error curr)
                (if (= prev curr)
                  (d/recur curr (conj acc msg))
                  (if (= ::x prev)
                    (d/recur curr [msg])
                    (d/chain' (s/put! out acc)
                              (fn [_] (d/recur curr [msg])))))))))))
    (s/source-only out)))

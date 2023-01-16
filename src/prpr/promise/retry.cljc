(ns prpr.promise.retry
  (:require
   [promesa.core :as pr]
   [prpr.promise :as prpr]
   [prpr.error :as err]
   [taoensso.timbre :refer [warn]]))

(defn retry-n*
  "execute a fn repeatedly until it succeeds
   - f - a 1-args function of try-count, yielding a promise
   - max-retries - maximum number of times to re-try f before
                   giving up (if 0 then f will be invoked just once)
   - delay-ms - delay between invocations of f"
  [f log-description max-retries delay-ms]

  #_{:clj-kondo/ignore [:loop-without-recur]}
  (pr/loop [n 0
            p (f 0)]
    (prpr/handle-always
     p
     (fn [r e]

       (if (some? e)
         (if (< n max-retries)

           (do
             ;; only warn in the retry case - the exception
             ;; thrown should otherwise be enough
             (warn "retrying promise:" n log-description)

             (pr/chain

              (pr/timeout
               (pr/deferred)
               delay-ms
               ::timeout)

              (fn [_]
                (pr/recur
                 (inc n)
                 (f (inc n))))))

           (err/wrap-uncaught e))

         r)))))

(defn retry-n
  [f log-description max-retries delay-ms]
  (pr/let [r (retry-n* f log-description max-retries delay-ms)]
    (err/unwrap r)))

(defn retry
  [f log-description max-retries delay-ms]

  (retry-n
   (fn [_n] (f))
   log-description
   max-retries
   delay-ms))

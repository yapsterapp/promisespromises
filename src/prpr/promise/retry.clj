(ns prpr.promise.retry
  (:require
   [promesa.core :as pr]
   [taoensso.timbre :refer [warn]]))

(defn retry-n
  "given a 0-args fn f which yields a promise,
   execute that fn repeatedly until it succeds
   - f - a 0-args function yielding a promise
   - max-retries - maximum number of times to re-try f before
                   giving up (if 0 then f will be invoked just once)
   - delay-ms - delay between invocations of f"
  [f
   log-description
   max-retries
   delay-ms]

  #_{:clj-kondo/ignore [:loop-without-recur]}
  (pr/loop [n 0
            p (f 0)]
    (pr/catch
        p
        (fn [e]

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

            (throw e))))))

(defn retry
  [f
   log-description
   max-retries
   delay-ms]

  (retry-n
   (fn [_n] (f))
   log-description
   max-retries
   delay-ms))

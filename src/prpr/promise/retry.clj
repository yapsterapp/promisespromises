(ns prpr.promise.retry
  (:require
   [manifold.deferred :as d]
   [taoensso.timbre :refer [warn]]))

(defn retry
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

  (d/loop [p (f)
           n max-retries]
    (d/catch
        p
        (fn [e]

          (if (> n 0)

            (do
              ;; only warn in the retry case - the exception
              ;; thrown should otherwise be enough
              (warn "retrying promise:" n log-description)
              (d/chain
               (d/timeout!
                (d/deferred)
                delay-ms
                ::timeout)
               (fn [_]
                 (d/recur
                  (f)
                  (dec n)))))

            (throw e))))))

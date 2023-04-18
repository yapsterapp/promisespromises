(ns promisespromises.stream.test
  (:require
   [promesa.core :as pr]
   [promisespromises.promise :as prpr]
   [promisespromises.stream.protocols :as pt]
   [promisespromises.stream.transport :as stream.transport]
   ))

(defn stream-of
  "returns a stream of the individual values
   (*not* chunked)"
  [vs]
  (let [s (stream.transport/stream)]
    (stream.transport/put-all-and-close! s vs)
    s))

(defn consume
  "consume a stream to a vector. an error
   will be added to the end of the vector
   as [::error <error>]"
  [s]
  (pr/loop [rs []]
    (prpr/handle-always
     (stream.transport/take! s ::drained)
     (fn [v e]
       (cond
         (some? e) (conj rs [::error e])

         (= ::drained v) rs

         :else
         (pr/recur (conj rs v)))))))

(defn safe-take!
  "transport/take! (with unwrapping) from a stream returning
   Promise<[::ok <val>]> | Promise<[::error <err>]>"
  [s & args]
  (prpr/handle-always
   (apply stream.transport/take! s args)
   (fn [v e]
     (if (some? e)
       [::error e]
       [::ok v]))))

(defn safe-consume
  "keep safe-take! ing until ::closed, returning
   a vector of safe-take!s"
  [s]
  #_{:clj-kondo/ignore [:loop-without-recur]}
  (pr/loop [r []]
    (pr/let [[_t v :as t-v] (safe-take! s ::closed)]
      (if (= ::closed v)
        (conj r t-v)
        (pr/recur (conj r t-v))))))


(defn safe-low-take!
  "take! directly from a stream impl without any unwrapping
   Promise<[::ok <val>]> | Promise<[::error <err>]>"
  [s & args]
  (prpr/handle-always
   (apply pt/-take! s args)
   (fn [v e]
     (if (some? e)
       [::error e]
       [::ok v]))))

(defn safe-low-consume
  "keep safe-low-take! ing until ::closed, returning
   a vector of safe-low-take!s"
  [s]
  #_{:clj-kondo/ignore [:loop-without-recur]}
  (pr/loop [r []]
    (pr/let [[_t v :as t-v] (safe-low-take! s ::closed)]
      (if (= ::closed v)
        (conj r t-v)
        (pr/recur (conj r t-v))))))

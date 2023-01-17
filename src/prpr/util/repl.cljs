(ns prpr.util.repl
  (:require
   [promesa.core :as p]))

(defrecord Error [error])

(defn error?
  [v]
  (instance? Error v))

(defn capture
  "capture a promise in an atom - for easy repl inspection"
  [p]
  (let [a (atom ::unfulfilled)]
    (p/handle
     p
     (fn [succ err]
       (if (some? err)
         (reset! a (->Error err))
         (reset! a succ))))

    a))

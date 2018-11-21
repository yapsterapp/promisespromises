(ns prpr.cats.writer
  (:require
   [cats.context :as ctx]))

(defprotocol MonadWriter
  "A specific case of Monad abstraction that
  allows emulate write operations in pure functional
  way.

  A great example is writing a log message."
  (-listen [m mv] "Given a writer, yield a (value, log) pair as a value.")
  (-tell [m v] "Add the given value to the log.")
  (-pass [m mv]
    "Given a writer whose value is a pair with a function as its second element,
     yield a writer which has the first element of the pair as the value and
     the result of applying the aforementioned function to the log as the new log."))

(defn tell
  ([v] (tell (ctx/infer) v))
  ([ctx v] (-tell ctx v)))

(defn listen
  ([mv] (listen (ctx/infer mv) mv))
  ([ctx mv] (-listen ctx mv)))

(defn pass
  ([mv] (pass (ctx/infer mv) mv))
  ([ctx mv] (-pass ctx mv)))

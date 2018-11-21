(ns prpr.cats.reader
  (:require
   [cats.context :as ctx]))

(defprotocol MonadReader
  "A specific case of Monad abstraction that
  allows a read only access to an environment."
  (-ask [m] "Return the current environment.")
  (-local [m f reader] "Create a reader in a modified version of the environment."))

(defn ask
  ([] (ask (ctx/infer)))
  ([ctx] (-ask ctx)))

(defn local
  ([f reader] (local (ctx/infer reader) f reader))
  ([ctx f reader] (-local ctx f reader)))

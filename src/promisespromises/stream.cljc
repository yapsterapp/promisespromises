(ns promisespromises.stream
  (:refer-clojure :exclude [concat count filter map mapcat reduce reductions])
  (:require
   [promisespromises.stream.transport :as transport]
   [promisespromises.stream.operations :as operations]
   [promisespromises.stream.cross-impl :as cross-impl]))

(def stream transport/stream)
(def stream? transport/stream?)
(def put! transport/put!)
(def error! transport/error!)
(def take! transport/take!)
(def close! transport/close!)
(def connect-via transport/connect-via)

(def put-all! operations/put-all!)
(def put-all-and-close! operations/put-all-and-close!)
(def ->source operations/->source)
(def realize-each operations/realize-each)
(def transform operations/transform)
(def map operations/map)
(def mapcon operations/mapcon)
(def zip operations/zip)
(def mapcat operations/mapcat)
(def filter operations/filter)
(def reductions operations/reductions)
(def reduce operations/reduce)
(def count operations/count)
(def chunkify operations/chunkify)

(def cross cross-impl/cross)

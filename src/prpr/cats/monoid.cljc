(ns prpr.cats.monoid
  (:require
   [clojure.set :as set]))

;; taken from https://github.com/ReadyForZero/babbage/blob/master/src/babbage/monoid.clj
;; and brought to cljc

(defprotocol Monoid
  (<> [self other] "'add' two monoidal values of the same type.")
  (mempty [self] "return the zero element for this type.")
  (mempty? [self] "Is this the zero element?")
  (value [self] "return the wrapped value"))


(extend-protocol Monoid
  ;; the accumulators library treated maps as collections of values:
  ;; (<> {:x 6} [:y 5]) --> {:x 6 :y 5}
  ;; we, instead, treat maps as collections of named monoids.
  #?(:clj clojure.lang.IPersistentMap
     :cljs cljs.core/PersistentHashMap)
  (<> [self other] (merge-with <> self other))
  (mempty [self] {})
  (mempty? [self] (every? mempty? (vals self)))
  (value [self] self)

  #?@(:cljs
      [cljs.core/PersistentHashMap
       (<> [self other] (merge-with <> self other))
       (mempty [self] {})
       (mempty? [self] (every? mempty? (vals self)))
       (value [self] self)

       cljs.core/PersistentArrayMap
       (<> [self other] (merge-with <> self other))
       (mempty [self] {})
       (mempty? [self] (every? mempty? (vals self)))
       (value [self] self)])

  #?(:clj clojure.lang.IPersistentList
     :cljs cljs.core/List)
  (<> [self other] (apply list (concat self other)))
  (mempty [self] '())
  (mempty? [self] (empty? self))
  (value [self] self)

  #?@(:cljs
      [cljs.core/EmptyList
       (<> [self other] (apply list (concat self other)))
       (mempty [self] '())
       (mempty? [self] (empty? self))
       (value [self] self)])

  #?(:clj clojure.lang.IPersistentSet
     :cljs cljs.core/PersistentHashSet)
  (<> [self other] (set/union self (set other)))
  (mempty [self] #{})
  (mempty? [self] (empty? self))
  (value [self] self)

  #?(:clj clojure.lang.PersistentVector
     :cljs cljs.core/PersistentVector)
  (<> [self other] (vec (concat self other)))
  (mempty [self] [])
  (mempty? [self] (empty? self))
  (value [self] self)

  #?(:clj clojure.lang.LazySeq
     :cljs cljs.core/LazySeq)
  (<> [self other] (concat self other))
  (mempty [self] '())
  (mempty? [self] (empty? self))
  (value [self] self)

  nil
  (<> [self other] other)
  (mempty [self] nil)
  (mempty? [self] true)
  (value [self] nil))

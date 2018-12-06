(ns prpr.cats.prws
  #?(:cljs (:require [cats.core :as monad]
                     [cats.protocols :as p]
                     [cats.context :as ctx :include-macros true]
                     [cats.data :as d]
                     [cats.util :as util]
                     [cats.monad.state :as state]
                     [prpr.promise :as prpr :include-macros true]
                     [prpr.cats.monoid :refer [<>]]
                     [prpr.cats.reader :as reader]
                     [prpr.cats.writer :as writer]
                     [schema.core :as s])
     :clj (:require [cats.core :as monad]
                    [cats.protocols :as p]
                    [cats.context :as ctx]
                    [cats.data :as d]
                    [cats.util :as util]
                    [cats.monad.state :as state]
                    [prpr.promise :as prpr]
                    [prpr.cats.monoid :refer [<>]]
                    [prpr.cats.reader :as reader]
                    [prpr.cats.writer :as writer]
                    [schema.core :as s])))

;; inspired by https://github.com/bwo/monads/blob/master/src/monads/rws.clj
;; with a little async sauce added

(declare context)

;; equality doesn't make much sense for
;; this type, which is really just to avoid
;; adding p/Contextual to IFn, so deftype is
;; more appropriate than defrecord
(deftype PRWS_T [mfn]
  p/Contextual
  (-get-context [_] context)

  #?@(:cljs [cljs.core/IFn
             (-invoke [self v]
                      (mfn v))]
      :clj  [clojure.lang.IFn
             (invoke [self v]
                     (mfn v))]))

(alter-meta! #'->PRWS_T assoc :private true)

(defn PRWS
  [f]
  (PRWS_T. f))

(defn PRWS?
  [v]
  (instance? PRWS_T v))

(s/defschema PRWSFnArgs
  {(s/optional-key ::state/state) s/Any
   (s/optional-key ::reader/env) s/Any})

(s/defschema PRWSResultValue
  {::monad/val s/Any
   (s/optional-key ::state/state) s/Any
   (s/optional-key ::writer/log) s/Any})

;; the PRWS values are functions of
;; {::state/state <state> ::reader/env <env>}
;; which return a
;; Deferred<{::monad/val <val> ::state/state <state> ::writer/log <log>}>
;; the values inside the Deferred should be equality comparable
;; with extractable fields, so defrecord works better than deftype
(defrecord PRWS_V_T [])
(alter-meta! #'->PRWS_V_T assoc :private true)
(alter-meta! #'map->PRWS_V_T assoc :private true)

(defn PRWS_V?
  [v]
  (instance? PRWS_V_T v))

(s/defn tag-result
  [{v ::monad/val
    s ::state/state
    l ::writer/log
    :as r} :- PRWSResultValue]
  (prpr/success-pr
   (map->PRWS_V_T r)))

(declare run-prws)

(defn lift-promise
  [p]
  (PRWS_T.
   (s/fn [{s ::state/state e ::reader/env :as t} :- PRWSFnArgs]
     (prpr/chain-pr
      p
      (fn [v]
        (cond
          (PRWS? v) (run-prws v t) ;; shouldn't happen ?
          (PRWS_V? v) (prpr/success-pr v) ;; also shouldn't happen
          ;; a naked value
          :else (tag-result
                 {::monad/val v
                  ::state/state s
                  ::writer/log nil})))))))

(defn lift-value
  [v]
  (PRWS_T.
   (s/fn [{s ::state/state e ::reader/env :as t} :- PRWSFnArgs]
     (tag-result
      {::monad/val v
       ::state/state s
       ::writer/log nil}))))

(defn lift
  "lift a value into the PRWS monad"
  [v]
  (cond
    (PRWS? v) v
    (prpr/promise? v) (lift-promise v)
    :else (lift-value v)))

(def ^{:no-doc true}
  context
  (reify
    p/Context

    p/Functor
    (-fmap [_ f fv]
      (PRWS
       (s/fn [{s ::state/state e ::reader/env :as t} :- PRWSFnArgs]
         (prpr/chain-pr
          (run-prws fv t)
          (s/fn [{a ::monad/val s' ::state/state w ::writer/log} :- PRWSResultValue]
            (tag-result {::monad/val (f a)
                         ::state/state s'
                         ::writer/log w}))))))

    p/Monad
    (-mreturn [_ v]
      (PRWS
       (s/fn [{s ::state/state e ::reader/env :as t} :- PRWSFnArgs]
         (tag-result {::monad/val v
                      ::state/state s
                      ::writer/log nil}))))

    (-mbind [_ mv f]
      (PRWS
       (s/fn [{s ::state/state e ::reader/env :as t} :- PRWSFnArgs]
         (prpr/chain-pr
          (prpr/catch
              (fn [err]
                (prpr/error-pr
                 ::prws-error
                 {::state/state s
                  ::reader/env e
                  ::err err
                  ::mv mv
                  ::f f}))
              (run-prws mv t))
          (s/fn [{a ::monad/val s' ::state/state w ::writer/log} :- PRWSResultValue]
            (prpr/chain-pr
             (prpr/catch
                 (fn [err]
                   (prpr/error-pr
                    ::prws-error
                    {::monad/val a
                     ::state/state s
                     ::state/state' s
                     ::reader/env e
                     ::writer/log w
                     ::err err
                     ::mv mv
                     ::f f}))
                 (run-prws (f a) {::state/state s' ::reader/env e}))
             (s/fn [{b ::monad/val s'' ::state/state w' ::writer/log} :- PRWSResultValue]
               (tag-result {::monad/val b
                            ::state/state s''
                            ::writer/log (<> w w')}))))))))

    state/MonadState
    (-get-state [_]
      (PRWS
       (s/fn [{s ::state/state e ::reader/env :as t} :- PRWSFnArgs]
         (tag-result {::monad/val s
                      ::state/state s
                      ::writer/log nil}))))

    (-put-state [_ newstate]
      (PRWS
       (s/fn [{s ::state/state e ::reader/env :as t} :- PRWSFnArgs]
         (tag-result {::monad/val s
                      ::state/state newstate
                      ::writer/log nil}))))

    (-swap-state [_ f]
      (PRWS
       (s/fn [{s ::state/state e ::reader/env :as t} :- PRWSFnArgs]
         (tag-result {::monad/val s
                      ::state/state (f s)
                      ::writer/log nil}))))

    reader/MonadReader
    (-ask [m]
      (PRWS
       (s/fn [{s ::state/state e ::reader/env :as t} :- PRWSFnArgs]
         (tag-result {::monad/val e
                      ::state/state s
                      ::writer/log nil}))))
    (-local [m f reader]
      (PRWS
       (s/fn [{s ::state/state e ::reader/env :as t} :- PRWSFnArgs]
         (run-prws reader {::state/state s ::reader/env (f e)}))))

    writer/MonadWriter
    (-listen [m mv]
      (PRWS
       (s/fn [{s ::state/state e ::reader/env :as t} :- PRWSFnArgs]
         (prpr/chain-pr
          (run-prws mv t)
          (fn [{a ::monad/val s' ::state/state w ::writer/log}]
            (tag-result {::monad/val [a w]
                         ::state/state s'
                         ::writer/log w}))))))
    (-tell [m v]
      (PRWS
       (s/fn [{s ::state/state e ::reader/env :as t} :- PRWSFnArgs]
         (tag-result {::monad/val nil
                      ::state/state s
                      ::writer/log v}))))
    (-pass [m mv]
      (PRWS
       (s/fn [{s ::state/state e ::reader/env :as t} :- PRWSFnArgs]
         (prpr/chain-pr
          (run-prws mv t)
          (s/fn [{[v f] ::monad/val s' ::state/state w ::writer/log} :- PRWSResultValue]
            (tag-result {::monad/val v
                         ::state/state s'
                         ::writer/log (f w)}))))))

    p/Printable
    (-repr [_])))

(s/defn run-prws
  "run a PRWS computation, returning
     Deferred<{::monad/val <val> ::state/state <state> ::writer/log <log>}"
  [mv :- PRWS_T
   {state ::state/state
    env ::reader/env
    :as t} :- PRWSFnArgs]
  (mv t))

(s/defn eval-prws
  "run a PRWS computation returning only the Deferred<<val>> ::monad/val value"
  [mv :- PRWS_T
   {state ::state/state
    env ::reader/env
    :as t} :- PRWSFnArgs]
  (prpr/chain-pr
   (mv t)
   (s/fn [{v ::monad/val s' ::state/state w ::writer/log} :- PRWSResultValue]
     (prpr/success-pr v))))

#?(:clj
   (defmacro prwsdo
     [& body]
     `(cats.context/with-context
        prpr.cats.prws/context
        (cats.core/mlet ~@body))))

(comment

  ;; should be able to write functions like this... note the macro sugar

  (defprws update-thing
    [thing-id updates]
    (prwsdo [;; get what we want from the app context
             {cassandra :cassandra/session
              kafka :kafka/producer} (reader/ask)

             ;; keep track of updated thing ids
             _ (state/swap #(update % ::updated-thing-ids (fnil conj [])))

             ;; get the thing record
             a-thing (cassandra/select-one
                      a-model.entities/Thing
                      [:id]
                      thing-id)]

            ;; update the thing record
            (cassandra/change
             a-model.entities/Thing
             a-thing
             (merge a-thing updates))))

  ;; which expands into this... automatic async fn call+arg tracing!

  (defn update-thing
    [thing-id updates]
    (prwsdo [;; log the call to the writer
             _ (writer/tell {:prpr.cats.prws/trace
                             [[:update-thing {:thing-id thing-id :updates updates}]]})

             ;; get what we want from the app context
             {cassandra :cassandra/session
              kafka :kafka/producer} (reader/ask)

             ;; keep track of updated thing ids
             _ (state/swap #(update % ::updated-thing-ids (fnil conj [])))

             ;; get the thing record
             a-thing (cassandra/select-one
                      a-model.entities/Thing
                      [:id]
                      thing-id)]

            ;; update the thing record
            (cassandra/change
             a-model.entities/Thing
             a-thing
             (merge a-thing updates))))

  ;; and gets called with something like this from e.g. an API method:

  (run-prws (update-thing #uuid "36a458c0-85e7-11e8-a4d4-49c83153c871" {:foo 10})
            {::state/state {}
             ::reader/env @(start!)})

  ;; which will return something like

  Deferred<
  {::monad/val [:upsert
                {:id #uuid "36a458c0-85e7-11e8-a4d4-49c83153c871"
                 :foo 10}]
   ::state/state {::updated-thing-ids [#uuid "36a458c0-85e7-11e8-a4d4-49c83153c871"]}
   ::writer/log {:prpr.cats.prws/trace [[:update-thing
                                         {:thing-id #uuid "36a458c0-85e7-11e8-a4d4-49c83153c871"
                                          :updates {:foo 10}}]
                                        [:cassandra/select-one
                                         ;; identifying the entity by key in the log
                                         ;; will require some additional work
                                         :a-model.entities/Thing
                                         [:id]
                                         #uuid "36a458c0-85e7-11e8-a4d4-49c83153c871"]
                                        [:cassandra/change
                                         :a-model.entities/Thing
                                         {:id #uuid "36a458c0-85e7-11e8-a4d4-49c83153c871"
                                          :foo 0}
                                         {:id #uuid "36a458c0-85e7-11e8-a4d4-49c83153c871"
                                          :foo 10}]]}}>
  )

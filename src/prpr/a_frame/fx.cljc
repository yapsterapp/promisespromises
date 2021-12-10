(ns prpr.a-frame.fx
  (:require
   [prpr.a-frame.schema :as schema]
   [prpr.promise :as prpr :refer [ddo return]]
   [prpr.a-frame.registry :as registry]
   [prpr.a-frame.router :as router]
   [schema.core :as s]
   [taoensso.timbre :refer [warn]]
   #?(:clj [manifold.deferred :as p]
      :cljs [promesa.core :as p])))

(defn reg-fx
  "register an fx which will be called with:
   (<fx-handler> app fx-data)"
  [id handler]
  (let [ctx-handler (fn [{app schema/a-frame-app-ctx
                         :as _context}
                        fx-data]
                      (handler app fx-data))]
    (registry/register-handler schema/a-frame-kind-fx id ctx-handler)))

(defn reg-fx-ctx
  "register an fx which will be called with:
  (<fx-handler> ctx fx-data)

  note that the fx-handler will receive the interceptor ctx,
  so can access values from the ctx (e.g. to dispatch to the event-stream),
  but it cannot modify the ctx"
  [id handler]
  (registry/register-handler schema/a-frame-kind-fx id handler))

(defn do-single-effect
  "return a promise of the result of the effect"
  [context fx-id fx-data]
  (let [handler (registry/get-handler schema/a-frame-kind-fx fx-id)]
    (if (some? handler)
      (ddo [r (handler context fx-data)]
        (return {fx-id r}))
      (throw
       (prpr/error-ex
        ::no-fx-handler
        {:id fx-id
         :data fx-data})))))

(defn do-map-of-effects
  [context effects]

  ;; do individual effects from the map concurrently
  (ddo [:let [result-ps (for [[id data] effects]
                          (do-single-effect context id data))]
        all-results (-> (apply prpr/all-pr result-ps)
                        (prpr/chain-pr
                         #(apply merge %)))]
    (return
     all-results)))

(defn do-seq-of-effects
  [context effects]

  ;; do the seq-of-maps-of-effects in strict sequential order
  (p/chain
   (p/loop [[results [first-map-fx & rest-map-fx]] [[] effects]]
     (p/chain
      (do-map-of-effects context first-map-fx)
      (fn [r]
        (if (empty? rest-map-fx)
          [(conj results r) []]
          (p/recur [(conj results r) rest-map-fx])))))
   (fn [[results _remaining]]
     results)))

;; an interceptor which will execute all effects from the
;; :a-frame/effects key of the interceptor context
(def do-fx
  {:id :do-fx
   :leave (fn do-fx-leave
            [{_app schema/a-frame-app-ctx
              effects schema/a-frame-effects
              :as context}]

            (ddo [_ (if (map? effects)
                      (do-map-of-effects context effects)
                      (do-seq-of-effects context effects))]
              context))})

(defn apply-transitive-coeffects?
  [default-transitive-coeffects?
   {transitive-coeffects? schema/a-frame-event-transitive-coeffects?
    :as _extended-event}]
  (if (some? transitive-coeffects?)
    transitive-coeffects?
    default-transitive-coeffects?))

(defn xev-with-all-coeffects
  "take coeffects from the context and merge
   with any specified on the event to give the
   final coeffects "
  [{ctx-coeffects schema/a-frame-coeffects
    :as _context}
   default-transitive-coeffects?
   event-or-extended-event]
  (let [{_ev schema/a-frame-event
         ev-coeffects schema/a-frame-coeffects
         :as extended-event} (router/coerce-extended-event
                              event-or-extended-event)
        transitive-coeffects? (apply-transitive-coeffects?
                               default-transitive-coeffects?
                               extended-event)]
    (if transitive-coeffects?
      (assoc
       extended-event
       schema/a-frame-coeffects
       (merge ctx-coeffects
              ev-coeffects))

      extended-event)))

;; standard fx

;; dispatch an event - coeffects are *not* transitive
;; by default
(reg-fx-ctx
 :a-frame/dispatch
 (s/fn [{router schema/a-frame-router
         :as context}
        event :- schema/EventOrExtendedEvent]
   (router/dispatch
    router
    (xev-with-all-coeffects context false event))))

;; dispatch a vector of events - coeffects are *not*
;; transitive by default
(reg-fx-ctx
 :a-frame/dispatch-n
 (s/fn [{router schema/a-frame-router
         :as context}
        events :- schema/EventsOrExtendedEvents]
   (router/dispatch-n
    router
    (map (partial xev-with-all-coeffects context false) events))))

;; dispatch an event and wait for it to be fully processed
;; before proceeding (pausing fx processing for the current
;; event, and any further processing on the main event stream)
;;
;; coeffects *are* transitive by default
(reg-fx-ctx
 :a-frame/dispatch-sync
 (s/fn [{router schema/a-frame-router
        :as context}
       event :- schema/EventOrExtendedEvent]
   (router/dispatch-sync
    router
    (xev-with-all-coeffects context true event))))

;; dispatch n events, and wait for them all to be fully processed
;; before proceeding (pausing fx processing for the current
;; event, and any further processing on the main event stream)
;;
;; coeffects *are* transitive by default
(reg-fx-ctx
 :a-frame/dispatch-n-sync
 (s/fn [{router schema/a-frame-router
        :as context}
       events :- schema/EventsOrExtendedEvents]
   (router/dispatch-n-sync
    router
    (map (partial xev-with-all-coeffects context true) events))))

(ns prpr.a-frame.schema
  (:require
   [schema.core :as s]
   [prpr.schema :as prpr.s]
   #?(:clj [prpr.stream :as stream])))

(def a-frame-state :a-frame/state)
(def a-frame-state-handlers-a :a-frame.state/handlers-a)
(def a-frame-router :a-frame/router)
(def a-frame-router-event-stream :a-frame.router/event-stream)
(def a-frame-router-executor :a-frame.router/executor)
(def a-frame-router-buffer-size :a-frame.router/buffer-size)
(def a-frame-router-global-interceptors :a-frame.router/global-interceptors)
(def a-frame-router-global-interceptors-a :a-frame.router/global-interceptors-a)

(def a-frame-app-ctx :a-frame/app-ctx)

(def a-frame-event :a-frame/event)
(def a-frame-events :a-frame/events)
(def a-frame-event-transitive-coeffects? :a-frame.event/transitive-coeffects?)

(def a-frame-event-modify-interceptor-chain
  :a-frame.event/modify-interceptor-chain)

(def a-frame-interceptor-init-ctx :a-frame.events.interceptor/init-ctx)

(def a-frame-effects :a-frame/effects)
(def a-frame-coeffects :a-frame/coeffects)
(def a-frame-coeffect-event :a-frame.coeffect/event)

(def a-frame-kind-fx :a-frame.kind/fx)
(def a-frame-kind-cofx :a-frame.kind/cofx)
(def a-frame-kind-event :a-frame.kind/event)

;; an event as dispatched by applications
(s/defschema Event
  [(s/one s/Keyword :event-kw)
   s/Any])

(s/defschema Events
  [Event])

;; these are initial coeffects which can be given to a dispatch
;; they don't yet have the Event associated with them
(s/defschema InitialCoeffects
  {s/Keyword s/Any})

;; an event along with any initial coeffects
(s/defschema ExtendedEvent
  {a-frame-event Event
   (s/optional-key a-frame-coeffects) InitialCoeffects
   (s/optional-key a-frame-event-transitive-coeffects?) s/Bool
   (s/optional-key a-frame-event-modify-interceptor-chain) (s/pred fn?)})

(s/defschema EventOrExtendedEvent
  (s/conditional
   vector? Event
   map? ExtendedEvent))

(s/defschema EventsOrExtendedEvents
  [EventOrExtendedEvent])

;; these are the Coeffects that a handler will see
(s/defschema Coeffects
  {a-frame-coeffect-event Event
   s/Keyword s/Any})

(defn derive-coeffects-schema
  "given some expected coeffects, derive
   a Coeffects schema from the base schema
   (always including the event, with a lax schema if no
    stricter schema is given)

   - strict? : true to prevent additional coeffect keys. false by default
   - event-schema : a more restrictive schema for the event
   - expected-coeffects-schema : a map schema for exepcted coeffects"
  ([strict? event-schema expected-coeffects-schema]
   (prpr.s/merge-map-schemas
    (if (some? event-schema)
     {a-frame-coeffect-event event-schema}
     {a-frame-coeffect-event Event})
    expected-coeffects-schema
    (when-not strict?
      {s/Keyword s/Any})))
  ([expected-coeffects-schema]
   (derive-coeffects-schema false nil expected-coeffects-schema))
  ([event-schema expected-coeffects-schema]
   (derive-coeffects-schema false event-schema expected-coeffects-schema)))

(s/defschema EffectsMap
  {s/Keyword s/Any})

(declare Effects)
(s/defschema EffectsVector
  [Effects])

(s/defschema Effects
  (s/conditional
   vector?
   EffectsVector

   map?
   EffectsMap

   nil?
   (s/eq nil)))

(s/defschema AppCtx
  {s/Keyword s/Any})

(s/defschema Interceptor
  {:id s/Keyword
   (s/optional-key :enter) (s/pred fn?)
   (s/optional-key :leave) (s/pred fn?)})

(s/defschema Router
  {a-frame-app-ctx AppCtx
   a-frame-router-event-stream #?(:clj (s/pred stream/stream?)
                                  :cljs s/Any)
   a-frame-router-global-interceptors-a s/Any
   (s/optional-key a-frame-router-executor) s/Any
   (s/optional-key a-frame-router-buffer-size) s/Int})

(s/defschema HandleEventInterceptorCtx
  {a-frame-router Router
   a-frame-app-ctx AppCtx
   a-frame-effects Effects
   a-frame-coeffects Coeffects
   s/Keyword s/Any})

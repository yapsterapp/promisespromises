(ns prpr.a-frame.schema
  (:require
   [schema.core :as s]
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

(def a-frame-interceptor-init-ctx :a-frame.events.interceptor/init-ctx)

(def a-frame-effects :a-frame/effects)
(def a-frame-coeffects :a-frame/coeffects)
(def a-frame-coeffect-event :a-frame.coeffect/event)

(def a-frame-kind-fx :a-frame.kind/fx)
(def a-frame-kind-cofx :a-frame.kind/cofx)
(def a-frame-kind-event :a-frame.kind/event)


(s/defschema Coeffects
  {s/Keyword s/Any})

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

;; an event as dispatched by applications
(s/defschema Event
  [(s/one s/Keyword :event-kw)
   s/Any])

(s/defschema Events
  [Event])

;; an event along with any initial coeffects
(s/defschema ExtendedEvent
  {a-frame-event Event
   (s/optional-key a-frame-coeffects) Coeffects
   (s/optional-key a-frame-event-transitive-coeffects?) s/Bool})

(s/defschema EventOrExtendedEvent
  (s/conditional
   vector? Event
   map? ExtendedEvent))

(s/defschema EventsOrExtendedEvents
  [EventOrExtendedEvent])

(s/defschema HandleEventInterceptorCtx
  {a-frame-router Router
   a-frame-app-ctx AppCtx
   a-frame-effects Effects
   a-frame-coeffects Coeffects
   s/Keyword s/Any})

(ns prpr.a-frame.events
  (:require
   [schema.core :as s]
   [prpr.promise :as prpr]
   [prpr.a-frame.schema :as schema]
   [prpr.a-frame.registry :as registry]
   [prpr.a-frame.std-interceptors :as std-interceptors]
   [prpr.a-frame.interceptor-chain :as interceptor-chain]
   [taoensso.timbre :refer [warn]]))

(defn flatten-and-remove-nils
  [_id interceptors]
  (->> interceptors
       flatten
       (remove nil?)))

(defn register
  "register an interceptor chain for an event - this should terminate
   in an interceptor which has a data-reference to the pure event-handler
   fn for the event"
  [id interceptors]
  (registry/register-handler
   schema/a-frame-kind-event
   id
   (flatten-and-remove-nils id interceptors)))

(defn register-pure
  "register a pure event-handler fn"
  [id pure-handler-fn]
  (registry/register-handler
   schema/a-frame-kind-event-pure
   id
   pure-handler-fn))

(defn reg-event-fx
  ([id handler]
   (reg-event-fx id nil handler))

  ([id
    interceptors
    handler]
   (register-pure id handler)
   (register
    id
    [::std-interceptors/unhandled-error-report
     :prpr.a-frame.fx/do-fx
     interceptors
     (std-interceptors/fx-handler->interceptor id)])))

(defn reg-event-ctx
  ([id handler]
   (reg-event-ctx id nil handler))

  ([id
    interceptors
    handler]
   (register-pure id handler)
   (register
    id
    [::std-interceptors/unhandled-error-report
     :prpr.a-frame.fx/do-fx
     interceptors
     (std-interceptors/ctx-handler->interceptor id)])))

(defn clear-event
  ([]
   (registry/unregister-handler
    schema/a-frame-kind-event-pure)
   (registry/unregister-handler
    schema/a-frame-kind-event))

  ([id]
   (registry/unregister-handler
    schema/a-frame-kind-event-pure
    id)
   (registry/unregister-handler
    schema/a-frame-kind-event
    id)))

(s/defn coerce-extended-event
  "Event|ExtendedEvent -> ExtendedEvent"
  [event-or-extended-event :- schema/EventOrExtendedEvent]
  (if (map? event-or-extended-event)
    event-or-extended-event
    {schema/a-frame-event event-or-extended-event}))

(defn handle
  [{app-ctx schema/a-frame-app-ctx
    a-frame-router schema/a-frame-router
    global-interceptors schema/a-frame-router-global-interceptors}

   event-or-extended-ev]

  (let [{[event-id & _event-args :as event-v] schema/a-frame-event
         init-coeffects schema/a-frame-coeffects
         modify-interceptor-chain schema/a-frame-event-modify-interceptor-chain
         :as _router-ev} (coerce-extended-event event-or-extended-ev)

        interceptors (registry/get-handler
                      schema/a-frame-kind-event
                      event-id)]

    (if (some? interceptors)
      (let [interceptors (into (vec global-interceptors) interceptors)

            interceptors (if (some? modify-interceptor-chain)
                           (modify-interceptor-chain interceptors)
                           interceptors)

            init-ctx (-> {schema/a-frame-coeffects init-coeffects
                          schema/a-frame-effects {}}

                         (assoc schema/a-frame-app-ctx app-ctx)

                         ;; add the event to any init-ctx coeffects ...
                         ;; so cofx handlers can access it
                         (assoc-in [schema/a-frame-coeffects
                                    schema/a-frame-coeffect-event]
                                   event-v))]

        (interceptor-chain/execute
         app-ctx
         a-frame-router
         interceptors
         init-ctx))

      (throw
       (prpr/error-ex
        ::no-event-handler
        {:event-v event-v})))))

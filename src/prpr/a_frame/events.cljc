(ns prpr.a-frame.events
  (:require
   [prpr.promise :as prpr]
   [prpr.a-frame.schema :as schema]
   [prpr.a-frame.registry :as registry]
   [prpr.interceptor-chain :as interceptor-chain]
   [taoensso.timbre :refer [warn]]))

(defn flatten-and-remove-nils
  [_id interceptors]
  (->> interceptors
       flatten
       (remove nil?)))

(defn register
  [id interceptors]
  (registry/register-handler
   schema/a-frame-kind-event
   id
   (flatten-and-remove-nils id interceptors)))


(defn handle
  ([app event-v]
   (handle
    {schema/a-frame-app-ctx app
     schema/a-frame-event event-v}))

  ([{app schema/a-frame-app-ctx
     init-ctx schema/a-frame-interceptor-init-ctx
     global-interceptors schema/a-frame-router-global-interceptors
     [event-id & _event-args :as event-v] schema/a-frame-event}]

   (let [interceptors (registry/get-handler
                       schema/a-frame-kind-event
                       event-id)]

     (if (some? interceptors)
       (let [interceptors (into (vec global-interceptors) interceptors)
             init-ctx (-> {schema/a-frame-effects {}}

                          (merge init-ctx)

                          ;; don't let init-ctx override the app-ctx
                          ;; or the event coeffect

                          (assoc schema/a-frame-app-ctx app)

                          ;; add the event to any init-ctx coeffects ...
                          ;; so cofx handlers can access it
                          (assoc-in [schema/a-frame-coeffects
                                     schema/a-frame-coeffect-event]
                                    event-v))]

         (interceptor-chain/execute interceptors init-ctx))

       (throw
        (prpr/error-ex
         ::no-event-handler
         {:event-v event-v}))))))

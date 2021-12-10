(ns prpr.a-frame
  (:require
   [prpr.a-frame.schema :as schema]
   [prpr.a-frame.events :as events]
   [prpr.a-frame.fx :as fx]
   [prpr.a-frame.cofx :as cofx]
   [prpr.a-frame.std-interceptors :refer [fx-handler->interceptor
                                          ctx-handler->interceptor]]
   [prpr.a-frame.registry :as registry]
   [prpr.a-frame.router :as router]
   [taoensso.timbre :refer [info]]))

;; like re-frame, but for backend stuff
;; provide an app-context map to most of the core fns, from which an
;; a-frame instance (with event-stream, handler registrations &c)
;; can be extracted at :prpr.a-frame/app
;; app-context will also be provided to event/fx/cofx handlers
;; so they can do complex stuff

(defn create-a-frame-router
  "deferst factory function for an a-frame app

  returns [a-frame-state close-fn]"
  ([app] (create-a-frame-router app {}))
  ([app opts]
   (let [router (router/create-router app opts)]

     (router/run-a-frame-router router)

     [router #(router/stop-a-frame-router router)])))

(defn dispatch
  "dispatch an event"
  [router event]
  (router/dispatch router event))

(defn dispatch-n
  [router events]
  (router/dispatch-n router events))

(defn dispatch-sync
  [router event]
  (router/dispatch-sync router event))

(defn dispatch-n-sync
  [router events]
  (router/dispatch-n-sync router events))

(defn reg-event-fx
  "register an event-handler expected to return a (promise of a) seq of fx

   (<handler> app cofx event) -> Promise<[{<fx-id> <fx-args>}]>

   the seq of fx will be processed sequentially - maps with multiple fx
   entries may be processed concurrently

   note that processing of the partition's queue will be suspended until the
   handler returns"
  ([id handler]
   (reg-event-fx id nil handler))
  ([id
    interceptors
    handler]
   (events/register
    id
    [fx/do-fx
     interceptors
     (fx-handler->interceptor handler)])))

(defn reg-event-ctx
  "register an event-handler expected to return a (promise of an) updated
   event-context

   (<handler> context) -> Promise<context>

   fx from the returned context will be processed as described in reg-event-fx"
  ([id handler]
   (reg-event-ctx id nil handler))
  ([id
    interceptors
    handler]
   (events/register
    id
    [fx/do-fx
     interceptors
     (ctx-handler->interceptor handler)])))

(defn clear-event
  ([]
   (registry/unregister-handler schema/a-frame-kind-event))
  ([id]
   (registry/unregister-handler schema/a-frame-kind-event id)))

(defn reg-fx
  "register an fx handler
   (<handler> app arg) -> Promise<*>"
  [id handler]
  (fx/reg-fx id handler))

(defn clear-fx
  ([]
   (registry/unregister-handler schema/a-frame-kind-fx))
  ([id]
   (registry/unregister-handler schema/a-frame-kind-fx id)))

(defn reg-cofx
  "register a cofx handler
   (<handler> app) -> Promise<*>
   (<handler> app arg) -> Promise<*>"
  [id handler]
  (cofx/reg-cofx id handler))

(defn inject-cofx
  ([id]
   (cofx/inject-cofx id))
  ([id value]
   (cofx/inject-cofx id value)))

(defn clear-cofx
  ([]
   (registry/unregister-handler schema/a-frame-kind-cofx))
  ([id]
   (registry/unregister-handler schema/a-frame-kind-cofx id)))

(defn reg-global-interceptor
  ([interceptor]
   (registry/register-handler
    schema/a-frame-kind-global-interceptor
    interceptor)))

(defn clear-global-interceptor
  ([]
   (registry/unregister-handler
    schema/a-frame-kind-global-interceptor))
  ([id]
   (registry/unregister-handler
    schema/a-frame-kind-global-interceptor
    id)))

(defn ->interceptor
  [{id :id
    before :before
    after :after}])

(defn get-coeffect
  ([context])
  ([context key])
  ([context key not-found]))

(defn assoc-coeffect
  [context key value])

(defn get-effect
  ([context])
  ([context key])
  ([context key not-found]))

(defn assoc-effect
  [context key value])

(defn enqueue
  [context interceptors])

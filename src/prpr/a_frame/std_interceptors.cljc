(ns prpr.a-frame.std-interceptors
  (:require
   [prpr.a-frame.schema :as schema]))

(defn fx-handler->interceptor
  [handler-fn]
  {:id :fx-handler
   :enter (fn fx-handler-fn
            [context]
            (let [{app-ctx schema/a-frame-app-ctx
                   {event schema/a-frame-coeffect-event
                    :as coeffects} schema/a-frame-coeffects} context
                  effects (handler-fn app-ctx coeffects event)]
              (assoc context schema/a-frame-effects effects)))})

(defn ctx-handler->interceptor
  [handler-fn]
  {:id :ctx-handler
   :enter handler-fn})

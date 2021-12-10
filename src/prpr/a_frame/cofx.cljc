(ns prpr.a-frame.cofx
  (:require
   [prpr.promise :as prpr :refer [ddo return]]
   [prpr.a-frame.schema :as schema]
   [prpr.a-frame.registry :as registry]
   [prpr.a-frame.cofx.resolve-arg :as resolve-arg]
   [taoensso.timbre :refer [info warn]]))

(defn reg-cofx
  [id handler]
  (registry/register-handler schema/a-frame-kind-cofx id handler))

;; interceptor

(defn inject-cofx
  ([id]
   {:id :coeffects-0-arg
    :enter (fn coeffects-0-arg-enter
             [{app schema/a-frame-app-ctx
               coeffects schema/a-frame-coeffects
               :as context}]
             (let [handler (registry/get-handler schema/a-frame-kind-cofx id)]
               (if (some? handler)
                 (ddo [coeffects' (handler app coeffects)]
                   (return
                    (assoc context schema/a-frame-coeffects coeffects')))

                 (throw (prpr/error-ex
                         ::no-cofx-handler
                         {:id id})))))})
  ([id arg-spec]
   {:id :coeffects
    :enter (fn coeffects-0-arg-enter
             [{app schema/a-frame-app-ctx
               coeffects schema/a-frame-coeffects
               :as context}]
             (let [handler (registry/get-handler schema/a-frame-kind-cofx id)
                   arg (resolve-arg/resolve-cofx-arg arg-spec coeffects)]
               (if (some? handler)
                 (ddo [coeffects' (handler app coeffects arg)]
                   (return
                    (assoc context schema/a-frame-coeffects coeffects')))

                 (throw (prpr/error-ex
                         ::no-cofx-handler
                         {:id id
                          :arg-spec arg-spec})))))}))

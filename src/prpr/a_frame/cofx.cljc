(ns prpr.a-frame.cofx
  (:require
   [prpr.promise :as prpr :refer [ddo return]]
   [prpr.a-frame.schema :as schema]
   [prpr.a-frame.registry :as registry]
   [prpr.a-frame.cofx.data.tag-readers #?@(:cljs [:include-macros true])]
   [prpr.a-frame.interceptor-chain :as interceptor-chain]
   [taoensso.timbre :refer [info warn]]))

(defn reg-cofx
  [id handler]
  (registry/register-handler schema/a-frame-kind-cofx id handler))

(defn inject-cofx
  "creates an InterceptorSpec data-structure for an event
   interceptor chain"
  ([id]
   {::interceptor-chain/key ::inject-cofx

    ::interceptor-chain/data
    {::interceptor-chain/enter-data {::id id}}})

  ([id arg-spec]
   {::interceptor-chain/key ::inject-cofx

    ::interceptor-chain/data
    {::interceptor-chain/enter-data {::id id
                                     ::arg arg-spec}}}))

;; interceptor

(def inject-cofx-interceptor
  "the interceptor functions to execute the interceptor
   described by an InterceptorSpec produced by inject-cofx"
  {::interceptor-chain/name ::inject-cofx

   ::interceptor-chain/enter
   (fn inject-cofx-enter
     [{app schema/a-frame-app-ctx
       coeffects schema/a-frame-coeffects
       :as context}

      {id ::id
       arg ::arg
       :as data}]

     (let [handler (registry/get-handler schema/a-frame-kind-cofx id)
           has-arg? (contains? data ::arg)]

       (if (some? handler)
         (ddo [coeffects' (if has-arg?
                            (handler app coeffects arg)
                            (handler app coeffects))]
              (return
               (assoc context schema/a-frame-coeffects coeffects')))

         (throw (prpr/error-ex
                 ::no-cofx-handler
                 {::id id
                  ::arg arg})))))})

(interceptor-chain/register-interceptor
 ::inject-cofx
 inject-cofx-interceptor)

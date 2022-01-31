(ns prpr.a-frame.std-interceptors
  (:require
   [prpr.a-frame.schema :as schema]
   [taoensso.timbre :refer [info warn]]))

(defn fx-handler->interceptor
  [handler-fn]
  {:id :fx-handler
   :enter (fn fx-handler-fn
            [context]
            (let [{{event schema/a-frame-coeffect-event
                    :as coeffects} schema/a-frame-coeffects} context
                  effects (handler-fn coeffects event)]
              (assoc context schema/a-frame-effects effects)))})

(defn ctx-handler->interceptor
  [handler-fn]
  {:id :ctx-handler
   :enter handler-fn})

(def extract-coeffects-interceptor
  "an interceptor to extract coeffects. must be at the head of the
   chain, because this breaks the interceptor-context"
  {:id :extract-coeffects
   :leave (fn [ctx]
            (info "extract-coeffects-interceptor")
            (get ctx schema/a-frame-coeffects))})

(defn modify-interceptors-for-coeffects
  "an interceptor modifier which
    - removes n (default 1) interceptors completely from the and of the chain
    - removes all :leave and :error fns from the remaining interceptors
    - inserts a new interceptor at the beginning of the chain which
        extracts coeffects from the context"
  [n interceptors]
  (info "modify-interceptors-for-coeffects - dropping:" n)
  (let [interceptors (vec interceptors)
        cnt (count interceptors)
        interceptors (subvec interceptors 0 (max (- cnt n) 0))
        interceptors (mapv #(dissoc % :leave :error)
                           interceptors)]

    (into
     [extract-coeffects-interceptor]
     interceptors)))

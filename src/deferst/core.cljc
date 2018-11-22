(ns deferst.core
  (:require
   #?(:clj [clojure.tools.namespace.repl :as tn])
   [clojure.string :as str]
   #?(:clj [manifold.deferred :as d]
      :cljs [promesa.core :as p])
   #?(:clj [cats.labs.manifold :as dm]
      :cljs [cats.labs.promise :as pm])
   [deferst.system :as s]))

(defprotocol ISys
  (start! [_] [_ conf])
  (stop! [_])
  (system-map [_]))

(defn return-pr
  [v]
  #?(:clj (d/success-deferred v)
     :cljs (p/promise v)))

;; cross-platform promise creation in the promesa style
#?(:clj (defn create-pr
          [resolve-reject-fn]
          (let [d (d/deferred)]
            (resolve-reject-fn
             #(d/success! d %)
             #(d/error! d %))
            d))
   :cljs (def create-pr p/promise))

;; cross-platform branch
#?(:clj (defn branch
          [p success-fn error-fn]
          (-> p
              (d/chain success-fn)
              (d/catch error-fn)))
   :cljs (def branch p/branch))

(defn start!*
  [builder sys-val conf label resolve-fn reject-fn]
  (#?(:clj send :cljs swap!)
   sys-val
   (fn [sys-pr]
     (let [sys-pr (if sys-pr
                    sys-pr
                    (do
                      (when label (println "starting: " label))
                      (s/start-system! builder conf)))]
       (-> sys-pr
           s/system-map
           (branch resolve-fn reject-fn))
       sys-pr))))

(defn stop!*
  [builder sys-val label resolve-fn reject-fn]
  (#?(:clj send :cljs swap!)
   sys-val
   (fn [sys-pr]
     (let [stop-pr (if sys-pr
                     (do
                       (when label (println "stopping: " label))
                       (s/system-map
                        (s/stop-system! sys-pr)))
                     (return-pr nil))]
       ;; returns nil on success or an error to the caller
       (branch stop-pr resolve-fn reject-fn)
       nil))))

(defn system-map*
  [sys-val resolve-fn reject-fn]
  (#?(:clj send :cljs swap!)
   sys-val
   (fn [sys-pr]
     (let [sm-pr (if sys-pr
                   (s/system-map sys-pr)
                   (return-pr nil))]
       (branch sm-pr resolve-fn reject-fn))
     sys-pr)))

(defrecord Sys [builder sys-val default-conf label]
  ISys
  (start! [this]
    (start! this default-conf))

  (start! [_ conf]
    (create-pr
     (fn [resolve-fn reject-fn]
       (start!* builder sys-val conf label resolve-fn reject-fn))))

  (stop! [_]
    (create-pr
     (fn [resolve-fn reject-fn]
       (stop!* builder sys-val label resolve-fn reject-fn))))

  (system-map [_]
    (create-pr
     (fn [resolve-fn reject-fn]
       (system-map* sys-val resolve-fn reject-fn)))))

(defn create-system
  ([builder]
   (create-system builder nil nil))
  ([builder default-conf]
   (create-system builder default-conf nil))
  ([builder default-conf label]
   (map->Sys {:builder builder
              :sys-val #?(:clj (agent nil)
                          :cljs (atom nil))
              :default-conf default-conf
              :label label})))
#?(:clj
   (defn- make-name
     [base-name suffix]
     (->> [base-name suffix]
          (filter identity)
          (str/join "-")
          symbol)))

(defn refresh-start
  "refresh and start on clj... error on cljs"
  [after-sym]
  #?(:clj (tn/refresh :after after-sym)
     :cljs (throw (js/Error. "not implemented on cljs"))))

#?(:clj
   (defn- defsystem*
     [base-name builder default-conf]
     (let [name (make-name base-name "sys")
           start-name (make-name base-name "start!")
           system-map-name (make-name base-name "system-map")
           stop-name (make-name base-name "stop!")
           reload-name (make-name base-name "reload!")]
       `(do
          (def ~name (deferst/create-system
                       ~builder
                       ~default-conf
                       (symbol (name (ns-name *ns*)) (name '~name))))
          (defn ~start-name
            ([] (deferst/start! ~name))
            ([conf#] (deferst/start! ~name conf#)))
          (defn ~system-map-name [] (deferst/system-map ~name))
          (defn ~stop-name [] (deferst/stop! ~name))
          (defn ~reload-name []
            (deferst/stop! ~name)
            (refresh-start
             (symbol (name (ns-name *ns*)) (name '~start-name))))))))

#?(:clj
   (defmacro defsystem
     "macro which defs some vars around a system and provides
   easy tools-namespace reloading. you get

   <base-name>-sys - the Sys record
   <base-name>-start! - start the system, with optional config
   <base-name>-stop! - stop the system if running
   <base-name>-reload! - stop!, tools.namespace refresh, start!"
     ([builder default-conf]
      (defsystem* nil builder default-conf))
     ([base-name builder default-conf]
      (defsystem* base-name builder default-conf))))

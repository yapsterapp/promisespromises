(ns deferst.core
  (:require
   #?(:clj [clojure.tools.namespace.repl :as tn])
   [clojure.string :as str]
   [prpr.promise :as pr #?@(:cljs [:include-macros true])]
   [deferst.system :as s]))

(defprotocol ISys
  (start! [_] [_ conf])
  (stop! [_])
  (system-map [_]))

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
           (pr/branch-pr resolve-fn reject-fn))
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
                     (pr/success-pr nil))]
       ;; returns nil on success or an error to the caller
       (pr/branch-pr stop-pr resolve-fn reject-fn)
       nil))))

(defn system-map*
  [sys-val resolve-fn reject-fn]
  (#?(:clj send :cljs swap!)
   sys-val
   (fn [sys-pr]
     (let [sm-pr (if sys-pr
                   (s/system-map sys-pr)
                   (pr/success-pr nil))]
       (pr/branch-pr sm-pr resolve-fn reject-fn))
     sys-pr)))

(defrecord Sys [builder sys-val default-conf label]
  ISys
  (start! [this]
    (start! this default-conf))

  (start! [_ conf]
    (pr/factory-pr
     (fn [resolve-fn reject-fn]
       (start!* builder sys-val conf label resolve-fn reject-fn))))

  (stop! [_]
    (pr/factory-pr
     (fn [resolve-fn reject-fn]
       (stop!* builder sys-val label resolve-fn reject-fn))))

  (system-map [_]
    (pr/factory-pr
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
          (def ~name (deferst.core/create-system
                       ~builder
                       ~default-conf
                       (symbol (name (ns-name *ns*)) (name '~name))))
          (defn ~start-name
            ([] (deferst.core/start! ~name))
            ([conf#] (deferst.core/start! ~name conf#)))
          (defn ~system-map-name [] (deferst.core/system-map ~name))
          (defn ~stop-name [] (deferst.core/stop! ~name))
          (defn ~reload-name []
            (deferst.core/stop! ~name)
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

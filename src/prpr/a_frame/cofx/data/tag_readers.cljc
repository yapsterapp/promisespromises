(ns prpr.a-frame.cofx.data.tag-readers
  (:require
   #?(:clj [prpr.util.macro :refer [if-cljs]])
   [prpr.a-frame.interceptor-chain.data.data-path
    :refer [->DataPath]]
   [prpr.a-frame.interceptor-chain.data.tag-readers]
   [prpr.a-frame.schema :as af.schema]))

;; see https://github.com/clojure/clojurescript-site/issues/371
;; 3! different versions of the tag-readers are required for:
;; 1. clj compiling cljs
;; 2. clj
;; 3. cljs self-hosted or runtime

#?(:clj
   (if-cljs
       (defn read-cofx-path
         [path]
         `(->DataPath (into
                       [af.schema/a-frame-coeffects]
                       ~path)))

     (defn read-cofx-path
       [path]
       ;; if we eval the path then we can use var symbols
       ;; in the path. this will only work on clj
       (->DataPath (into
                    [af.schema/a-frame-coeffects]
                    (eval path))))))

#?(:cljs
   (defn ^:export read-cofx-path
     [path]
     `(->DataPath (into
                   [af.schema/a-frame-coeffects]
                   ~path))))

#?(:clj
   (if-cljs
       (defn read-event-path
         "the event is always in the cofx at a known key"
         [path]
         `(->DataPath (into
                       [af.schema/a-frame-coeffects
                        af.schema/a-frame-coeffect-event]
                       ~path)))

     (defn read-event-path
       "the event is always in the cofx at a known key"
       [path]
       (->DataPath (into
                    [af.schema/a-frame-coeffects
                     af.schema/a-frame-coeffect-event]
                    (eval path))))))

#?(:cljs
   (defn ^:export read-event-path
     "the event is always in the cofx at a known key"
     [path]
     `(->DataPath (into
                   [af.schema/a-frame-coeffects
                    af.schema/a-frame-coeffect-event]
                   ~path))))

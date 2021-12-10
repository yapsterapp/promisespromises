(ns prpr.a-frame.cofx.resolve-arg.tag-readers
  (:require
   #?(:clj [prpr.util.macro :refer [if-cljs]])
   [prpr.a-frame.cofx.resolve-arg.protocols
    :refer [->CofxPath]]
   [prpr.a-frame.schema :as af.schema]))

;; see https://github.com/clojure/clojurescript-site/issues/371
;; 3! different versions of the tag-readers are required for:
;; 1. clj
;; 2. clj compiling cljs
;; 3. cljs self-hosted or runtime

#?(:clj
   (if-cljs
       (defn read-cofx-path
         [path]
         `(->CofxPath ~path))

       (defn read-cofx-path
         [path]
         (->CofxPath path))))

#?(:clj
   (if-cljs
       (defn read-event-path
         "the event is always in the cofx at a known key"
         [path]
         `(->CofxPath (into [af.schema/a-frame-coeffect-event] ~path)))

       (defn read-event-path
         "the event is always in the cofx at a known key"
         [path]
         (->CofxPath (into [af.schema/a-frame-coeffect-event] path)))))

#?(:cljs
   (defn ^:export read-cofx-path
     [path]
     `(->CofxPath ~path)))

#?(:cljs
   (defn ^:export read-event-path
     "the event is always in the cofx at a known key"
     [path]
     `(->CofxPath (into [af.schema/a-frame-coeffect-event] ~path))))

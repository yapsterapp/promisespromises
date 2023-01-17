(ns prpr.util.macro
  #?(:cljs (:require-macros [prpr.util.macro])))

;; from https://github.com/plumatic/schema/blob/master/src/clj/schema/macros.clj

#?(:clj
   (defn cljs-env?
     "Take the &env from a macro, and tell whether we are expanding into cljs."
     [env]
     (boolean (:ns env))))

#?(:clj
   (defmacro if-cljs
     "Return then if we are generating cljs code and else for Clojure code.
      https://groups.google.com/d/msg/clojurescript/iBY5HaQda4A/w1lAQi9_AwsJ"
     [then else]
     (if (cljs-env? &env) then else)))

#?(:clj
   (defmacro try-catch
     "A cross-platform variant of try-catch that catches all normal exceptions.
      Does not support finally, and does not take an exception class."
     [& body]
     (let [try-body (butlast body)
           [catch sym & catch-body :as _catch-form] (last body)]
       (assert (= catch 'catch))
       (assert (symbol? sym))
       `(if-cljs
         (try ~@try-body (~'catch :default ~sym ~@catch-body))
         (try ~@try-body (~'catch Exception ~sym ~@catch-body))))))

(defproject employeerepublic/promisespromises "_"
  :description "a lib for working with promises and streams"
  :url "https://github.com/employeerepublic/promisespromises"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}

  :plugins [[lein-modules-bpk/lein-modules "0.3.13.bpk-20160816.002513-1"]]

  :pedantic? :abort

  :exclusions [org.clojure/clojure
               org.clojure/tools.logging
               org.clojure/tools.reader]

  :dependencies [[org.clojure/clojure "_"]
                 [org.clojure/clojurescript "_"]
                 [org.clojure/tools.reader "_"]
                 [org.clojure/tools.namespace "0.2.11"]
                 [org.clojure/tools.macro "0.1.5"]

                 ;; wow, such logging
                 [org.clojure/tools.logging "_"]
                 [com.taoensso/timbre "4.10.0"]
                 ;; timbre 5.1.2 is causing cljs compile
                 ;; errors, so sticking with 4.1.0
                 ;; [com.taoensso/timbre "5.1.2"
                 ;;  :exclusions [com.taoensso/encore]]
                 ;; [com.taoensso/encore "3.21.0"]
                 [org.slf4j/slf4j-api "1.7.25"]
                 [employeerepublic/slf4j-timbre "0.5.1"]
                 [org.slf4j/jcl-over-slf4j "1.7.25"]
                 [org.slf4j/log4j-over-slf4j "1.7.25"]
                 [org.slf4j/jul-to-slf4j "1.7.25"]


                 [funcool/cats "2.3.6"]
                 [prismatic/schema "1.1.9"]
                 [funcool/promesa "5.1.0"]
                 [manifold "0.1.8"]
                 [potemkin "0.4.5"
                  :exclusions [riddley]]
                 [org.clojure/math.combinatorics "0.1.4"]
                 [frankiesardo/linked "1.3.0"]

                 [danlentz/clj-uuid "0.1.7"]
                 [com.lucasbradstreet/cljs-uuid-utils "1.0.2"]]

  :profiles {:repl {:pedantic? :ranges}
             :test {:resource-paths ["test-resources" "resources"]}})

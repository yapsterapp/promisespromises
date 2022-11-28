(defproject yapsterapp/promisespromises "3.0.0-nustreams-SNAPSHOT"
  :description "a lib for working with promises and streams"
  :url "https://github.com/yapsterapp/promisespromises"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}

  :plugins []

  :pedantic? :abort

  :exclusions [org.clojure/clojure
               org.clojure/tools.logging
               org.clojure/tools.reader]

  :dependencies [[io.aviso/pretty "1.1.1"]

                 ;; clojure 1.11.0 is causing issues
                 ;; with io.aviso.pretty which is used
                 ;; by tools.logging and timbre for
                 ;; printing exception traces
                 [org.clojure/clojure "1.10.3"]

                 [org.clojure/clojurescript "1.11.4"]
                 [org.clojure/tools.reader "1.3.6"]
                 [org.clojure/tools.namespace "1.2.0"]
                 [org.clojure/tools.macro "0.1.5"]
                 [org.clojure/core.async "1.5.648"]


                 ;; wow, such logging
                 [org.clojure/tools.logging "1.2.4"]
                 [com.taoensso/timbre "4.10.0"
                  :exclusions [io.aviso/pretty]]
                 ;; timbre 5.1.2 is causing cljs compile
                 ;; errors, so sticking with 4.10.0
                 ;; [com.taoensso/timbre "5.1.2"
                 ;;  :exclusions [com.taoensso/encore]]
                 ;; [com.taoensso/encore "3.21.0"]
                 [org.slf4j/slf4j-api "1.7.36"]
                 [employeerepublic/slf4j-timbre "0.5.1"]
                 [org.slf4j/jcl-over-slf4j "1.7.36"]
                 [org.slf4j/log4j-over-slf4j "1.7.36"]
                 [org.slf4j/jul-to-slf4j "1.7.36"]


                 [funcool/cats "2.4.2"]
                 [prismatic/schema "1.2.0"]
                 [funcool/promesa "8.0.450"]
                 [manifold "0.2.3"]
                 [potemkin "0.4.5"
                  :exclusions [riddley]]
                 [org.clojure/math.combinatorics "0.1.6"]
                 [frankiesardo/linked "1.3.0"]

                 [danlentz/clj-uuid "0.1.9"]
                 [com.lucasbradstreet/cljs-uuid-utils "1.0.2"]]

  :profiles {:repl {:pedantic? :ranges
                    :dependencies [[org.clojure/test.check "1.1.1"]]}
             :test {:resource-paths ["test-resources" "resources"]
                    :dependencies [[org.clojure/test.check "1.1.1"]]}})

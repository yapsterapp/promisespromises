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

  :dependencies [;; clojure 1.11.0 is causing issues
                 ;; with io.aviso.pretty which is used
                 ;; by tools.logging and timbre for
                 ;; printing exception traces
                 [org.clojure/clojure "1.11.1"]

                 [org.clojure/clojurescript "1.11.60"]
                 [org.clojure/tools.reader "1.3.6"]
                 [org.clojure/tools.namespace "1.3.0"]
                 [org.clojure/tools.macro "0.1.5"]
                 [org.clojure/core.async "1.6.673"]


                 ;; wow, such logging
                 [org.clojure/tools.logging "1.2.4"]
                 [com.taoensso/timbre "6.0.4"]
                 [org.slf4j/slf4j-api "2.0.6"]
                 ;; [employeerepublic/slf4j-timbre "0.5.1"]
                 [org.slf4j/jcl-over-slf4j "2.0.6"]
                 [org.slf4j/log4j-over-slf4j "2.0.6"]
                 [org.slf4j/jul-to-slf4j "2.0.6"]

                 ;; TODO - maybe drop?
                 [funcool/cats "2.4.2"]

                 [metosin/malli "0.9.2"]
                 [funcool/promesa "10.0.571"]
                 [manifold "0.3.0"]
                 [org.clojure/math.combinatorics "0.1.6"]
                 [frankiesardo/linked "1.3.0"]

                 [danlentz/clj-uuid "0.1.9"]
                 [com.lucasbradstreet/cljs-uuid-utils "1.0.2"]]

  :profiles {:repl {:pedantic? :ranges
                    :dependencies [[org.clojure/test.check "1.1.1"]]}
             :test {:resource-paths ["test-resources" "resources"]
                    :dependencies [[org.clojure/test.check "1.1.1"]]}})

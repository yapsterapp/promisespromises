(defproject employeerepublic/promisespromises "0.6"
  :description "a lib for working with promises and streams"
  :url "https://github.com/employeerepublic/promisespromises"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}

  :plugins [[lein-cljsbuild "1.1.7"
             :exclusions [org.clojure/clojure org.clojure/clojurescript]]
            [cider/cider-nrepl "0.18.0"]]

  :pedantic? :abort

  :exclusions [org.clojure/clojure
               org.clojure/tools.logging
               org.clojure/tools.reader]

  :dependencies [[org.clojure/clojure "1.9.0"]
                 [org.clojure/clojurescript "1.10.339"]
                 [org.clojure/tools.reader "1.3.0"]

                 ;; wow, such logging
                 [org.clojure/tools.logging "0.4.1"]
                 [com.taoensso/timbre "4.10.0"]
                 [org.slf4j/slf4j-api "1.7.25"]
                 [employeerepublic/slf4j-timbre "0.5.0"]
                 [org.slf4j/jcl-over-slf4j "1.7.25"]
                 [org.slf4j/log4j-over-slf4j "1.7.25"]
                 [org.slf4j/jul-to-slf4j "1.7.25"]


                 [mccraigmccraig/cats "2.2.0.101"]
                 [prismatic/schema "1.1.9"]
                 [funcool/promesa "1.9.0"]
                 [manifold "0.1.8"]
                 [potemkin "0.4.5"
                  :exclusions [riddley]]
                 [org.clojure/math.combinatorics "0.1.4"]
                 [frankiesardo/linked "1.3.0"]]

  :profiles {:repl {:pedantic? :ranges
                    :dependencies [[org.clojure/tools.nrepl "0.2.13"]]}

             :test {:resource-paths ["test-resources" "resources"]}}

  ;; :hooks [leiningen.cljsbuild]

  :cljsbuild
  {:builds {:test
            {:source-paths ["src" "test"]
             :resource-paths ["test-resources" "resources"]
             :compiler {:output-to "phantom/unit-test-js/unit-test.js"
                        :output-dir "phantom/unit-test-js"
                        :source-map "phantom/unit-test-js/unit-test.js.map"
                        :optimizations :whitespace
                        :pretty-print true
                        :closure-output-charset "US-ASCII"}}}

   :test-commands
   {"unit" ["phantomjs"
            "phantom/unit-test.js"
            "phantom/unit-test.html"]}}

  :clean-targets ^{:protect false} ["phantom/unit-test-js"
                                    :target-path])

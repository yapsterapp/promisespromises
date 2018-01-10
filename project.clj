(defproject employeerepublic/promisespromises "_"
  :description "a lib for working with promises and streams"
  :url "https://github.com/employeerepublic/promisespromises"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}

  :plugins [[lein-modules-bpk/lein-modules "0.3.13.bpk-20160816.002513-1"]
            [lein-cljsbuild "1.1.7"
             :exclusions [org.clojure/clojure org.clojure/clojurescript]]]

  :pedantic? :abort

  :exclusions [org.clojure/clojure
               org.clojure/tools.logging
               org.clojure/tools.reader]

  :dependencies [[org.clojure/clojure "_"]
                 [org.clojure/clojurescript "_"]
                 [org.clojure/tools.reader "_"]

                 ;; wow, such logging
                 [org.clojure/tools.logging "0.3.1"]
                 [com.taoensso/timbre "4.10.0"]
                 [org.slf4j/slf4j-api "1.7.25"]
                 [employeerepublic/slf4j-timbre "0.4.3"]
                 [org.slf4j/jcl-over-slf4j "1.7.25"]
                 [org.slf4j/log4j-over-slf4j "1.7.25"]
                 [org.slf4j/jul-to-slf4j "1.7.25"]


                 [funcool/cats "2.1.0"]
                 [funcool/promesa "1.9.0"]
                 [manifold "0.1.7-alpha6"]
                 [potemkin "0.4.3"
                  :exclusions [riddley]]
                 [org.clojure/math.combinatorics "0.1.4"]
                 [frankiesardo/linked "1.2.9"]]

  :profiles {:repl {:pedantic? :ranges}}

  :hooks [leiningen.cljsbuild]

  :cljsbuild
  {:builds {:test
            {:source-paths ["src" "test"]
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

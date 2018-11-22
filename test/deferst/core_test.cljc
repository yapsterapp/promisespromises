(ns deferst.core-test
  (:require
   [prpr.test :refer [deftest test-async is testing use-fixtures]]
   [prpr.promise :as pr :refer [ddo]]
   [cats.core :refer [return]]
   [clojure.set :as set]
   [schema.test]
   [deferst.system :as s]
   [deferst.core :as d]))

(deftest simple-sys
  (test-async
   (let [sb (s/system-builder [[:a identity {:a-arg [:foo]}]])
         sys (d/create-system sb {:foo 10})]

     (testing "simple system starts"
       (ddo [sysmap (d/start! sys)
             :let [x-sysmap {:foo 10 :a {:a-arg 10}}]]
         (return
          (is (= sysmap x-sysmap)))))

     (testing "simple system returns system map"
       (ddo [sysmap (d/system-map sys)
             :let [x-sysmap {:foo 10 :a {:a-arg 10}}]]
         (return
          (is (= sysmap x-sysmap)))))

     (testing "start! returns same system map if already started"
       (ddo [sysmap (d/start! sys {:foo 20})
             :let [x-sysmap {:foo 10 :a {:a-arg 10}}]]
         (return
          (is (= sysmap x-sysmap)))))

     (testing "simple system stops and returns a promise of the config"
       (ddo [stop-sys (d/stop! sys)
             :let [x-stopmap {:foo 10}]]
         (return
          (is (= stop-sys x-stopmap)))))

     (testing "start! returns new system map when restarted"
       (ddo [sysmap (d/start! sys {:foo 20})
             :let [x-sysmap {:foo 20 :a {:a-arg 20}}]]
         (return
          (is (= sysmap x-sysmap))))))))

;; --- Entry Point

#?(:cljs (enable-console-print!))
#?(:cljs (set! *main-cli-fn* #(t/run-tests
                               'deferst.core-test
                               'deferst.system-test)))
#?(:cljs
   (defmethod t/report [:cljs.test/default :end-run-tests]
     [m]
     (if (t/successful? m)
       (set! (.-exitCode js/process) 0)
       (set! (.-exitCode js/process) 1))))

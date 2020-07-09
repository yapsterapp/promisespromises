(ns prpr.promise-ddefn-test
  (:require
   [clojure.test :refer [deftest is are testing use-fixtures]]
   [prpr.promise :as pr]
   [schema.core :as s]
   [schema.utils :as s.u]
   [schema.test :refer [validate-schemas]])
  (:import
   [schema.utils ValidationError]))

(use-fixtures :once validate-schemas)

(defn double-if-int
  [x]
  (if (int? x)
    (* x 2)
    x))

(pr/ddefn ^:always-validate promise-test-fn
  :- s/Int
  [x]
  (if (= :dont-defer x)
    x
    (pr/always-pr
     (double-if-int x))))

(deftest with-promised-output-validate-test
  (testing "valid promise return value"
    (is (= 4 @(promise-test-fn 2))))
  (testing "invalid promise return value"
    (let [e (try
              @(promise-test-fn "2")
              (catch Exception e
                e))
          ve (:error (ex-data e))]
      (is (= {:type :schema.core/error
              :schema s/Int
              :value "2"}
             (dissoc
              (ex-data e)
              :error)))
      (is (instance? ValidationError ve))
      (is (= '(not (integer? "2"))
             (s.u/validation-error-explain ve)))))
  (testing "non-promise fn return value"
    (let [e (try
              (promise-test-fn :dont-defer)
              (catch Exception e
                e))
          ve (:error (ex-data e))]
      (is (= {:type :schema.core/error
              :value :dont-defer}
             (dissoc
              (ex-data e)
              :schema
              :error)))
      (is (instance? ValidationError ve))
      (is (= '(not (promise? :dont-defer))
             (s.u/validation-error-explain ve))))))

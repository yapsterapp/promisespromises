(ns prpr.promise-ddefn-test
  (:require [cljs.test :refer-macros [deftest async is testing use-fixtures]]
            [prpr.promise :as pr]
            [promesa.core :as p]
            [schema.core :as s]
            [schema.utils :as s.u]))

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
    (let [pr (promise-test-fn 2)]
      (async
       done
       (p/then
        pr
        (fn [v]
          (is (= 4 v))
          (done))))))
  (testing "invalid promise return value"
    (let [pr (promise-test-fn "2")]
      (async
       done
       (p/catch
        pr
        (fn [e]
          (let [ve (:error (ex-data e))]
            (is (= {:type :schema.core/error
                    :schema s/Int
                    :value "2"}
                   (dissoc
                    (ex-data e)
                    :error)))
            (is (instance? s.u/ValidationError ve))
            (is (= '(not (integer? "2"))
                   (s.u/validation-error-explain ve))))
          (done))))))
  (testing "non-promise fn return value"
    (try
      (let [pr (promise-test-fn :dont-defer)]
        (try
          (async
           done
           (p/then pr (fn [v] (done))))))
      (catch js/Error e
        (let [ve (:error (ex-data e))]
          (is (= {:type :schema.core/error
                  :value :dont-defer}
                 (dissoc
                  (ex-data e)
                  :schema
                  :error)))
          (is (instance? s.u/ValidationError ve))
          (is (= '(not (promise? :dont-defer))
                 (s.u/validation-error-explain ve))))))))

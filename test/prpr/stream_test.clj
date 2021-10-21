(ns prpr.stream-test
  (:require
   [clojure.test :as t :refer [deftest testing is are use-fixtures]]
   [prpr.util.test :refer [with-log-level]]
   [manifold.deferred :as d]
   [manifold.stream :as s]
   [prpr.stream :as sut]
   [taoensso.timbre :refer [debug info warn]])
  (:import
   [prpr.stream StreamError]))

(deftest map-test
  (testing "single stream"
    (is (= [2 3 4 5]
           @(->> [1 2 3 4]
                 (sut/->source)
                 (sut/map inc)
                 (sut/reduce conj [])))))
  (testing "multiple streams"
    (is (= [[1 :foo] [2 :bar] [3 :baz]]
           @(->> (sut/map vector
                          (sut/->source [1 2 3 4])
                          (sut/->source [:foo :bar :baz]))
                 (sut/reduce conj []))))))

(deftest s-first-test
  (testing "empty stream"
    (let [s (s/stream)
          _ (s/close! s)]
      (is (= ::none @(sut/s-first ::none s)))))
  (testing "empty stream default no-val"
    (let [s (s/stream)
          _ (s/close! s)]
      (is (= ::sut/none @(sut/s-first s)))))
  (testing "first value"
    (let [s (s/stream 3)
          _ @(s/put-all! s [:foo :bar :baz])
          _ (s/close! s)]
      (is (= :foo @(sut/s-first s))))))

(deftest divert-stream-errors-test
  (testing "empty source"
    (let [s (s/stream)
          _ (s/close! s)
          [err o] (sut/divert-stream-errors s)]
      (is (= [] (s/stream->seq o)))
      (is (= [] (s/stream->seq err)))))

  (testing "no errors source"
    (let [s (s/stream 3)
          _ @(s/put-all! s [:foo :bar :baz])
          _ (s/close! s)
          [err o] (sut/divert-stream-errors s)]
      (is (= [:foo :bar :baz] (s/stream->seq o)))
      (is (= [] (s/stream->seq err)))))

  (testing "all errors source"
    (let [s (s/stream 3)
          _ @(s/put-all! s [(StreamError.
                             (ex-info "foo" {:foo :foo}))
                            (StreamError.
                             (ex-info "bar" {:bar :bar}))
                            (StreamError.
                             (ex-info "baz" {:baz :baz}))])
          _ (s/close! s)
          [err o] (sut/divert-stream-errors s)]
      ;; have to convert err first or it will block
      (is (= [{:foo :foo}
              {:bar :bar}
              {:baz :baz}]
             (map (comp ex-data :error) (s/stream->seq err))))
      (is (= [] (s/stream->seq o)))))
  (testing "mixed source"
    (let [s (s/stream 3)
          _ @(s/put-all! s [:foo
                            (StreamError.
                             (ex-info "bar" {:bar :bar}))
                            :baz])
          _ (s/close! s)
          [err o] (sut/divert-stream-errors s)]
      ;; have to take in the right order or will block
      (is (= :foo @(s/take! o)))
      (is (= {:bar :bar}
             (-> @(s/take! err)
                 :error
                 ex-data)))
      (is (= :baz @(s/take! o)))
      (is (= ::closed @(s/take! o ::closed)))
      (is (= ::closed @(s/take! err ::closed))))))

(deftest reduce-all-throw-test
  (with-log-level :error
    (testing "empty source"
      (let [s (s/stream)
            _ (s/close! s)
            r @(sut/reduce-all-throw "empty-source" conj [] s)]
        (is (= [] r))))

    (testing "no errors source"
      (let [s (s/stream 3)
            _ @(s/put-all! s [:foo :bar :baz])
            _ (s/close! s)
            r @(sut/reduce-all-throw "no-errors-source" conj [] s)]
        (is (= [:foo :bar :baz] r))))

    (testing "all errors source"
      (let [s (s/stream 3)
            _ @(s/put-all! s [(StreamError.
                               (ex-info "foo" {:foo :foo}))
                              (StreamError.
                               (ex-info "bar" {:bar :bar}))
                              (StreamError.
                               (ex-info "baz" {:baz :baz}))])
            _ (s/close! s)
            r (sut/reduce-all-throw
               "all-errors-source"
               conj
               []
               s)
            ed (ex-data (d/error-value r ::oops))]
        (is (= {:foo :foo} ed))))

    (testing "mixed source"
      (let [s (s/stream 3)
            _ @(s/put-all! s [:foo
                              (StreamError.
                               (ex-info "bar" {:bar :bar}))
                              :baz])
            _ (s/close! s)
            r (sut/reduce-all-throw
               "mixed-source"
               conj
               []
               s)
            ed (ex-data (d/error-value r ::oops))]
        (is (= {:bar :bar} ed))))

    (testing "no init"
      (let [s (s/stream 3)
            _ @(s/put-all! s [1 2 3])
            _ (s/close! s)
            r @(sut/reduce-all-throw
                "no-init"
                +
                s)]
        (is (= 6 r))))

    (testing "no init error first"
      (let [s (s/stream 3)
            _ @(s/put-all! s [(StreamError.
                               (ex-info "bar" {:bar :bar}))
                              2
                              3])
            _ (s/close! s)
            r (sut/reduce-all-throw
               "no-init-error-first"
               +
               s)
            ed (ex-data (d/error-value r ::oops))]
        (is (= {:bar :bar} ed))))

    (testing "mixed no init"
      (let [s (s/stream 3)
            _ @(s/put-all! s [1
                              (StreamError.
                               (ex-info "bar" {:bar :bar}))
                              3])
            _ (s/close! s)
            r (sut/reduce-all-throw
               "mixed-no-init"
               +
               s)
            ed (ex-data (d/error-value r ::oops))]
        (is (= {:bar :bar} ed))))

    (testing "throw gets caught"
      (let [s (s/stream 1)
            ev (d/chain (d/success-deferred :foo)
                        (fn [v]
                          (throw (ex-info "boo" {:foo ::foo}))))
            _ @(s/put! s ev)
            _ (s/close! s)
            r (sut/reduce-all-throw
               "throw"
               conj
               []
               s)
            ed (ex-data (d/error-value r ::oops))]
        (is (= {:foo ::foo} ed))))

    (testing "captures errors during reduction"
      (let [processed (atom [])
            s (s/stream 8)
            _ @(s/put-all! s [0 1 2 3 4 5 6 7])
            _ (s/close! s)
            r (sut/reduce-all-throw
               "exeception-in-reduction-fn"
               (fn [rs i]
                 (swap! processed conj i)
                 (case i
                   3 (throw (ex-info (str ::odd-number) {:n i}))
                   5 (throw (ex-info (str ::odd-number) {:n i}))
                   #_else (conj rs i)))
               []
               s)
            rs (d/success-value r ::not-successful)
            ed (ex-data (d/error-value r ::no-error))]
        (is (= (range 0 8) @processed))
        (is (= ::not-successful rs))
        (is (= {:n 3} ed))))))

(deftest realize-each-captures-errors-test
  (let [xs (->> [(d/future 0)
                 (sut/->StreamError (ex-info (str ::odd-number) {:n 1}))
                 2
                 (d/success-deferred 3)
                 4
                 (d/error-deferred (ex-info (str ::odd-number) {:n 5}))
                 6
                 (d/future (/ 1 0))]
                (s/->source)
                (sut/realize-each)
                ;; NOTE: using vanilla stream reduce to confirm presence of errors
                (s/reduce
                 conj
                 [])
                deref)]
    (is (= [0 2 3 4 6] (remove sut/stream-error? xs)))
    (is (= 3 (count (filter sut/stream-error? xs))))
    (is (= 1 (->> xs
                  (filter sut/stream-error?)
                  (filter
                   (fn [se]
                     (instance? ArithmeticException (.error se))))
                  (count))))
    (is (= [1 5]
           (->> xs
                (filter sut/stream-error?)
                (map (comp ex-data #(.error %)))
                (remove nil?)
                (map :n))))))

(deftest map-captures-errors-test
  (let [xs (->> [0 1 2 3 4 5 6 7]
                (s/->source)
                (sut/map
                 (fn [i]
                   (case i
                     1 (sut/->StreamError (ex-info (str ::odd-number) {:n i}))
                     3 (throw (ex-info (str ::odd-number) {:n i}))
                     4 (d/success-deferred 4)
                     5 (d/error-deferred (ex-info (str ::odd-number) {:n i}))
                     7 (d/future (throw (ex-info (str ::odd-number) {:n i})))
                     #_else i)))
                ;; NOTE: using vanilla stream reduce to confirm presence of errors
                (s/reduce
                 conj
                 [])
                deref)]
    (is (= [0 2 4 6] (remove sut/stream-error? xs)))
    (is (= 4 (count (filter sut/stream-error? xs))))
    (is (= [1 3 5 7]
           (->> xs
                (filter sut/stream-error?)
                (map (comp ex-data #(.error %)))
                (remove nil?)
                (map :n))))))

(deftest map-skips-existing-streamerrors-test
  (let [processed (atom [])
        xs (->> [0
                 (sut/->StreamError (ex-info (str ::odd-number) {:n 1}))
                 2]
                (s/->source)
                (sut/map
                 (fn [i]
                   (swap! processed conj i)
                   (* i 2)))
                ;; NOTE: using vanilla stream reduce to confirm presence of error
                (s/reduce
                 conj
                 [])
                deref)]
    (is (= [0 2] @processed))
    (is (= [0 4] (remove sut/stream-error? xs)))
    (is (= 1 (count (filter sut/stream-error? xs))))
    (is (= [1]
           (->> xs
                (filter sut/stream-error?)
                (map (comp ex-data #(.error %)))
                (remove nil?)
                (map :n))))))

(deftest mapcat-captures-errors-test
  (let [xs (->> [0 1 2 3 4 5]
                (s/->source)
                (sut/mapcat
                 (fn [i]
                   (case i
                     1 [(sut/->StreamError (ex-info (str ::odd-number) {:n i}))]
                     3 (throw (ex-info (str ::odd-number) {:n i}))
                     5 [i (* i nil)] ;; generate a NullPointerException
                     #_else [i (* i 10)])))
                ;; NOTE: using vanilla stream reduce to confirm presence of errors
                (s/reduce
                 conj
                 [])
                deref)]
    (is (= [0 0 2 20 4 40] (remove sut/stream-error? xs)))
    (is (= 3 (count (filter sut/stream-error? xs))))
    (is (= 1 (->> xs
                  (filter sut/stream-error?)
                  (filter
                   (fn [se]
                     (instance? NullPointerException (.error se))))
                  (count))))
    (is (= [1 3]
           (->> xs
                (filter sut/stream-error?)
                (map (comp ex-data #(.error %)))
                (remove nil?)
                (map :n))))))

(deftest mapcat-skips-existing-streamerrors-test
  (let [processed (atom [])
        xs (->> [0
                 (sut/->StreamError (ex-info (str ::odd-number) {:n 1}))
                 2]
                (s/->source)
                (sut/mapcat
                 (fn [i]
                   (swap! processed conj i)
                   [i (* i 10)]))
                ;; NOTE: using vanilla stream reduce to confirm presence of error
                (s/reduce
                 conj
                 [])
                deref)]
    (is (= [0 2] @processed))
    (is (= [0 0 2 20] (remove sut/stream-error? xs)))
    (is (= 1 (count (filter sut/stream-error? xs))))
    (is (= [1]
           (->> xs
                (filter sut/stream-error?)
                (map (comp ex-data #(.error %)))
                (remove nil?)
                (map :n))))))

(deftest filter-captures-errors-test
  (let [xs (->> [0 1 2 3 4 5]
                (s/->source)
                (sut/filter
                 (fn [i]
                   (case i
                     0 false
                     1 (sut/->StreamError (ex-info (str ::odd-number) {:n i}))
                     3 (throw (ex-info (str ::odd-number) {:n i}))
                     5 (> i nil) ;; generate a NullPointerException
                     #_else i)))
                ;; NOTE: using vanilla stream reduce to confirm presence of errors
                (s/reduce
                 conj
                 [])
                deref)]
    (is (= [2 4] (remove sut/stream-error? xs)))
    (is (= 3 (count (filter sut/stream-error? xs))))
    (is (= 1 (->> xs
                  (filter sut/stream-error?)
                  (filter
                   (fn [se]
                     (instance? NullPointerException (.error se))))
                  (count))))
    (is (= [1 3]
           (->> xs
                (filter sut/stream-error?)
                (map (comp ex-data #(.error %)))
                (remove nil?)
                (map :n))))))

(deftest transform-captures-errors-test
  (let [xs (->> [0 1 2 3 4 5]
                (s/->source)
                (sut/transform
                 (map
                  (fn [i]
                    (case i
                      1 (sut/->StreamError (ex-info (str ::odd-number) {:n i}))
                      3 (throw (ex-info (str ::odd-number) {:n i}))
                      5 (> i nil) ;; generate a NullPointerException
                      #_else i))))
                ;; NOTE: using vanilla stream reduce to confirm presence of errors
                (s/reduce
                 conj
                 [])
                deref)]
    (is (= [0 2 4] (remove sut/stream-error? xs)))
    (is (= 3 (count (filter sut/stream-error? xs))))
    (is (= 1 (->> xs
                  (filter sut/stream-error?)
                  (filter
                   (fn [se]
                     (instance? NullPointerException (.error se))))
                  (count))))
    (is (= [1 3]
           (->> xs
                (filter sut/stream-error?)
                (map (comp ex-data #(.error %)))
                (remove nil?)
                (map :n))))))

(deftest transform-skips-existing-streamerrors-test
  (let [processed (atom [])
        xs (->> [0
                 (sut/->StreamError (ex-info (str ::odd-number) {:n 1}))
                 2]
                (s/->source)
                (sut/transform
                 (map
                  (fn [i]
                    (swap! processed conj i)
                    (* i 2))))
                ;; NOTE: using vanilla stream reduce to confirm presence of error
                (s/reduce
                 conj
                 [])
                deref)]
    (is (= [0 2] @processed))
    (is (= [0 4] (remove sut/stream-error? xs)))
    (is (= 1 (count (filter sut/stream-error? xs))))
    (is (= [1]
           (->> xs
                (filter sut/stream-error?)
                (map (comp ex-data #(.error %)))
                (remove nil?)
                (map :n))))))

(deftest realize-stream-reduces-all-and-throws-first-error-test
  (with-log-level :error
    (let [processed (atom [])
          s (s/stream 8)
          _ @(s/put-all! s [0 1 2 3 4 5 6 7])
          _ (s/close! s)
          r (->> s
                 (sut/map
                  (fn [i]
                    (swap! processed conj i)
                    (case i
                      3 (throw (ex-info (str ::odd-number) {:n i}))
                      5 (throw (ex-info (str ::odd-number) {:n i}))
                      #_else i)))
                 (sut/realize-stream))
          rs (d/success-value r ::not-successful)
          ed (ex-data (d/error-value r ::no-error))]
      (is (= (range 0 8) @processed))
      (is (= ::not-successful rs))
      (is (= {:n 3} ed)))))

(deftest test-realize-stream-reduces-deferred-stream-and-throws-first-error-test
  (with-log-level :error
    (let [processed (atom [])
          s (s/stream 8)
          _ @(s/put-all! s [0 1 2 3 4 5 6 7])
          _ (s/close! s)
          r (->> s
                 (sut/map
                  (fn [i]
                    (swap! processed conj i)
                    (case i
                      3 (throw (ex-info (str ::odd-number) {:n i}))
                      5 (throw (ex-info (str ::odd-number) {:n i}))
                      #_else i)))
                 (d/success-deferred)
                 (sut/test-realize-stream))
          rs (d/success-value r ::not-successful)
          ed (ex-data (d/error-value r ::no-error))]
      (is (= (range 0 8) @processed))
      (is (= ::not-successful rs))
      (is (= {:n 3} ed)))))

(deftest map-serially*-test
  (testing "behaves like map"
    (let [r (->> [0 1 2 3 4 5]
                 (sut/map-serially*
                  ::map
                  (fn [v]
                    (d/future
                      (Thread/sleep 20)
                      (inc v))))
                 (sut/reduce conj [])
                 deref)]
      (is (= r [1 2 3 4 5 6]))))
  (testing "behaves like map with plain valued map fns"
    (let [r (->> [0 1 2 3 4 5]
                 (sut/map-serially* ::map inc)
                 (sut/reduce conj [])
                 deref)]
      (is (= r [1 2 3 4 5 6]))))
  (testing "catches errors"
    (let [r (->> [0 1 2 3 4 5]
                 (sut/map-serially*
                  ::map
                  (fn [v]
                    (d/future
                      (Thread/sleep 20)
                      (if (#{2 5} v)
                        (throw (ex-info "boo" {:v v}))
                        (inc v)))))
                 ;; reduce without error propagation
                 (s/reduce conj [])
                 deref)]
      (is (= (filter number? r)
             [1 2 4 5]))
      (is (= (->> r
                  (filter sut/stream-error?)
                  (map (comp :v ex-data :error)))
             [2 5]))))
  (testing "catches errors with plain-valued map fns"
    (let [r (->> [0 1 2 3 4 5]
                 (sut/map-serially*
                  ::map
                  (fn [v]
                    (if (#{2 5} v)
                      (throw (ex-info "boo" {:v v}))
                      (inc v))))
                 ;; reduce without error propagation
                 (s/reduce conj [])
                 deref)]
      (is (= (filter number? r)
             [1 2 4 5]))
      (is (= (->> r
                  (filter sut/stream-error?)
                  (map (comp :v ex-data :error)))
             [2 5]))))
  (testing "applies strictly serially"
    (let [active-a (atom #{})
          active-history-a (atom [])
          r (->> [0 1 2 3 4 5]
                 (sut/map-serially*
                  ::map
                  (fn [v]
                    (d/future
                      (swap! active-a conj v)
                      (swap! active-history-a conj @active-a)
                      (Thread/sleep 20)
                      (swap! active-a disj v)
                      (swap! active-history-a conj @active-a)
                      (inc v))))
                 (s/reduce conj [])
                 deref)]
      (is (= r [1 2 3 4 5 6]))
      (is (= #{} @active-a))
      (is (= 12 (count @active-history-a)))
      (is (= [#{0} #{} #{1} #{} #{2} #{} #{3} #{} #{4} #{} #{5} #{}]
             @active-history-a)))))

(deftest map-concurrency-two-test
  (testing "limits concurrency to 2"
    (let [active-a (atom #{})
          active-history-a (atom [])
          r (->> [0 1 2 3 4 5 6 7 8 9]
                 (sut/map-concurrency-two
                  (fn [v]
                    (d/future
                      (swap! active-a conj v)
                      (swap! active-history-a conj @active-a)
                      (Thread/sleep 20)
                      (swap! active-a disj v)
                      (swap! active-history-a conj @active-a)
                      (inc v))))
                 (s/reduce conj [])
                 deref)]
      (is (= r [1 2 3 4 5 6 7 8 9 10]))
      (is (= #{} @active-a))
      (is (= 20 (count @active-history-a)))
      (is (= 2 (->> @active-history-a
                    (map count)
                    (reduce max)))))))

(deftest buffer-concurrency-test
  (testing "limits concurrency to 3"
    (let [active-a (atom #{})
          active-history-a (atom [])
          r (->> [0 1 2 3 4 5 6 7 8 9]
                 (sut/map
                  (fn [v]
                    (d/future
                      (swap! active-a conj v)
                      (swap! active-history-a conj @active-a)
                      (Thread/sleep 20)
                      (swap! active-a disj v)
                      (swap! active-history-a conj @active-a)
                      (inc v))))
                 (sut/buffer-concurrency 3)
                 (s/reduce conj [])
                 deref)]
      (is (= r [1 2 3 4 5 6 7 8 9 10]))
      (is (= #{} @active-a))
      (is (= 20 (count @active-history-a)))
      (is (= 3 (->> @active-history-a
                    (map count)
                    (reduce max))))))
  (testing "limits concurrency to 5"
    (let [active-a (atom #{})
          active-history-a (atom [])
          r (->> [0 1 2 3 4 5 6 7 8 9 10 11 12 13 14]
                 (sut/map
                  (fn [v]
                    (d/future
                      (swap! active-a conj v)
                      (swap! active-history-a conj @active-a)
                      (Thread/sleep 20)
                      (swap! active-a disj v)
                      (swap! active-history-a conj @active-a)
                      (inc v))))
                 (sut/buffer-concurrency 5)
                 (s/reduce conj [])
                 deref)]
      (is (= r [1 2 3 4 5 6 7 8 9 10 11 12 13 14 15]))
      (is (= #{} @active-a))
      (is (= 30 (count @active-history-a)))
      (is (= 5 (->> @active-history-a
                    (map count)
                    (reduce max)))))))

(deftest map-concurrently-test
  (testing "limits concurrency to 1"
    (let [active-a (atom #{})
          active-history-a (atom [])
          r (->> [0 1 2 3 4 5]
                 (sut/map-concurrently
                  1
                  (fn [v]
                    (d/future
                      (swap! active-a conj v)
                      (swap! active-history-a conj @active-a)
                      (Thread/sleep 20)
                      (swap! active-a disj v)
                      (swap! active-history-a conj @active-a)
                      (inc v))))
                 (s/reduce conj [])
                 deref)]
      (is (= r [1 2 3 4 5 6]))
      (is (= #{} @active-a))
      (is (= 12 (count @active-history-a)))
      (is (= [#{0} #{} #{1} #{} #{2} #{} #{3} #{} #{4} #{} #{5} #{}]
             @active-history-a))))
  (testing "limits concurrency to 2"
    (let [active-a (atom #{})
          active-history-a (atom [])
          r (->> [0 1 2 3 4 5 6 7 8 9]
                 (sut/map-concurrently
                  2
                  (fn [v]
                    (d/future
                      (swap! active-a conj v)
                      (swap! active-history-a conj @active-a)
                      (Thread/sleep 20)
                      (swap! active-a disj v)
                      (swap! active-history-a conj @active-a)
                      (inc v))))
                 (s/reduce conj [])
                 deref)]
      (is (= r [1 2 3 4 5 6 7 8 9 10]))
      (is (= #{} @active-a))
      (is (= 20 (count @active-history-a)))
      (is (= 2 (->> @active-history-a
                    (map count)
                    (reduce max))))))
  (testing "limits concurrency to 3"
    (let [active-a (atom #{})
          active-history-a (atom [])
          r (->> [0 1 2 3 4 5 6 7 8 9]
                 (sut/map-concurrently
                  3
                  (fn [v]
                    (d/future
                      (swap! active-a conj v)
                      (swap! active-history-a conj @active-a)
                      (Thread/sleep 20)
                      (swap! active-a disj v)
                      (swap! active-history-a conj @active-a)
                      (inc v))))
                 (s/reduce conj [])
                 deref)]
      (is (= r [1 2 3 4 5 6 7 8 9 10]))
      (is (= #{} @active-a))
      (is (= 20 (count @active-history-a)))
      (is (= 3 (->> @active-history-a
                    (map count)
                    (reduce max))))))
  (testing "limits concurrency to 5"
    (let [active-a (atom #{})
          active-history-a (atom [])
          r (->> [0 1 2 3 4 5 6 7 8 9 10 11 12 13 14]
                 (sut/map-concurrently
                  5
                  (fn [v]
                    (d/future
                      (swap! active-a conj v)
                      (swap! active-history-a conj @active-a)
                      (Thread/sleep 20)
                      (swap! active-a disj v)
                      (swap! active-history-a conj @active-a)
                      (inc v))))
                 (s/reduce conj [])
                 deref)]
      (is (= r [1 2 3 4 5 6 7 8 9 10 11 12 13 14 15]))
      (is (= #{} @active-a))
      (is (= 30 (count @active-history-a)))
      (is (= 5 (->> @active-history-a
                    (map count)
                    (reduce max))))))
  (testing "timeout option"
    (let [result-stream (->> (sut/->source (range 4))
                             (sut/map-concurrently {:timeout-ms 50}
                                               2
                                               (fn [_] (d/future (Thread/sleep 100))))
                             (s/stream->seq))]
      (is (= 4 (count result-stream)))
      (is (= #{StreamError}
             (into #{} (map type result-stream)))))))

(deftest take-n-test
  (testing "takes from the stream"
    (is (= [1 2 3]
           (s/stream->seq
            (sut/take-n 3 (s/->source [1 2 3 4 5 6]))))))

  (testing "downstream closes when upstream is closed"
    (let [in-s (s/stream)
          out-s (sut/take-n 3 in-s)
          _ @(s/put! in-s 1)
          _ (s/close! in-s)

          out (s/stream->seq out-s)]
      (is (= [1] out)))))

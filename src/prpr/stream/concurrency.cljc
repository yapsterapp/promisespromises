(ns prpr.stream.concurrency
  (:require
   [promesa.core :as pr])
  #?(:clj
     (:import
      [java.util.concurrent ExecutorService ConcurrentLinkedQueue]
      [java.util LinkedList])))

(defprotocol IConcState
  (get-signal [_])
  (set-signal! [_ s])

  (queue-take! [_])
  (queue-put! [_ v])

  (get-active [_])
  (inc-active! [_] [_ n])
  (dec-active! [_] [_ n]))

(deftype ConcState [^:volatile-mutable signal
                    ^:volatile-mutable queue
                    ^:volatile-mutable active]
  IConcState
  (get-signal [_] signal)
  (set-signal! [_ s] (set! signal s))

  (queue-take! [_] (.poll queue))
  (queue-put! [_ v] (.add queue v))


  (get-active [_] active)
  (inc-active! [_] (set! active (inc active)))
  (inc-active! [_ n] (set! active (+ active n)))
  (dec-active! [_] (set! active (dec active)))
  (dec-active! [_ n] (set! active (- active n))))

(defn conc-state
  []
  (->ConcState (pr/deferred) (LinkedList.) 0))

(defn process-queue
  "gets a signal every time work is added or completes..."
  [n lock state #?@(:clj [^ExecutorService executor]
                    :cljs [executor])]
  (pr/loop []
    (pr/then
     (get-signal state)
     (fn [continue?]
       (let [work (locking lock
                    (let [active (get-active state)

                          available-slots (- n active)

                          work (reduce
                                (fn [w n]
                                  (if-let [f (queue-take! state)]
                                    (conj w f)
                                    (reduced w)))
                                []
                                (range 0 available-slots))]

                      (inc-active! state (count work))
                      (set-signal! state (pr/deferred))

                      work))]

         (when (not-empty work)
           (doseq [f work]

             #?(:clj (if (some? executor)
                       (.execute executor f)
                       (f))

                :cljs (f))))

         (when continue?
           (pr/recur))))
     executor)))

(defn concurrency-limited-fn
  "returns a fn which will call an (optionally) promise-returning
   function, f, in a concurrency limited fashion

   the returned fn will return a promise immediately when called,
   but behind the scenes f will be called with concurrency
   controlled, such that there will be at most n unresolved results
   of f at any one time"
  ([f n]
   (concurrency-limited-fn f n nil))

  ([f n #?@(:clj [^ExecutorService executor] :cljs [_executor])]
   (let [lock #?(:clj (Object.) :cljs (js/Object.))
         state (conc-state)]

     (process-queue n lock state executor)

     [(fn kill []
        (locking lock
          (pr/resolve!
           (get-signal state)
           false)))

      (fn limit [& args]

        (let [r-p (pr/deferred)

              f0 (fn []
                   (pr/handle

                    (try
                      (apply f args)
                      (catch #?(:clj Exception :cljs :default) e
                        (pr/rejected e)))

                    (fn [v err]
                      (try
                        (if (some? err)
                          (pr/reject! r-p err)
                          (pr/resolve! r-p v))
                        (finally
                          (locking lock
                            (dec-active! state)
                            (pr/resolve!
                             (get-signal state)
                             true))))

                      true)))]

          (locking lock
            (queue-put! state f0)
            (pr/resolve!
             (get-signal state)
             true))

          r-p))])))

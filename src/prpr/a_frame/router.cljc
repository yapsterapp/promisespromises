(ns prpr.a-frame.router
  (:require
   #?(:clj [manifold.deferred :as d])
   #?(:clj [prpr.stream :as stream])
   [prpr.promise :as prpr :refer [ddo]]
   [prpr.a-frame.schema :as schema]
   [prpr.a-frame.events :as events]
   [schema.core :as s]
   [taoensso.timbre :refer [debug info warn error]]))

;; use a record so we can
;; override the print-method to hide the app-context
;; for more readable error messages
(defrecord AFrameRouter [])

#?(:clj
   (defmethod print-method AFrameRouter [x writer]
     (print-method
      (into
       {}
       (assoc x schema/a-frame-app-ctx "<app-ctx-hidden>"))
      writer)))

(s/defn create-router :- schema/Router
  [app
   {global-interceptors schema/a-frame-router-global-interceptors
    executor schema/a-frame-router-executor
    buffer-size schema/a-frame-router-buffer-size
    :or {buffer-size 100}
    :as opts}]
  (let [opts (dissoc opts schema/a-frame-router-global-interceptors)]
    #?(:clj
       (merge
        (->AFrameRouter)
        opts
        {schema/a-frame-router-global-interceptors-a (atom (vec global-interceptors))
         schema/a-frame-app-ctx app
         schema/a-frame-router-event-stream (stream/stream buffer-size nil executor)})

       :cljs
       (throw (ex-info "not implemented" {:app app
                                          :global-interceptors global-interceptors
                                          :executor executor
                                          :buffer-size buffer-size
                                          :opts opts})))))

(defn -replace-global-interceptor
  [global-interceptors
   {interceptor-id :id
    :as interceptor}]
  (reduce
    (fn [ret existing-interceptor]
      (if (= interceptor-id
             (:id existing-interceptor))
        (do
          (debug "a-frame: replacing duplicate global interceptor id: " (:id interceptor))
          (conj ret interceptor))
        (conj ret existing-interceptor)))
    []
    global-interceptors))

(defn reg-global-interceptor
  [{global-interceptors-a schema/a-frame-router-global-interceptors-a
    :as _router}
   {interceptor-id :id
    :as interceptor}]
  (swap!
   global-interceptors-a
   (fn [global-interceptors]
     (let [ids (map :id global-interceptors)]
       (if (some #{interceptor-id} ids)
         ;; If the id already exists we replace it in-place to maintain the ordering of
         ;; global interceptors esp during hot-code reloading in development.
         (-replace-global-interceptor global-interceptors interceptor)
         (conj global-interceptors interceptor))))))

(defn clear-global-interceptors
  ([{global-interceptors-a schema/a-frame-router-global-interceptors-a
     :as _router}]
   (reset! global-interceptors-a []))

  ([{global-interceptors-a schema/a-frame-router-global-interceptors-a
     :as _router}
    id]
   (swap!
    global-interceptors-a
    (fn [global-interceptors]
      (into [] (remove #(= id (:id %)) global-interceptors))))))

(s/defn coerce-extended-event
     "Event|ExtendedEvent -> ExtendedEvent"
     [event-or-extended-event :- schema/EventOrExtendedEvent]
     (if (map? event-or-extended-event)
       event-or-extended-event
       {schema/a-frame-event event-or-extended-event}))

(s/defn dispatch
  "dispatch an Event or ExtendedEvent"
  [{event-s schema/a-frame-router-event-stream
    :as _router} :- schema/Router
   event-or-extended-event :- schema/EventOrExtendedEvent]

  #?(:clj
     (stream/put! event-s (coerce-extended-event event-or-extended-event))
     :cljs
     (throw (ex-info "not implemented" {:event-s event-s
                                        :event-or-stream-event event-or-extended-event}))))

(s/defn dispatch-n
  "dispatch a seq of Events or ExtendedEvents in a backpressure sensitive way"
  [router :- schema/Router
   events-or-extended-events :- schema/EventsOrExtendedEvents]
  #?(:clj
     (d/loop [[evoce & rest-evoces] events-or-extended-events]
       (d/chain
        (dispatch router evoce)
        (fn [_]
          (if (not-empty rest-evoces)
            (d/recur rest-evoces)
            true))))

     :cljs
     (throw (ex-info "not implemented" {:router router
                                        :events-or-extended-events events-or-extended-events}))))

(s/defn handle-event
  [{app schema/a-frame-app-ctx
    global-interceptors-a schema/a-frame-router-global-interceptors-a
    :as router} :- schema/Router
   catch? :- s/Bool
   extended-ev :- schema/ExtendedEvent]

  (let [{event schema/a-frame-event
         init-coeffects schema/a-frame-coeffects
         :as _router-ev} extended-ev

        init-ctx {schema/a-frame-router router
                  schema/a-frame-coeffects init-coeffects}

        handle-arg {schema/a-frame-app-ctx app
                    schema/a-frame-interceptor-init-ctx init-ctx
                    schema/a-frame-router-global-interceptors @global-interceptors-a
                    schema/a-frame-event event}]
    (if catch?
      (prpr/catchall
       (events/handle handle-arg)
       (fn [err]
         (warn err "handle-event")
         err))

      (events/handle handle-arg))))

(s/defn handle-event-stream
  "handle a regular, infinite, event-stream"
  [{event-s schema/a-frame-router-event-stream
    :as router} :- schema/Router]
  #?(:clj
     (->> event-s
          (stream/map
           (partial handle-event router true))
          (stream/realize-each)
          (stream/count-all-throw
           ::handle-event-stream))
     :cljs
     (throw (ex-info "not implemented" {:event-s event-s
                                        :router router}))))

(s/defn handle-sync-event-stream
  "handle events off of the stream until the stream is empty,
   at which point return the interceptor context of the
   very first event off of the stream"
  [{tmp-event-s schema/a-frame-router-event-stream
    :as tmp-router} :- schema/Router]
  #?(:clj
     (let [rv-a (atom nil)]
       (d/loop []
         (d/chain
          ;; since handle-event parks for events to be fully handled,
          ;; we know that, if the stream is empty, then
          ;; there were no further dispatches and we are done
          (stream/try-take! tmp-event-s ::default 0 ::timeout)

          (fn [router-ev]

            (if-not (#{::default ::timeout} router-ev)

              (d/chain
               (handle-event tmp-router false router-ev)
               (fn [r]
                 (swap!
                  rv-a
                  (fn [[_rv :as rv-wrapper] nv]
                    (if (nil? rv-wrapper)
                      [nv]
                      rv-wrapper))
                  r)
                 (d/recur)))

              ;; tmp-event-s is empty - close and return
              (do
                (stream/close! tmp-event-s)
                (let [[rv] @rv-a]
                  rv)))))))

     :cljs
     (throw (ex-info "not implemented" {:tmp-event-s tmp-event-s
                                        :tmp-router tmp-router}))))

(s/defn dispatch-sync
  "puts the event-v on to a temporary stream,
   handles events from the stream and return
   when the stream is empty.

   returns Promise<interceptor-context> from the handling of
   the event-v, so dispatch-sync can be called to handle
   an event locally, and then extract a result from the
   interceptor context

   errors at any point during the handling of the event-v or
   any dispatches resulting from it will propagate back to
   the caller - if the caller was itself an event then the
   handling of that event will fail"
  [{app schema/a-frame-app-ctx
    :as router} :- schema/Router
   event-or-extended-event :- schema/EventOrExtendedEvent]

  #?(:clj
     ;; create a temp event-stream, with same buffer-size
     ;; and executor as the original
     (ddo [:let [{tmp-event-s schema/a-frame-router-event-stream
                  :as tmp-router} (create-router app (dissoc router schema/a-frame-router))]

           _ (stream/put!
              tmp-event-s
              (coerce-extended-event event-or-extended-event))]

       (handle-sync-event-stream tmp-router))

     :cljs
     (throw (ex-info "not implemented" {:app app
                                        :router router
                                        :event-or-extended-event event-or-extended-event}))))

(s/defn dispatch-n-sync
  "puts events onto a temporary stream, handles events from
   the stream, and returns when the stream is empty"
  [{app schema/a-frame-app-ctx
     :as router} :- schema/Router

   events-or-extended-events :- schema/EventsOrExtendedEvents]

  #?(:clj

     (let [extended-events (map coerce-extended-event events-or-extended-events)]
       ;; create a temp event-stream, with same buffer-size
       ;; and executor as the original
       (ddo [:let [{tmp-event-s schema/a-frame-router-event-stream
                    :as tmp-router} (create-router app (dissoc router schema/a-frame-router))]

             _ (stream/put-all! tmp-event-s extended-events)]

         (handle-sync-event-stream tmp-router)))

     :cljs
     (throw (ex-info "not implemented" {:app app
                                        :router router
                                        :events-or-extended-events events-or-extended-events}))))

(s/defn run-a-frame-router
  [router :- schema/Router]
  (handle-event-stream
   router))

(s/defn stop-a-frame-router
  [{event-s schema/a-frame-router-event-stream
    :as _router} :- schema/Router]
  (info "closing a-frame")
  #?(:clj
     (stream/close! event-s)

     :cljs
     (throw (ex-info "not implemented" {:event-s event-s}))))

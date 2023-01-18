# promisespromises

TODO - much documentation expansion

1. prpr.streams : A streams API, exposing a Clojure and ClojureScript
compatible Manifold-like promises + streams abstraction
2. prpr.a-frame : A port of the [re-frame](https://github.com/day8/re-frame)
event and effect handling machinery to the async domain, offering a 
straightforward separation of pure and effectful code for both 
Clojure and ClojureScript

## prpr.streams

Implements a [Manifold-like](https://github.com/yapsterapp/promisespromises/blob/trunk/src/prpr/stream.cljc)
asynchronous streams API. The fundamental backpressure-sensitive
Promise + Stream model is the same as Manifold, and the API follows the 
[Manifold streams API](https://github.com/clj-commons/manifold) closely

``` clojure
(require '[prpr.stream :as s])
(def s (s/stream))
(s/put-all-and-close! s [0 1 2 3 4])
(def r (->> s (s/map inc) (s/reduce ::add +)))
;; => 15
```

It does a few things which vanilla Manifold doesn't:

* **error propagation** - errors occuring during stream operations
(`map`, `reduce`, `transform` &c) propagate downstream
* **chunking** - streams can be (mostly) transparently chunked for
improved performance (through fewer callbacks)
* **uniform clj and cljs API** - you can use the same streams API everywhere!
* **stream-joins** - `prpr.stream/cross` joins streams sorted in a key - it
has various styles of join - inner, outer, left-n, merge, intersect, union -
and can be used for a sensible resource-constrained approach to in-memory joins
with databases such as cassandra

`prpr.stream` has been found particularly suitable for working with cold
streams - but it's built on top of Manifold Stream (clj) and core.async chan
(cljs) and you can fallback to those APIs when necessary.

It [re-implements](https://github.com/yapsterapp/promisespromises/blob/trunk/src/prpr/stream.cljc) 
the
[manifold.stream](https://github.com/clj-commons/manifold/blob/master/src/manifold/stream.clj) 
operations but using [funcool/promesa](https://github.com/funcool/promesa/) 
for promises and manifold.stream (on clj)
and core.async (on cljs) as a backpressure-enabled error-free transport
  * differences such as core.async's inability to put nils on a channel are
  papered over to present the uniform API


## prpr.a-frame

A-frame is a port of the non-view parts of
[re-frame](https://github.com/day8/re-frame) - event-handling, cofx and
fx - to the async domain. cofx and fx handlers are async functions, while event
handlers remain pure functions. This
makes it straightforward to cleanly separate pure and impure elements of a
program. A-frame was originally developed for a back-end event-driven 
game engine, but it has been found more generally useful and has been 
successfully used for implementing APIs and is perhaps useful client-side too

* cofx handlers are async functions, returning a Promise of updated coeffects
* fx handlers are async functions, returning a Promise of an ignored result
* event handlers are pure, returning a single`{<effect-key> <effect-data>}` map,
or a list of such maps (which will be processed strictly serially)
* based around a pure-data driven async interceptor-chain
[`prpr.a-frame.interceptor-chain`](https://github.com/yapsterapp/promisespromises/blob/trunk/src/prpr/a_frame/interceptor_chain.cljc)
and implemented on top of promesa and
prpr.streams. Being pure-data driven leads to some nice
properties
  * interceptor contexts are fully de/serializable
  * errors can include a 'resume-context' allowing for:
    * automatic retry
    * logging of the resume-context, allowing retry in a REPL
* unlike re-frame, where `dispatch-sync` is uncommon,
`prpr.a-frame/dispatch-sync` has been perhaps the most used type of dispatch
with a-frame. `dispatch-sync` is actually an async fn, but it does not resolve
the result promise until all effects (including transitive dispatches)
resulting from the event have been processed

``` clojure
(require '[prpr.a-frame :as af])

(af/reg-cofx
  ::load-foo
  (fn [app coeffects {id :id}]
    (assoc coeffects ::foo {:id id :name "foo"})))

(af/reg-fx
  :api/response
  (fn [app data]
  ;; do nothing
  ))

(af/reg-event-fx
  ::get-foo
  [(af/inject-cofx ::load-foo {:id #cofx/path [:params :query :id]})]
  (fn [{foo ::foo
        :as coeffects} event]
    [{:api/response {:foo foo}}]))


(def router (af/create-router {:api nil}))

(def ctx (af/dispatch-sync router {:params {:query {:id "1000"}}} [::get-foo]))

;; unpick deref'ing a promise only works on clj
(-> @ctx :a-frame/effects first :api/response)
;; => {:foo {:id "1000", :name "foo"}}

```

# promisespromises

[![Build Status](https://github.com/yapsterapp/promisespromises/actions/workflows/clojure.yml/badge.svg)](https://github.com/yapsterapp/promisespromises/actions)
[![Clojars Project](https://img.shields.io/clojars/v/com.github.yapsterapp/promisespromises.svg)](https://clojars.org/com.github.yapsterapp/promisespromises)
[![cljdoc badge](https://cljdoc.org/badge/com.github.yapsterapp/promisespromises)](https://cljdoc.org/d/com.github.yapsterapp/promisespromises)


TODO - much documentation expansion

A streams API, exposing a Clojure and ClojureScript
compatible Manifold-like promises + streams abstraction

## prpr3.streams

Implements a [Manifold-like](https://github.com/yapsterapp/promisespromises/blob/trunk/src/prpr/stream.cljc)
asynchronous streams API. The fundamental backpressure-sensitive
Promise + Stream model is the same as Manifold, and the API follows the 
[Manifold streams API](https://github.com/clj-commons/manifold) closely

``` clojure
(require '[prpr3.stream :as s])
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
* **stream-joins** - `prpr3.stream/cross` joins streams sorted in a key - it
has various styles of join - inner, outer, left-n, merge, intersect, union -
and can be used for a sensible resource-constrained approach to in-memory joins
with databases such as cassandra

`prpr3.stream` has been found particularly suitable for working with cold
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



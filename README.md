# promisespromises

[![Build Status](https://github.com/yapsterapp/promisespromises/actions/workflows/clojure.yml/badge.svg)](https://github.com/yapsterapp/promisespromises/actions)
[![Clojars Project](https://img.shields.io/clojars/v/com.github.yapsterapp/promisespromises.svg)](https://clojars.org/com.github.yapsterapp/promisespromises)
[![cljdoc badge](https://cljdoc.org/badge/com.github.yapsterapp/promisespromises)](https://cljdoc.org/d/com.github.yapsterapp/promisespromises)

A cross-platform Clojure+Script asynchronous streams API - with
error-propagation and transparent chunking.
Modelled on the [Manifold streams API](https://github.com/clj-commons/manifold)
and using [Promesa CSP](https://funcool.github.io/promesa/latest/channels.html)
as its transport.

## promisespromises.streams

Implements a [Manifold-like](https://github.com/yapsterapp/promisespromises/blob/trunk/src/prpr/stream.cljc)
asynchronous streams API. The underlying backpressure-sensitive
Promise + Stream model is the same as Manifold, and the API follows the
[Manifold streams API](https://github.com/clj-commons/manifold) closely

``` clojure
(require '[promisespromises.stream :as s])
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
* **uniform clj and cljs API** - you can use the same asynchronous
co-ordination code on clj and cljs
* **stream-joins** - `promisespromises.stream/cross` joins streams
sorted in a key - it has various styles of join - inner, outer,
left-n, merge, intersect, union -
and can be used for a sensible constant-memory approach to
in-memory joins with databases such as cassandra

The `promisespromises.stream` API comes from work with cold streams, but
stream objects are just
[Promesa CSP](https://funcool.github.io/promesa/latest/channels.html)
channels, and promise objects are just Promesa Promises, so you can
always fall back to Promesa if you need something a bit different

(ns prpr.stream.protocols)

;; cross-platform interface to build a stream
(defprotocol IStreamFactory
  (-stream
    [_]
    [_ buffer]
    [_ buffer xform]
    #?(:clj [_ buffer xform executor])))

(defprotocol IMaybeStream
  (-stream? [this]))

;; cross-platform stream interface.
;; under the hood these will be regular manifold streams
;; or core.async chans and the full manifold/core.async
;; API will be there for more complex stream processing
(defprotocol IStream
  (-closed? [s])
  (-put!
    [sink val]
    [sink val timeout timeout-val])
  (-take!
    [source]
    [source default-val]
    [source default-val timeout timeout-val])
  (-close! [this])
  (-connect-via
    [source f sink]
    [source f sink opts])
  (-wrap-value [_ v])
  (-buffer [_ n]))

;; a chunk of values which can be placed on a stream and
;; will be handled as if the values themselves were on the stream,
;; leading to much less resource-intensive stream processing in
;; some circumstances (where data naturally comes in chunks,
;; e.g. pages of db-query results)
(defprotocol IStreamChunk
  (-chunk-values [this])
  (-chunk-flatten [this] "flatten any nested promises returning Promise<value>"))

;; a stateful object for efficiently accumulating chunks
(defprotocol IStreamChunkBuilder
  (-start-chunk [_] [_ val])
  (-add-to-chunk [_ val])
  (-finish-chunk [_] [_ val])
  (-discard-chunk [_])
  (-building-chunk? [_])
  (-chunk-state [_]))

;; a potentially wrapped value on a stream
(defprotocol IStreamValue
  (-unwrap-value [_]))

(defprotocol IStreamError
  (-unwrap-error [_]))

;; j.u.c wraps errors in platform exceptions...
;; this protocol helps us unwrap them
(defprotocol IPlatformErrorWrapper
  (-unwrap-platform-error [_]))

(defprotocol IChunkConsumer
  (-peek-chunk [_]
    "peek a single chunk or value")
  (-take-chunk! [_]
    "take chunks or plain values from a stream paying attention to errors
   - returns Promise<chunk | value | error | :prpr.stream/end>")
  (-pushback-chunk! [_ chunk-or-val]
    "push a chunk or values back onto the logical stream (into the buffer)"))

(defprotocol IValueConsumer
  (-peek-value [_]
    "peek a single value")
  (-take-value! [_]
    "take single values from a stream paying attention to chunks and errors
     - returns Promise<value | error | :prpr.stream/end>"))

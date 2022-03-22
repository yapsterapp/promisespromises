(ns prpr.stream.protocols)

(defprotocol IStreamFactory
  (-stream
    [_]
    [_ buffer]
    [_ buffer xform]
    #?(:clj [_ buffer xform executor])))

(defprotocol IStream
  (-put!
    [sing val]
    [sink val timeout timeout-val])
  (-error! [sink err])
  (-take!
    [source]
    [source default-val]
    [source default-val timeout timeout-val])
  (-close! [this])
  (-connect-via
    [source f sink]
    [source f sink opts]))

(defprotocol IStreamChunk
  (-chunk-values [this])
  (-flatten [this] "flatten any nested promises returning Promise<value>"))

(defprotocol IStreamError
  (-error [_]))

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

(ns prpr.error.protocols)

(defprotocol IErrorWrapper
  (-unwrap [_] "unwrap and maybe throw")
  (-unwrap-value [_] "unwrap any error value, never throwing"))

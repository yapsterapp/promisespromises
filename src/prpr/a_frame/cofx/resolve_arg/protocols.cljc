(ns prpr.a-frame.cofx.resolve-arg.protocols)

(defprotocol ICofxPath)

(defrecord CofxPath [path]
  ICofxPath)

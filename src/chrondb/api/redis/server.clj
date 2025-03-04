(ns chrondb.api.redis.server
  "Convenience functions for starting and stopping the Redis server"
  (:require [chrondb.api.redis.core :as redis]
            [chrondb.index.memory :as memory-index]))

(defn start-server
  "Starts a Redis protocol server for ChronDB.
   Parameters:
   - storage: The storage implementation
   - port: The port number to listen on (default: 6379)
   Returns: The server socket"
  ([storage]
   (start-server storage 6379))
  ([storage port]
   (let [index (memory-index/create-memory-index)]
     (redis/start-redis-server storage index port))))

(defn stop-server
  "Stops the Redis server.
   Parameters:
   - server-socket: The server socket to close
   Returns: nil"
  [server-socket]
  (redis/stop-redis-server server-socket))
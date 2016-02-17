(ns ^{:doc "RabbitMQ 中的publish/subscribe 模型"
      :author "savior"}
    rabbitmq-pub-and-sub.publish
  (:require [langohr.core :as lc]
            [langohr.channel :as lch]
            [langohr.exchange :as le]
            [langohr.queue :as lq]
            [langohr.basic :as lb]
            [clojure.data.json :as json]
            [langohr.consumers :as lcons])
  (:import [java.util.concurrent Executors ExecutorService Callable]))



(def mq-config {:host "127.0.0.1" :username "demo" :password "demo"})

(defmacro fn-name [f]
  `(-> ~f var meta :name str))



(defn publish [exchange content]
  (try
    (with-open [conn (lc/connect mq-config)]
      (let [ch (lch/open conn)
            content (json/write-str content)]
        (le/fanout ch exchange {:durable false :auto-delete false})
        (lb/publish ch exchange "" content)
        (println (format "Sent Message %s" content))))
    (catch Exception ex
      (prn ex))))


(defn subscribe
  ([exchange queue register-fn]
   (future
     (with-open [conn (lc/connect mq-config)]
       (let [ch (lch/open conn)
             {:keys [queue]} (lq/declare ch queue {:durable true :auto-delete false})
             handle-fn (fn [ch metadata payload]
                         (let [payload (String. payload "UTF-8")
                               message (json/read-str payload :key-fn keyword)]
                           (register-fn message)))]
         (le/fanout ch exchange {:durable false :auto-delete false})
         (lq/bind ch queue exchange)
         (lcons/blocking-subscribe ch queue handle-fn {:auto-ack true}))))))


(defmacro sub [arg1 & [arg2 arg3]]
  `(let [[exchange# queue# fn-hook#] (cond (and ~arg1 ~arg2 ~arg3) [~arg1 ~arg2 ~arg3]
                                           (and ~arg1 ~arg2) [~arg1 (str "queue-" (fn-name ~arg1)) ~arg3]
                                           ~arg1 [(str "exchange-" (fn-name ~arg1)) (str "queue-" (fn-name ~arg1)) ~arg1])]

     (subscribe exchange# queue# fn-hook#)))

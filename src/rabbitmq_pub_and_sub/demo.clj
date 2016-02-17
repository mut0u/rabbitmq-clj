(ns rabbitmq-pub-and-sub.demo
  (:require [rabbitmq-pub-and-sub.publish :refer :all]))

(defn test-demo-function-hook [data]
  (prn data))

;;bind a queue to a exchange..
(subscribe "exchange-test-demo-function-hook" "queue-test-demo-function" test-demo-function-hook)

;;or
;;(sub test-demo-function-hook)

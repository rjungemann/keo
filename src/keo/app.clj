(ns keo.app
  (:require [keo.env]
            [keo.producer])
  (:use ring.adapter.jetty
        ring.middleware.reload)
  (:import org.apache.kafka.clients.producer.ProducerRecord))

(defonce producer
  (keo.producer/create-basic-producer))

(defn generate-record []
  (ProducerRecord. keo.env/in-topic
                   (Integer. 0)
                   (Long. (quot (System/currentTimeMillis) 1000))
                   "foo"
                   "Hello, from ring!"))

(defn handler [req]
  (keo.producer/basic-send-records-with-producer producer [(generate-record)])
  {:status  200
   :headers {"Content-Type" "text/html"}
   :body    "Sent message to queue."})

(def app
  (wrap-reload #'handler '(keo.app)))

(defn -main []
  (run-jetty #'app {:port 9292}))

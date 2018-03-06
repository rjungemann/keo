(ns keo.env
  (:require [perseverance.core]))

(defonce env
  (java.lang.System/getenv))

(defonce kafka-servers
  (or (get env "KAFKA_SERVERS") "localhost:9092"))

(defonce kafka-producer-transactional-id
  (or (get env "KAFKA_PRODUCER_TRANSACTIONAL_ID") "my-transactional-id"))

(defonce kafka-consumer-group-id
  (or (get env "KAFKA_CONSUMER_GROUP_ID") "my-transactional-consumer-group"))

(defonce in-topic
  (or (get env "IN_TOPICS") "foo"))

; TODO: Consider a mapping between in topics and out topics.
(defonce out-topic
  (or (get env "OUT_TOPIC") "bar"))

(defonce poll-size
  (Integer/parseInt (or (get env "POLL_SIZE") "100")))
